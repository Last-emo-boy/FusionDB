use crate::common::{FusionError, Result};
use crate::monitor;
use crc32fast::Hasher as Crc32Hasher;
use std::fmt::Write as FmtWrite;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use tokio::sync::oneshot;

const MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64 MB per segment
const MIN_WAL_REPLAY_RECORD_BYTES: u64 = 1 + 4;
const MAX_WAL_REPLAY_PREALLOC_ENTRIES: usize = 8192;
const WAL_BATCH_MAGIC: [u8; 4] = *b"FWAL";
const WAL_BATCH_VERSION: u8 = 1;
const WAL_BATCH_HEADER_BYTES: u64 = 4 + 1 + 4;
const WAL_BATCH_CRC_BYTES: u64 = 4;
const WAL_BATCH_ENTRY_COUNT_BYTES: usize = 4;

#[derive(Debug)]
pub enum WalEntry {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalReplayCursor {
    pub segment_id: u64,
    pub offset: u64,
}

#[derive(Debug)]
pub struct WalReplayResult {
    pub entries: Vec<WalEntry>,
    pub cursor: Option<WalReplayCursor>,
    pub stats: monitor::WalReplayStats,
}

enum WalJob {
    Append {
        entries: Vec<WalEntry>,
        resp: oneshot::Sender<Result<()>>,
    },
}

/// WAL state shared between main thread and writer thread.
struct WalState {
    writer: Option<BufWriter<File>>,
    segment_id: u64,
    segment_size: u64,
    base_path: String,
}

impl WalState {
    fn rotate(&mut self) -> std::result::Result<(), FusionError> {
        if let Some(ref mut w) = self.writer {
            w.flush().map_err(WalManager::io_err)?;
            w.get_ref().sync_data().map_err(WalManager::io_err)?;
        }
        let next_segment_id = self
            .segment_id
            .checked_add(1)
            .ok_or_else(|| FusionError::Storage("WAL segment ID overflow".to_string()))?;
        let new_path = segment_path(&self.base_path, next_segment_id);
        let created = !Path::new(&new_path).exists();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)
            .map_err(|e| {
                FusionError::Storage(format!("Failed to open WAL segment {}: {}", new_path, e))
            })?;
        if created {
            file.sync_all().map_err(WalManager::io_err)?;
            WalManager::sync_parent_directory(&new_path)?;
        }
        self.segment_id = next_segment_id;
        self.segment_size = file.metadata().map_err(WalManager::io_err)?.len();
        self.writer = Some(BufWriter::new(file));
        Ok(())
    }
}

/// Compute the file path for a given segment ID.
/// Segment 0 = base_path (backward compat), segment N = base_path.seg.N
fn segment_path(base: &str, id: u64) -> String {
    if id == 0 {
        base.to_string()
    } else {
        let mut path = String::with_capacity(base.len() + ".seg.".len() + u64_decimal_len(id));
        path.push_str(base);
        path.push_str(".seg.");
        write!(&mut path, "{id}").expect("writing to String cannot fail");
        path
    }
}

fn wal_segment_file_prefix(base_name: &str) -> String {
    let mut prefix = String::with_capacity(base_name.len() + ".seg.".len());
    prefix.push_str(base_name);
    prefix.push_str(".seg.");
    prefix
}

fn wal_snapshot_path(base: &str) -> String {
    let mut path = String::with_capacity(base.len() + ".snap".len());
    path.push_str(base);
    path.push_str(".snap");
    path
}

fn u64_decimal_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 10 {
        value /= 10;
        len += 1;
    }
    len
}

fn wal_segment_list() -> Vec<(u64, String)> {
    Vec::with_capacity(1)
}

#[derive(Debug, Default)]
struct WalReplayFileStats {
    valid_bytes: u64,
    entry_count: u64,
    put_count: u64,
    delete_count: u64,
    partial_tail_count: u64,
    truncate_count: u64,
}

/// Find all existing WAL segment files for a base path, sorted by segment ID.
fn find_segments(base: &str) -> Result<Vec<(u64, String)>> {
    let mut segments = wal_segment_list();

    // Check base file (segment 0)
    if std::path::Path::new(base).exists() {
        segments.push((0, base.to_string()));
    }

    // Check segment files: {base}.seg.{N}
    let parent = Path::new(base)
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let base_name = Path::new(base)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let prefix = wal_segment_file_prefix(&base_name);

    let directory = fs::read_dir(parent).map_err(|error| {
        FusionError::Storage(format!(
            "Failed to list WAL directory {}: {}",
            parent.display(),
            error
        ))
    })?;
    for entry in directory {
        let entry = entry.map_err(|error| {
            FusionError::Storage(format!(
                "Failed to inspect WAL directory {}: {}",
                parent.display(),
                error
            ))
        })?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(suffix) = name.strip_prefix(&prefix) {
            if let Ok(id) = suffix.parse::<u64>() {
                segments.push((id, entry.path().to_string_lossy().to_string()));
            }
        }
    }

    segments.sort_by_key(|(id, _)| *id);
    if let Some((first_id, _)) = segments.first() {
        if *first_id != 0 {
            return Err(FusionError::Storage(format!(
                "WAL segment sequence starts at {}, expected 0",
                first_id
            )));
        }
    }
    for pair in segments.windows(2) {
        let expected = pair[0].0.checked_add(1).ok_or_else(|| {
            FusionError::Storage("WAL segment ID overflow while listing segments".to_string())
        })?;
        if pair[1].0 != expected {
            return Err(FusionError::Storage(format!(
                "WAL segment sequence gap: expected {}, found {}",
                expected, pair[1].0
            )));
        }
    }
    Ok(segments)
}

pub struct WalManager {
    state: Arc<Mutex<WalState>>,
    tx: mpsc::Sender<WalJob>,
    path: String,
    segment_id: Arc<AtomicU64>,
}

impl WalManager {
    pub fn new(path: &str) -> Result<Self> {
        // Determine which segment to append to
        let segments = find_segments(path)?;
        let (active_id, active_path) = if segments.is_empty() {
            (0u64, path.to_string())
        } else {
            segments.last().unwrap().clone()
        };

        let created = !Path::new(&active_path).exists();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)
            .map_err(|e| FusionError::Storage(format!("Failed to open WAL: {}", e)))?;
        if created {
            file.sync_all().map_err(Self::io_err)?;
            Self::sync_parent_directory(&active_path)?;
        }

        let file_size = file.metadata().map_err(Self::io_err)?.len();

        let wal_state = WalState {
            writer: Some(BufWriter::new(file)),
            segment_id: active_id,
            segment_size: file_size,
            base_path: path.to_string(),
        };

        let state = Arc::new(Mutex::new(wal_state));
        let (tx, rx) = mpsc::channel();

        let writer_state = state.clone();

        thread::Builder::new()
            .name("wal-writer".to_string())
            .spawn(move || {
                Self::wal_writer_loop(rx, writer_state);
            })
            .map_err(|e| FusionError::Storage(format!("Failed to spawn WAL thread: {}", e)))?;

        Ok(Self {
            state,
            tx,
            path: path.to_string(),
            segment_id: Arc::new(AtomicU64::new(active_id)),
        })
    }

    fn wal_writer_loop(rx: Receiver<WalJob>, state: Arc<Mutex<WalState>>) {
        loop {
            let first_job = match rx.recv() {
                Ok(job) => job,
                Err(_) => break,
            };

            let mut batch = vec![first_job];

            // Group commit: collect more jobs
            for _ in 0..1000 {
                match rx.try_recv() {
                    Ok(job) => batch.push(job),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => break,
                }
            }

            let mut write_result = Ok(());
            {
                if let Ok(mut ws) = state.lock() {
                    // Check for segment rotation before writing
                    if ws.segment_size >= MAX_SEGMENT_SIZE {
                        if let Err(e) = ws.rotate() {
                            write_result = Err(e);
                        }
                    }

                    if write_result.is_ok() {
                        if let Some(ref mut writer) = ws.writer {
                            let mut batch_bytes = 0u64;
                            for job in &batch {
                                let WalJob::Append { entries, .. } = job;
                                match Self::write_batch_record(writer, entries) {
                                    Ok(n) => batch_bytes += n as u64,
                                    Err(e) => {
                                        write_result = Err(e);
                                        break;
                                    }
                                }
                            }

                            if write_result.is_ok() {
                                if let Err(e) = writer.flush().map_err(Self::io_err) {
                                    write_result = Err(e);
                                } else if let Err(e) =
                                    writer.get_ref().sync_data().map_err(Self::io_err)
                                {
                                    write_result = Err(e);
                                } else {
                                    ws.segment_size += batch_bytes;
                                    monitor::inc_wal_write();
                                }
                            }
                        } else {
                            write_result = Err(FusionError::Storage("WAL closed".to_string()));
                        }
                    }
                } else {
                    write_result = Err(FusionError::Storage("WAL Lock poisoned".to_string()));
                }
            }

            for job in batch {
                match job {
                    WalJob::Append { resp, .. } => {
                        let res = match &write_result {
                            Ok(_) => Ok(()),
                            Err(e) => Err(FusionError::Storage(format!("WAL Error: {}", e))),
                        };
                        let _ = resp.send(res);
                    }
                }
            }
        }
    }

    fn encode_entry(encoded: &mut Vec<u8>, entry: &WalEntry) -> Result<()> {
        match entry {
            WalEntry::Put(k, v) => {
                let key_len = u32::try_from(k.len()).map_err(|_| {
                    FusionError::Storage("WAL key exceeds u32 length boundary".to_string())
                })?;
                let value_len = u32::try_from(v.len()).map_err(|_| {
                    FusionError::Storage("WAL value exceeds u32 length boundary".to_string())
                })?;
                encoded.push(1);
                encoded.extend_from_slice(&key_len.to_le_bytes());
                encoded.extend_from_slice(k);
                encoded.extend_from_slice(&value_len.to_le_bytes());
                encoded.extend_from_slice(v);
            }
            WalEntry::Delete(k) => {
                let key_len = u32::try_from(k.len()).map_err(|_| {
                    FusionError::Storage("WAL key exceeds u32 length boundary".to_string())
                })?;
                encoded.push(2);
                encoded.extend_from_slice(&key_len.to_le_bytes());
                encoded.extend_from_slice(k);
            }
        }
        Ok(())
    }

    fn encode_batch_record(entries: &[WalEntry]) -> Result<Vec<u8>> {
        let entry_count = u32::try_from(entries.len()).map_err(|_| {
            FusionError::Storage("WAL batch exceeds u32 entry-count boundary".to_string())
        })?;
        let mut payload = Vec::new();
        payload.extend_from_slice(&entry_count.to_le_bytes());
        for entry in entries {
            Self::encode_entry(&mut payload, entry)?;
        }
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            FusionError::Storage("WAL batch exceeds u32 payload-length boundary".to_string())
        })?;
        let frame_capacity = WAL_BATCH_HEADER_BYTES
            .checked_add(payload.len() as u64)
            .and_then(|size| size.checked_add(WAL_BATCH_CRC_BYTES))
            .and_then(|size| usize::try_from(size).ok())
            .ok_or_else(|| FusionError::Storage("WAL batch frame size overflow".to_string()))?;

        let mut frame = Vec::with_capacity(frame_capacity);
        frame.extend_from_slice(&WAL_BATCH_MAGIC);
        frame.push(WAL_BATCH_VERSION);
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(&payload);
        let crc = Self::batch_crc(WAL_BATCH_VERSION, payload_len, &payload);
        frame.extend_from_slice(&crc.to_le_bytes());
        Ok(frame)
    }

    fn write_batch_record(writer: &mut BufWriter<File>, entries: &[WalEntry]) -> Result<usize> {
        let frame = Self::encode_batch_record(entries)?;
        writer.write_all(&frame).map_err(Self::io_err)?;
        monitor::add_wal_bytes(frame.len() as u64);
        Ok(frame.len())
    }

    fn batch_crc(version: u8, payload_len: u32, payload: &[u8]) -> u32 {
        let mut hasher = Crc32Hasher::new();
        hasher.update(&WAL_BATCH_MAGIC);
        hasher.update(&[version]);
        hasher.update(&payload_len.to_le_bytes());
        hasher.update(payload);
        hasher.finalize()
    }

    pub async fn append_batch_async(&self, entries: Vec<WalEntry>) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx
            .send(WalJob::Append {
                entries,
                resp: resp_tx,
            })
            .map_err(|_| FusionError::Storage("WAL thread dead".to_string()))?;

        resp_rx
            .await
            .map_err(|_| FusionError::Storage("WAL response channel closed".to_string()))??;
        Ok(())
    }

    // Keep synchronous append for compatibility/testing if needed, or implement it via channel?
    // Let's deprecate direct append in favor of async or make it block.
    pub fn append(&self, entry: &WalEntry) -> Result<()> {
        // Just use a temporary runtime or block?
        // Actually, we can just send to channel and block_on receiver.
        // But since we are likely in async context, let's assume we use append_batch_async mostly.
        // For existing sync usage (e.g. replay), we don't use append.

        // This is legacy sync append, we can implement it by sending to channel and blocking thread.
        let (resp_tx, resp_rx) = oneshot::channel();
        let entries = match entry {
            WalEntry::Put(k, v) => vec![WalEntry::Put(k.clone(), v.clone())],
            WalEntry::Delete(k) => vec![WalEntry::Delete(k.clone())],
        };

        self.tx
            .send(WalJob::Append {
                entries,
                resp: resp_tx,
            })
            .map_err(|_| FusionError::Storage("WAL thread dead".to_string()))?;

        // Block waiting for response
        futures::executor::block_on(resp_rx)
            .map_err(|_| FusionError::Storage("WAL response channel closed".to_string()))??;

        Ok(())
    }

    pub fn append_batch(&self, entries: &[WalEntry]) -> Result<()> {
        // Sync wrapper around async
        let (resp_tx, resp_rx) = oneshot::channel();

        // Clone entries because we need to send ownership
        // This is a cost, but WalEntry owns data.
        // Ideally we change signature to take Vec<WalEntry>
        let mut entries_vec = Vec::with_capacity(entries.len());
        for entry in entries {
            match entry {
                WalEntry::Put(k, v) => entries_vec.push(WalEntry::Put(k.clone(), v.clone())),
                WalEntry::Delete(k) => entries_vec.push(WalEntry::Delete(k.clone())),
            }
        }

        self.tx
            .send(WalJob::Append {
                entries: entries_vec,
                resp: resp_tx,
            })
            .map_err(|_| FusionError::Storage("WAL thread dead".to_string()))?;

        futures::executor::block_on(resp_rx)
            .map_err(|_| FusionError::Storage("WAL response channel closed".to_string()))??;
        Ok(())
    }

    /// Replay all WAL segments in order, returning all entries.
    pub fn replay(&self) -> Result<Vec<WalEntry>> {
        Ok(self.replay_with_summary()?.entries)
    }

    /// Replay all WAL segments and return the last complete record cursor.
    ///
    /// The cursor is not a persisted WAL floor by itself; it is the candidate
    /// that a manifest/checkpoint record can durably publish before safe WAL
    /// deletion is attempted.
    pub fn replay_with_summary(&self) -> Result<WalReplayResult> {
        let replay_start = Instant::now();
        let _guard = self
            .state
            .lock()
            .map_err(|_| FusionError::Storage("WAL Lock poisoned".to_string()))?;

        let mut replay_stats = monitor::WalReplayStats::default();
        let segments = match find_segments(&self.path) {
            Ok(segments) => segments,
            Err(error) => {
                replay_stats.error_count = 1;
                replay_stats.total_us =
                    u64::try_from(replay_start.elapsed().as_micros()).unwrap_or(u64::MAX);
                monitor::record_wal_replay(replay_stats);
                return Err(error);
            }
        };
        replay_stats.segment_count = segments.len() as u64;
        let mut all_entries = Vec::new();
        let mut cursor = None;

        for (segment_index, (seg_id, seg_path)) in segments.iter().enumerate() {
            replay_stats.last_segment_id = *seg_id;
            let file = match File::open(seg_path) {
                Ok(f) => f,
                Err(e) => {
                    replay_stats.error_count += 1;
                    replay_stats.total_us =
                        u64::try_from(replay_start.elapsed().as_micros()).unwrap_or(u64::MAX);
                    monitor::record_wal_replay(replay_stats);
                    return Err(FusionError::Storage(format!(
                        "Failed to open WAL segment for replay {}: {}",
                        seg_path, e
                    )));
                }
            };
            let file_len = match file.metadata() {
                Ok(metadata) => metadata.len(),
                Err(error) => {
                    replay_stats.error_count += 1;
                    replay_stats.total_us =
                        u64::try_from(replay_start.elapsed().as_micros()).unwrap_or(u64::MAX);
                    monitor::record_wal_replay(replay_stats);
                    return Err(FusionError::Storage(format!(
                        "Failed to inspect WAL segment {}: {}",
                        seg_path, error
                    )));
                }
            };
            replay_stats.bytes += file_len;
            if file_len == 0 {
                replay_stats.last_valid_offset = 0;
                cursor = Some(WalReplayCursor {
                    segment_id: *seg_id,
                    offset: 0,
                });
                continue;
            }
            all_entries.reserve(Self::replay_entry_capacity_hint(file_len));
            let allow_partial_tail = segment_index + 1 == segments.len();
            let (entries, file_stats) =
                match Self::replay_single_file(seg_path, file, file_len, allow_partial_tail) {
                    Ok(result) => result,
                    Err(error) => {
                        replay_stats.error_count += 1;
                        replay_stats.total_us =
                            u64::try_from(replay_start.elapsed().as_micros()).unwrap_or(u64::MAX);
                        monitor::record_wal_replay(replay_stats);
                        return Err(error);
                    }
                };
            replay_stats.entry_count += file_stats.entry_count;
            replay_stats.put_count += file_stats.put_count;
            replay_stats.delete_count += file_stats.delete_count;
            replay_stats.partial_tail_count += file_stats.partial_tail_count;
            replay_stats.truncate_count += file_stats.truncate_count;
            replay_stats.valid_bytes += file_stats.valid_bytes;
            replay_stats.last_valid_offset = file_stats.valid_bytes;
            cursor = Some(WalReplayCursor {
                segment_id: *seg_id,
                offset: file_stats.valid_bytes,
            });
            if !entries.is_empty() {
                println!(
                    "WAL Replay: segment {} ({}) -> {} entries",
                    seg_id,
                    seg_path,
                    entries.len()
                );
            }
            all_entries.extend(entries);
        }

        replay_stats.total_us =
            u64::try_from(replay_start.elapsed().as_micros()).unwrap_or(u64::MAX);
        monitor::record_wal_replay(replay_stats);
        Ok(WalReplayResult {
            entries: all_entries,
            cursor,
            stats: replay_stats,
        })
    }

    /// Replay a single WAL file, truncating partial records at the end.
    fn replay_single_file(
        path: &str,
        file: File,
        file_len: u64,
        allow_partial_tail: bool,
    ) -> Result<(Vec<WalEntry>, WalReplayFileStats)> {
        let mut reader = BufReader::new(file);
        let mut entries = Vec::with_capacity(Self::replay_entry_capacity_hint(file_len));
        let mut stats = WalReplayFileStats::default();
        let mut valid_pos = 0u64;

        loop {
            let mut opcode = [0u8; 1];
            match reader.read_exact(&mut opcode) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && valid_pos == file_len => {
                    break;
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    if allow_partial_tail {
                        stats.partial_tail_count += 1;
                        break;
                    }
                    return Err(FusionError::Storage(format!(
                        "Partial WAL record in non-final segment {} at offset {}",
                        path, valid_pos
                    )));
                }
                Err(e) => return Err(FusionError::Storage(format!("WAL Replay Error: {}", e))),
            }

            let parsed = match opcode[0] {
                1 | 2 => Self::read_legacy_record(&mut reader, opcode[0])
                    .map(|(entry, record_len)| (vec![entry], record_len)),
                byte if byte == WAL_BATCH_MAGIC[0] => {
                    Self::read_batch_record(&mut reader, file_len, valid_pos)
                }
                _ => {
                    return Err(FusionError::Storage(format!(
                        "Unknown WAL OpCode {} in {} at offset {}",
                        opcode[0], path, valid_pos
                    )))
                }
            };

            match parsed {
                Ok((record_entries, record_len)) => {
                    for entry in &record_entries {
                        match entry {
                            WalEntry::Put(_, _) => stats.put_count += 1,
                            WalEntry::Delete(_) => stats.delete_count += 1,
                        }
                    }
                    stats.entry_count += record_entries.len() as u64;
                    entries.extend(record_entries);
                    valid_pos = valid_pos.checked_add(record_len).ok_or_else(|| {
                        FusionError::Storage("WAL replay offset overflow".to_string())
                    })?;
                }
                Err(FusionError::Storage(msg)) if msg == "Partial Record" => {
                    if allow_partial_tail {
                        println!("WAL Replay: partial record at end of {}. Truncating.", path);
                        stats.partial_tail_count += 1;
                        break;
                    }
                    return Err(FusionError::Storage(format!(
                        "Partial WAL record in non-final segment {} at offset {}",
                        path, valid_pos
                    )));
                }
                Err(e) => return Err(e),
            }
        }

        // Truncate partial tail if needed
        stats.valid_bytes = valid_pos;
        if valid_pos < file_len {
            let file = OpenOptions::new().write(true).open(path).map_err(|error| {
                FusionError::Storage(format!(
                    "Failed to open partial WAL tail {} for truncation: {}",
                    path, error
                ))
            })?;
            file.set_len(valid_pos).map_err(|error| {
                FusionError::Storage(format!(
                    "Failed to truncate partial WAL tail {} to {} bytes: {}",
                    path, valid_pos, error
                ))
            })?;
            file.sync_all().map_err(|error| {
                FusionError::Storage(format!(
                    "Failed to sync truncated WAL tail {}: {}",
                    path, error
                ))
            })?;
            stats.truncate_count += 1;
        }

        Ok((entries, stats))
    }

    fn read_legacy_record(reader: &mut BufReader<File>, opcode: u8) -> Result<(WalEntry, u64)> {
        let mut record_len = 1u64;
        let key_len = Self::read_replay_u32(reader, &mut record_len)? as usize;
        let key = Self::read_replay_vec(reader, key_len, &mut record_len)?;
        match opcode {
            1 => {
                let value_len = Self::read_replay_u32(reader, &mut record_len)? as usize;
                let value = Self::read_replay_vec(reader, value_len, &mut record_len)?;
                Ok((WalEntry::Put(key, value), record_len))
            }
            2 => Ok((WalEntry::Delete(key), record_len)),
            _ => Err(FusionError::Storage(format!(
                "Unknown legacy WAL OpCode: {}",
                opcode
            ))),
        }
    }

    fn read_batch_record(
        reader: &mut BufReader<File>,
        file_len: u64,
        record_start: u64,
    ) -> Result<(Vec<WalEntry>, u64)> {
        let mut magic_tail = [0u8; 3];
        Self::read_replay_exact(reader, &mut magic_tail)?;
        if magic_tail != WAL_BATCH_MAGIC[1..] {
            return Err(FusionError::Storage(format!(
                "Invalid WAL batch magic at offset {}",
                record_start
            )));
        }

        let mut version = [0u8; 1];
        Self::read_replay_exact(reader, &mut version)?;
        let mut payload_len_bytes = [0u8; 4];
        Self::read_replay_exact(reader, &mut payload_len_bytes)?;
        let payload_len = u32::from_le_bytes(payload_len_bytes);
        let frame_len = WAL_BATCH_HEADER_BYTES
            .checked_add(payload_len as u64)
            .and_then(|size| size.checked_add(WAL_BATCH_CRC_BYTES))
            .ok_or_else(|| FusionError::Storage("WAL batch frame length overflow".to_string()))?;
        let frame_end = record_start
            .checked_add(frame_len)
            .ok_or_else(|| FusionError::Storage("WAL batch frame offset overflow".to_string()))?;
        if frame_end > file_len {
            return Err(FusionError::Storage("Partial Record".to_string()));
        }
        if version[0] != WAL_BATCH_VERSION {
            return Err(FusionError::Storage(format!(
                "Unsupported WAL batch version {} at offset {}",
                version[0], record_start
            )));
        }
        if (payload_len as usize) < WAL_BATCH_ENTRY_COUNT_BYTES {
            return Err(FusionError::Storage(format!(
                "Invalid WAL batch payload length {} at offset {}",
                payload_len, record_start
            )));
        }

        let mut payload = vec![0u8; payload_len as usize];
        Self::read_replay_exact(reader, &mut payload)?;
        let mut crc_bytes = [0u8; 4];
        Self::read_replay_exact(reader, &mut crc_bytes)?;
        let expected_crc = u32::from_le_bytes(crc_bytes);
        let actual_crc = Self::batch_crc(version[0], payload_len, &payload);
        if actual_crc != expected_crc {
            return Err(FusionError::Storage(format!(
                "WAL batch checksum mismatch at offset {}: expected {:08x}, computed {:08x}",
                record_start, expected_crc, actual_crc
            )));
        }

        let entries = Self::decode_batch_payload(&payload, record_start)?;
        Ok((entries, frame_len))
    }

    fn decode_batch_payload(payload: &[u8], record_start: u64) -> Result<Vec<WalEntry>> {
        let mut cursor = 0usize;
        let entry_count = Self::read_payload_u32(payload, &mut cursor, record_start)? as usize;
        let maximum_possible_entries = payload.len().saturating_sub(WAL_BATCH_ENTRY_COUNT_BYTES)
            / MIN_WAL_REPLAY_RECORD_BYTES as usize;
        if entry_count > maximum_possible_entries && entry_count != 0 {
            return Err(FusionError::Storage(format!(
                "WAL batch entry count {} exceeds payload boundary at offset {}",
                entry_count, record_start
            )));
        }
        let mut entries = Vec::with_capacity(entry_count.min(MAX_WAL_REPLAY_PREALLOC_ENTRIES));
        for _ in 0..entry_count {
            let opcode = *payload.get(cursor).ok_or_else(|| {
                FusionError::Storage(format!(
                    "WAL batch entry exceeds payload boundary at offset {}",
                    record_start
                ))
            })?;
            cursor += 1;
            let key_len = Self::read_payload_u32(payload, &mut cursor, record_start)? as usize;
            let key = Self::read_payload_vec(payload, &mut cursor, key_len, record_start)?;
            match opcode {
                1 => {
                    let value_len =
                        Self::read_payload_u32(payload, &mut cursor, record_start)? as usize;
                    let value =
                        Self::read_payload_vec(payload, &mut cursor, value_len, record_start)?;
                    entries.push(WalEntry::Put(key, value));
                }
                2 => entries.push(WalEntry::Delete(key)),
                _ => {
                    return Err(FusionError::Storage(format!(
                        "Unknown WAL batch OpCode {} at offset {}",
                        opcode, record_start
                    )))
                }
            }
        }
        if cursor != payload.len() {
            return Err(FusionError::Storage(format!(
                "WAL batch has {} trailing payload bytes at offset {}",
                payload.len() - cursor,
                record_start
            )));
        }
        Ok(entries)
    }

    fn read_replay_exact(reader: &mut BufReader<File>, bytes: &mut [u8]) -> Result<()> {
        reader.read_exact(bytes).map_err(|error| {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                FusionError::Storage("Partial Record".to_string())
            } else {
                FusionError::Storage(format!("WAL Read Error: {}", error))
            }
        })
    }

    fn read_replay_u32(reader: &mut BufReader<File>, record_len: &mut u64) -> Result<u32> {
        let mut bytes = [0u8; 4];
        Self::read_replay_exact(reader, &mut bytes)?;
        *record_len = record_len
            .checked_add(4)
            .ok_or_else(|| FusionError::Storage("WAL record length overflow".to_string()))?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_replay_vec(
        reader: &mut BufReader<File>,
        len: usize,
        record_len: &mut u64,
    ) -> Result<Vec<u8>> {
        let mut bytes = vec![0u8; len];
        Self::read_replay_exact(reader, &mut bytes)?;
        *record_len = record_len
            .checked_add(len as u64)
            .ok_or_else(|| FusionError::Storage("WAL record length overflow".to_string()))?;
        Ok(bytes)
    }

    fn read_payload_u32(payload: &[u8], cursor: &mut usize, record_start: u64) -> Result<u32> {
        let end = cursor
            .checked_add(4)
            .ok_or_else(|| FusionError::Storage("WAL batch payload offset overflow".to_string()))?;
        let bytes = payload.get(*cursor..end).ok_or_else(|| {
            FusionError::Storage(format!(
                "WAL batch u32 exceeds payload boundary at offset {}",
                record_start
            ))
        })?;
        *cursor = end;
        Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| {
            FusionError::Storage("Invalid WAL batch u32 encoding".to_string())
        })?))
    }

    fn read_payload_vec(
        payload: &[u8],
        cursor: &mut usize,
        len: usize,
        record_start: u64,
    ) -> Result<Vec<u8>> {
        let end = cursor
            .checked_add(len)
            .ok_or_else(|| FusionError::Storage("WAL batch payload offset overflow".to_string()))?;
        let bytes = payload.get(*cursor..end).ok_or_else(|| {
            FusionError::Storage(format!(
                "WAL batch value exceeds payload boundary at offset {}",
                record_start
            ))
        })?;
        *cursor = end;
        Ok(bytes.to_vec())
    }

    fn replay_entry_capacity_hint(file_len: u64) -> usize {
        (file_len / MIN_WAL_REPLAY_RECORD_BYTES).min(MAX_WAL_REPLAY_PREALLOC_ENTRIES as u64)
            as usize
    }

    /// Truncate all WAL segments — delete old segments and reset the active segment.
    pub fn truncate(&self) -> Result<()> {
        let mut ws = self
            .state
            .lock()
            .map_err(|_| FusionError::Storage("WAL Lock poisoned".to_string()))?;

        if let Some(mut writer) = ws.writer.take() {
            writer.flush().map_err(Self::io_err)?;
            writer.get_ref().sync_data().map_err(Self::io_err)?;
        }

        // Delete all segment files
        let segments = find_segments(&self.path)?;
        for (_, seg_path) in &segments {
            fs::remove_file(seg_path).map_err(|error| {
                FusionError::Storage(format!(
                    "Failed to remove WAL segment {}: {}",
                    seg_path, error
                ))
            })?;
        }

        // Re-create base file and reset state
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| FusionError::Storage(format!("Failed to recreate WAL: {}", e)))?;
        file.sync_all().map_err(Self::io_err)?;
        Self::sync_parent_directory(&self.path)?;

        ws.writer = Some(BufWriter::new(file));
        ws.segment_id = 0;
        ws.segment_size = 0;
        self.segment_id.store(0, Ordering::Relaxed);

        Ok(())
    }

    pub fn create_checkpoint<I>(&self, iter: I) -> Result<()>
    where
        I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        let snap_path = wal_snapshot_path(&self.path);
        let mut file = BufWriter::new(File::create(&snap_path).map_err(Self::io_err)?);

        for (k, v) in iter {
            file.write_all(&[1u8]).map_err(Self::io_err)?;
            file.write_all(&(k.len() as u32).to_le_bytes())
                .map_err(Self::io_err)?;
            file.write_all(&k).map_err(Self::io_err)?;
            file.write_all(&(v.len() as u32).to_le_bytes())
                .map_err(Self::io_err)?;
            file.write_all(&v).map_err(Self::io_err)?;
        }
        file.flush().map_err(Self::io_err)?;
        file.get_ref().sync_all().map_err(Self::io_err)?;
        Self::sync_parent_directory(&snap_path)?;
        drop(file);

        {
            let mut ws = self
                .state
                .lock()
                .map_err(|_| FusionError::Storage("WAL Lock poisoned".to_string()))?;

            if let Some(mut writer) = ws.writer.take() {
                writer.flush().map_err(Self::io_err)?;
                writer.get_ref().sync_data().map_err(Self::io_err)?;
            }
            let old_segments = find_segments(&self.path)?;

            if let Err(e) = fs::rename(&snap_path, &self.path) {
                let active_path = segment_path(&self.path, ws.segment_id);
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&active_path)
                    .map_err(Self::io_err)?;
                ws.writer = Some(BufWriter::new(file));
                return Err(Self::io_err(e));
            }
            Self::sync_parent_directory(&self.path)?;

            for (segment_id, seg_path) in &old_segments {
                if *segment_id == 0 {
                    continue;
                }
                fs::remove_file(seg_path).map_err(|error| {
                    FusionError::Storage(format!(
                        "Failed to remove checkpointed WAL segment {}: {}",
                        seg_path, error
                    ))
                })?;
            }
            Self::sync_parent_directory(&self.path)?;

            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .map_err(Self::io_err)?;
            let size = file.metadata().map(|m| m.len()).unwrap_or(0);
            ws.writer = Some(BufWriter::new(file));
            ws.segment_id = 0;
            ws.segment_size = size;
        }

        Ok(())
    }

    /// Return the number of WAL segment files.
    #[allow(dead_code)]
    pub fn segment_count(&self) -> usize {
        find_segments(&self.path)
            .map(|segments| segments.len())
            .unwrap_or(0)
    }

    fn sync_parent_directory(path: &str) -> Result<()> {
        let parent = Path::new(path)
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let directory = File::open(parent).map_err(|error| {
            FusionError::Storage(format!(
                "Failed to open WAL parent directory {} for sync: {}",
                parent.display(),
                error
            ))
        })?;
        directory.sync_all().map_err(|error| {
            FusionError::Storage(format!(
                "Failed to sync WAL parent directory {}: {}",
                parent.display(),
                error
            ))
        })
    }

    fn io_err(e: io::Error) -> FusionError {
        FusionError::Storage(format!("WAL IO Error: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_path() {
        assert_eq!(segment_path("data/fusion.wal", 0), "data/fusion.wal");
        assert_eq!(segment_path("data/fusion.wal", 1), "data/fusion.wal.seg.1");
        assert_eq!(
            segment_path("data/fusion.wal", 42),
            "data/fusion.wal.seg.42"
        );
    }

    #[test]
    fn test_segment_path_preallocates_segment_path() {
        let path = segment_path("data/fusion.wal", 42);

        assert_eq!(path, "data/fusion.wal.seg.42");
        assert!(path.capacity() >= path.len());
    }

    #[test]
    fn test_wal_segment_file_prefix_preallocates_exact_prefix() {
        let prefix = wal_segment_file_prefix("fusion.wal");

        assert_eq!(prefix, "fusion.wal.seg.");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn test_wal_snapshot_path_preallocates_exact_path() {
        let path = wal_snapshot_path("data/fusion.wal");

        assert_eq!(path, "data/fusion.wal.snap");
        assert!(path.capacity() >= path.len());
    }

    #[test]
    fn test_u64_decimal_len_counts_digits() {
        assert_eq!(u64_decimal_len(0), 1);
        assert_eq!(u64_decimal_len(9), 1);
        assert_eq!(u64_decimal_len(10), 2);
        assert_eq!(u64_decimal_len(u64::MAX), 20);
    }

    #[test]
    fn test_find_segments_empty() {
        let segments = find_segments("nonexistent_test_path_xyz.wal").unwrap();
        assert!(segments.is_empty());
    }

    #[test]
    fn test_wal_segment_list_preallocates_base_segment() {
        let segments = wal_segment_list();
        assert!(segments.capacity() >= 1);
    }

    fn write_raw_put_record(file: &mut File, key: &[u8], value: &[u8]) -> u64 {
        file.write_all(&[1u8]).unwrap();
        file.write_all(&(key.len() as u32).to_le_bytes()).unwrap();
        file.write_all(key).unwrap();
        file.write_all(&(value.len() as u32).to_le_bytes()).unwrap();
        file.write_all(value).unwrap();
        (1 + 4 + key.len() + 4 + value.len()) as u64
    }

    fn unique_wal_path(label: &str) -> String {
        format!(
            "test_wal_{}_{}_{}.wal",
            label,
            std::process::id(),
            uuid::Uuid::new_v4()
        )
    }

    #[test]
    fn test_wal_write_and_replay() {
        let path = format!("test_wal_seg_{}.wal", std::process::id());
        let wal = WalManager::new(&path).unwrap();
        wal.append(&WalEntry::Put(b"key1".to_vec(), b"val1".to_vec()))
            .unwrap();
        wal.append(&WalEntry::Put(b"key2".to_vec(), b"val2".to_vec()))
            .unwrap();
        wal.append(&WalEntry::Delete(b"key1".to_vec())).unwrap();

        // Re-open and replay
        let wal2 = WalManager::new(&path).unwrap();
        let entries = wal2.replay().unwrap();
        assert_eq!(entries.len(), 3);

        // Cleanup
        wal2.truncate().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_wal_create_checkpoint_replays_snapshot_entries() {
        let path = format!("test_wal_checkpoint_{}.wal", std::process::id());
        let wal = WalManager::new(&path).unwrap();
        wal.create_checkpoint(
            vec![(b"checkpoint-key".to_vec(), b"checkpoint-value".to_vec())].into_iter(),
        )
        .unwrap();

        let wal2 = WalManager::new(&path).unwrap();
        let entries = wal2.replay().unwrap();
        match entries.as_slice() {
            [WalEntry::Put(k, v)] => {
                assert_eq!(k, b"checkpoint-key");
                assert_eq!(v, b"checkpoint-value");
            }
            other => panic!("unexpected checkpoint entries: {:?}", other),
        }

        wal2.truncate().unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(wal_snapshot_path(&path));
    }

    #[test]
    fn test_wal_append_batch_replay_preserves_order() {
        let path = format!("test_wal_batch_{}.wal", std::process::id());
        let wal = WalManager::new(&path).unwrap();
        wal.append_batch(&[
            WalEntry::Put(b"key1".to_vec(), b"val1".to_vec()),
            WalEntry::Delete(b"key1".to_vec()),
            WalEntry::Put(b"key2".to_vec(), b"val2".to_vec()),
        ])
        .unwrap();

        let wal2 = WalManager::new(&path).unwrap();
        let entries = wal2.replay().unwrap();
        match entries.as_slice() {
            [WalEntry::Put(k1, v1), WalEntry::Delete(k2), WalEntry::Put(k3, v3)] => {
                assert_eq!(k1, b"key1");
                assert_eq!(v1, b"val1");
                assert_eq!(k2, b"key1");
                assert_eq!(k3, b"key2");
                assert_eq!(v3, b"val2");
            }
            other => panic!("unexpected WAL entries: {:?}", other),
        }

        wal2.truncate().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_wal_append_batch_writes_one_versioned_crc_frame() {
        let path = unique_wal_path("framed_batch");
        let wal = WalManager::new(&path).unwrap();
        wal.append_batch(&[
            WalEntry::Put(b"key1".to_vec(), b"value1".to_vec()),
            WalEntry::Delete(b"key2".to_vec()),
        ])
        .unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes[..WAL_BATCH_MAGIC.len()], &WAL_BATCH_MAGIC);
        assert_eq!(bytes[WAL_BATCH_MAGIC.len()], WAL_BATCH_VERSION);
        let payload_len_offset = WAL_BATCH_MAGIC.len() + 1;
        let payload_len = u32::from_le_bytes(
            bytes[payload_len_offset..payload_len_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(
            bytes.len(),
            WAL_BATCH_HEADER_BYTES as usize + payload_len + WAL_BATCH_CRC_BYTES as usize
        );
        let payload_start = WAL_BATCH_HEADER_BYTES as usize;
        let payload_end = payload_start + payload_len;
        let stored_crc = u32::from_le_bytes(bytes[payload_end..].try_into().unwrap());
        assert_eq!(
            stored_crc,
            WalManager::batch_crc(
                WAL_BATCH_VERSION,
                payload_len as u32,
                &bytes[payload_start..payload_end]
            )
        );
        assert_eq!(
            u32::from_le_bytes(bytes[payload_start..payload_start + 4].try_into().unwrap()),
            2
        );

        let replayed = wal.replay().unwrap();
        assert_eq!(replayed.len(), 2);
        wal.truncate().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_wal_append_batch_async_writes_replayable_frame() {
        let path = unique_wal_path("async_batch");
        let wal = WalManager::new(&path).unwrap();
        wal.append_batch_async(vec![
            WalEntry::Put(b"async-key".to_vec(), b"async-value".to_vec()),
            WalEntry::Delete(b"removed-key".to_vec()),
        ])
        .await
        .unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(&WAL_BATCH_MAGIC));
        let entries = wal.replay().unwrap();
        match entries.as_slice() {
            [WalEntry::Put(key, value), WalEntry::Delete(deleted)] => {
                assert_eq!(key, b"async-key");
                assert_eq!(value, b"async-value");
                assert_eq!(deleted, b"removed-key");
            }
            other => panic!("unexpected async WAL entries: {:?}", other),
        }

        wal.truncate().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_wal_replay_discards_every_partial_batch_boundary() {
        let path = unique_wal_path("partial_batch");
        let frame = WalManager::encode_batch_record(&[
            WalEntry::Put(b"tx-key-1".to_vec(), b"tx-value-1".to_vec()),
            WalEntry::Delete(b"tx-key-2".to_vec()),
            WalEntry::Put(b"tx-key-3".to_vec(), b"tx-value-3".to_vec()),
        ])
        .unwrap();

        for cut in 1..frame.len() {
            let legacy_len = {
                let mut file = File::create(&path).unwrap();
                let legacy_len = write_raw_put_record(&mut file, b"legacy", b"complete");
                file.write_all(&frame[..cut]).unwrap();
                file.sync_all().unwrap();
                legacy_len
            };
            let file_len = std::fs::metadata(&path).unwrap().len();
            let file = File::open(&path).unwrap();
            let (entries, stats) =
                WalManager::replay_single_file(&path, file, file_len, true).unwrap();

            assert_eq!(entries.len(), 1, "partial cut at byte {}", cut);
            assert_eq!(stats.entry_count, 1, "partial cut at byte {}", cut);
            assert_eq!(stats.partial_tail_count, 1, "partial cut at byte {}", cut);
            assert_eq!(stats.valid_bytes, legacy_len, "partial cut at byte {}", cut);
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                legacy_len,
                "partial cut at byte {}",
                cut
            );
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_wal_replay_rejects_corrupt_batch_without_expanding_entries() {
        let path = unique_wal_path("corrupt_batch");
        let mut frame = WalManager::encode_batch_record(&[
            WalEntry::Put(b"tx-key-1".to_vec(), b"tx-value-1".to_vec()),
            WalEntry::Put(b"tx-key-2".to_vec(), b"tx-value-2".to_vec()),
        ])
        .unwrap();
        let corrupt_offset = WAL_BATCH_HEADER_BYTES as usize + WAL_BATCH_ENTRY_COUNT_BYTES + 2;
        frame[corrupt_offset] ^= 0x40;
        std::fs::write(&path, &frame).unwrap();
        let file_len = frame.len() as u64;
        let file = File::open(&path).unwrap();

        let error = WalManager::replay_single_file(&path, file, file_len, true).unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), file_len);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_wal_replay_supports_mixed_legacy_and_framed_records() {
        let path = unique_wal_path("mixed_compat");
        {
            let mut file = File::create(&path).unwrap();
            write_raw_put_record(&mut file, b"legacy-key", b"legacy-value");
            let frame = WalManager::encode_batch_record(&[
                WalEntry::Delete(b"legacy-key".to_vec()),
                WalEntry::Put(b"new-key".to_vec(), b"new-value".to_vec()),
            ])
            .unwrap();
            file.write_all(&frame).unwrap();
            file.sync_all().unwrap();
        }

        let wal = WalManager::new(&path).unwrap();
        let entries = wal.replay().unwrap();
        match entries.as_slice() {
            [WalEntry::Put(key1, value1), WalEntry::Delete(key2), WalEntry::Put(key3, value3)] => {
                assert_eq!(key1, b"legacy-key");
                assert_eq!(value1, b"legacy-value");
                assert_eq!(key2, b"legacy-key");
                assert_eq!(key3, b"new-key");
                assert_eq!(value3, b"new-value");
            }
            other => panic!("unexpected mixed WAL entries: {:?}", other),
        }

        wal.truncate().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_wal_replay_rejects_partial_non_final_segment() {
        let path = unique_wal_path("partial_old_segment");
        let segment_one = segment_path(&path, 1);
        let frame = WalManager::encode_batch_record(&[
            WalEntry::Put(b"incomplete-key".to_vec(), b"incomplete-value".to_vec()),
            WalEntry::Delete(b"second-key".to_vec()),
        ])
        .unwrap();
        std::fs::write(&path, &frame[..frame.len() - 1]).unwrap();
        {
            let mut file = File::create(&segment_one).unwrap();
            write_raw_put_record(&mut file, b"later-key", b"later-value");
            file.sync_all().unwrap();
        }
        let original_len = std::fs::metadata(&path).unwrap().len();

        let wal = WalManager::new(&path).unwrap();
        let error = wal.replay().unwrap_err();
        assert!(error.to_string().contains("non-final segment"));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), original_len);

        wal.truncate().unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&segment_one);
    }

    #[test]
    fn test_wal_open_rejects_segment_sequence_gap() {
        let path = unique_wal_path("segment_gap");
        let segment_two = segment_path(&path, 2);
        File::create(&path).unwrap().sync_all().unwrap();
        File::create(&segment_two).unwrap().sync_all().unwrap();

        let error = match WalManager::new(&path) {
            Ok(_) => panic!("WAL with a missing segment unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("segment sequence gap"));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&segment_two);
    }

    #[test]
    fn test_wal_replay_entry_capacity_hint_is_bounded() {
        assert_eq!(WalManager::replay_entry_capacity_hint(0), 0);
        assert_eq!(
            WalManager::replay_entry_capacity_hint(MIN_WAL_REPLAY_RECORD_BYTES),
            1
        );
        assert_eq!(
            WalManager::replay_entry_capacity_hint(MIN_WAL_REPLAY_RECORD_BYTES * 3),
            3
        );
        assert_eq!(
            WalManager::replay_entry_capacity_hint(MAX_SEGMENT_SIZE),
            MAX_WAL_REPLAY_PREALLOC_ENTRIES
        );
    }

    #[test]
    fn test_wal_replay_reserves_total_entries_from_segment_hint() {
        let path = format!("test_wal_replay_capacity_{}.wal", std::process::id());
        let wal = WalManager::new(&path).unwrap();
        wal.append_batch(&[
            WalEntry::Put(b"key1".to_vec(), b"val1".to_vec()),
            WalEntry::Delete(b"key1".to_vec()),
            WalEntry::Put(b"key2".to_vec(), b"val2".to_vec()),
        ])
        .unwrap();
        let file_len = std::fs::metadata(&path).unwrap().len();
        let expected_capacity = WalManager::replay_entry_capacity_hint(file_len);

        let wal2 = WalManager::new(&path).unwrap();
        let entries = wal2.replay().unwrap();

        assert_eq!(entries.len(), 3);
        assert!(entries.capacity() >= expected_capacity);

        wal2.truncate().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_wal_replay_metrics_record_entries_and_partial_tail() {
        let path = format!(
            "test_wal_replay_metrics_{}_{}.wal",
            std::process::id(),
            uuid::Uuid::new_v4()
        );
        {
            let mut file = File::create(&path).unwrap();
            write_raw_put_record(&mut file, b"key", b"value");
            file.write_all(&[1u8, 0u8]).unwrap();
            file.flush().unwrap();
        }

        let metrics = &crate::monitor::GLOBAL_METRICS;
        let replay_before = metrics.wal_replay_count.load(Ordering::Relaxed);
        let entries_before = metrics.wal_replay_entry_count.load(Ordering::Relaxed);
        let puts_before = metrics.wal_replay_put_count.load(Ordering::Relaxed);
        let valid_bytes_before = metrics.wal_replay_valid_bytes.load(Ordering::Relaxed);
        let partial_before = metrics
            .wal_replay_partial_tail_count
            .load(Ordering::Relaxed);
        let truncate_before = metrics.wal_replay_truncate_count.load(Ordering::Relaxed);

        let wal = WalManager::new(&path).unwrap();
        let summary = wal.replay_with_summary().unwrap();
        assert_eq!(summary.entries.len(), 1);
        assert_eq!(
            summary.cursor,
            Some(WalReplayCursor {
                segment_id: 0,
                offset: 17,
            })
        );
        assert_eq!(summary.stats.bytes, 19);
        assert_eq!(summary.stats.valid_bytes, 17);
        assert_eq!(summary.stats.last_segment_id, 0);
        assert_eq!(summary.stats.last_valid_offset, 17);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 17);

        assert!(
            metrics
                .wal_replay_count
                .load(Ordering::Relaxed)
                .saturating_sub(replay_before)
                >= 1
        );
        assert!(
            metrics
                .wal_replay_entry_count
                .load(Ordering::Relaxed)
                .saturating_sub(entries_before)
                >= 1
        );
        assert!(
            metrics
                .wal_replay_put_count
                .load(Ordering::Relaxed)
                .saturating_sub(puts_before)
                >= 1
        );
        assert!(
            metrics
                .wal_replay_valid_bytes
                .load(Ordering::Relaxed)
                .saturating_sub(valid_bytes_before)
                >= 17
        );
        assert!(
            metrics
                .wal_replay_partial_tail_count
                .load(Ordering::Relaxed)
                .saturating_sub(partial_before)
                >= 1
        );
        assert!(
            metrics
                .wal_replay_truncate_count
                .load(Ordering::Relaxed)
                .saturating_sub(truncate_before)
                >= 1
        );

        wal.truncate().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_wal_replay_summary_tracks_last_segment_cursor() {
        let path = format!(
            "test_wal_replay_cursor_{}_{}.wal",
            std::process::id(),
            uuid::Uuid::new_v4()
        );
        let seg1_path = segment_path(&path, 1);
        let base_bytes = {
            let mut file = File::create(&path).unwrap();
            let bytes = write_raw_put_record(&mut file, b"base", b"value0");
            file.flush().unwrap();
            bytes
        };
        let seg1_bytes = {
            let mut file = File::create(&seg1_path).unwrap();
            let bytes = write_raw_put_record(&mut file, b"seg1", b"value1");
            file.flush().unwrap();
            bytes
        };

        let wal = WalManager::new(&path).unwrap();
        let summary = wal.replay_with_summary().unwrap();

        assert_eq!(summary.entries.len(), 2);
        assert_eq!(
            summary.cursor,
            Some(WalReplayCursor {
                segment_id: 1,
                offset: seg1_bytes,
            })
        );
        assert_eq!(summary.stats.segment_count, 2);
        assert_eq!(summary.stats.bytes, base_bytes + seg1_bytes);
        assert_eq!(summary.stats.valid_bytes, base_bytes + seg1_bytes);
        assert_eq!(summary.stats.last_segment_id, 1);
        assert_eq!(summary.stats.last_valid_offset, seg1_bytes);

        wal.truncate().unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&seg1_path);
    }

    #[test]
    fn test_wal_truncate_cleans_segments() {
        let path = format!("test_wal_trunc_{}.wal", std::process::id());
        let wal = WalManager::new(&path).unwrap();
        wal.append(&WalEntry::Put(b"k".to_vec(), b"v".to_vec()))
            .unwrap();
        wal.truncate().unwrap();

        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), 0);

        let _ = std::fs::remove_file(&path);
    }
}
