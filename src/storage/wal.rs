use crate::common::{FusionError, Result};
use crate::monitor;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::sync::oneshot;

const MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64 MB per segment
const MIN_WAL_REPLAY_RECORD_BYTES: u64 = 1 + 4;
const MAX_WAL_REPLAY_PREALLOC_ENTRIES: usize = 8192;

#[derive(Debug)]
pub enum WalEntry {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
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
        // Flush current
        if let Some(ref mut w) = self.writer {
            w.flush().map_err(WalManager::io_err)?;
        }
        self.segment_id += 1;
        self.segment_size = 0;
        let new_path = segment_path(&self.base_path, self.segment_id);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)
            .map_err(|e| {
                FusionError::Storage(format!("Failed to open WAL segment {}: {}", new_path, e))
            })?;
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
        format!("{}.seg.{}", base, id)
    }
}

fn wal_segment_list() -> Vec<(u64, String)> {
    Vec::with_capacity(1)
}

/// Find all existing WAL segment files for a base path, sorted by segment ID.
fn find_segments(base: &str) -> Vec<(u64, String)> {
    let mut segments = wal_segment_list();

    // Check base file (segment 0)
    if std::path::Path::new(base).exists() {
        segments.push((0, base.to_string()));
    }

    // Check segment files: {base}.seg.{N}
    let parent = std::path::Path::new(base)
        .parent()
        .unwrap_or(std::path::Path::new("."));
    let base_name = std::path::Path::new(base)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let prefix = format!("{}.seg.", base_name);

    if let Ok(entries) = fs::read_dir(parent) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(suffix) = name.strip_prefix(&prefix) {
                if let Ok(id) = suffix.parse::<u64>() {
                    segments.push((id, entry.path().to_string_lossy().to_string()));
                }
            }
        }
    }

    segments.sort_by_key(|(id, _)| *id);
    segments
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
        let segments = find_segments(path);
        let (active_id, active_path) = if segments.is_empty() {
            (0u64, path.to_string())
        } else {
            segments.last().unwrap().clone()
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)
            .map_err(|e| FusionError::Storage(format!("Failed to open WAL: {}", e)))?;

        let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);

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
                                for entry in entries {
                                    match Self::write_entry(writer, entry) {
                                        Ok(n) => batch_bytes += n as u64,
                                        Err(e) => {
                                            write_result = Err(e);
                                            break;
                                        }
                                    }
                                }
                                if write_result.is_err() {
                                    break;
                                }
                            }

                            if write_result.is_ok() {
                                if let Err(e) = writer.flush().map_err(Self::io_err) {
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

    /// Write a single entry. Returns bytes written on success.
    fn write_entry(writer: &mut BufWriter<File>, entry: &WalEntry) -> Result<usize> {
        match entry {
            WalEntry::Put(k, v) => {
                writer.write_all(&[1u8]).map_err(Self::io_err)?;
                writer
                    .write_all(&(k.len() as u32).to_le_bytes())
                    .map_err(Self::io_err)?;
                writer.write_all(k).map_err(Self::io_err)?;
                writer
                    .write_all(&(v.len() as u32).to_le_bytes())
                    .map_err(Self::io_err)?;
                writer.write_all(v).map_err(Self::io_err)?;
                let n = 1 + 4 + k.len() + 4 + v.len();
                monitor::add_wal_bytes(n as u64);
                Ok(n)
            }
            WalEntry::Delete(k) => {
                writer.write_all(&[2u8]).map_err(Self::io_err)?;
                writer
                    .write_all(&(k.len() as u32).to_le_bytes())
                    .map_err(Self::io_err)?;
                writer.write_all(k).map_err(Self::io_err)?;
                let n = 1 + 4 + k.len();
                monitor::add_wal_bytes(n as u64);
                Ok(n)
            }
        }
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
        let _guard = self
            .state
            .lock()
            .map_err(|_| FusionError::Storage("WAL Lock poisoned".to_string()))?;

        let segments = find_segments(&self.path);
        let mut all_entries = Vec::new();

        for (seg_id, seg_path) in &segments {
            let file = match File::open(seg_path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("WAL Replay: skipping segment {}: {}", seg_path, e);
                    continue;
                }
            };
            let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
            if file_len == 0 {
                continue;
            }
            all_entries.reserve(Self::replay_entry_capacity_hint(file_len));
            let entries = Self::replay_single_file(seg_path, file_len)?;
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

        Ok(all_entries)
    }

    /// Replay a single WAL file, truncating partial records at the end.
    fn replay_single_file(path: &str, file_len: u64) -> Result<Vec<WalEntry>> {
        let file = File::open(path).map_err(|e| {
            FusionError::Storage(format!("Failed to open WAL segment for replay: {}", e))
        })?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::with_capacity(Self::replay_entry_capacity_hint(file_len));
        let mut valid_pos = 0u64;

        loop {
            let mut opcode = [0u8; 1];
            match reader.read_exact(&mut opcode) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(FusionError::Storage(format!("WAL Replay Error: {}", e))),
            }

            let mut record_len = 1usize;

            let read_u32 = |r: &mut BufReader<File>, len: &mut usize| -> Result<u32> {
                let mut buf = [0u8; 4];
                r.read_exact(&mut buf).map_err(|e| {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        FusionError::Storage("Partial Record".to_string())
                    } else {
                        FusionError::Storage(format!("WAL Read Error: {}", e))
                    }
                })?;
                *len += 4;
                Ok(u32::from_le_bytes(buf))
            };

            let read_vec =
                |r: &mut BufReader<File>, n: usize, len: &mut usize| -> Result<Vec<u8>> {
                    let mut buf = vec![0u8; n];
                    r.read_exact(&mut buf).map_err(|e| {
                        if e.kind() == io::ErrorKind::UnexpectedEof {
                            FusionError::Storage("Partial Record".to_string())
                        } else {
                            FusionError::Storage(format!("WAL Read Error: {}", e))
                        }
                    })?;
                    *len += n;
                    Ok(buf)
                };

            let res = match opcode[0] {
                1 => (|| -> Result<WalEntry> {
                    let kl = read_u32(&mut reader, &mut record_len)? as usize;
                    let k = read_vec(&mut reader, kl, &mut record_len)?;
                    let vl = read_u32(&mut reader, &mut record_len)? as usize;
                    let v = read_vec(&mut reader, vl, &mut record_len)?;
                    Ok(WalEntry::Put(k, v))
                })(),
                2 => (|| -> Result<WalEntry> {
                    let kl = read_u32(&mut reader, &mut record_len)? as usize;
                    let k = read_vec(&mut reader, kl, &mut record_len)?;
                    Ok(WalEntry::Delete(k))
                })(),
                _ => {
                    return Err(FusionError::Storage(format!(
                        "Unknown WAL OpCode: {}",
                        opcode[0]
                    )))
                }
            };

            match res {
                Ok(entry) => {
                    entries.push(entry);
                    valid_pos += record_len as u64;
                }
                Err(FusionError::Storage(msg)) if msg == "Partial Record" => {
                    println!("WAL Replay: partial record at end of {}. Truncating.", path);
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        // Truncate partial tail if needed
        if valid_pos < file_len {
            let _ = OpenOptions::new()
                .write(true)
                .open(path)
                .and_then(|f| f.set_len(valid_pos));
        }

        Ok(entries)
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

        // Close current writer
        ws.writer = None;

        // Delete all segment files
        let segments = find_segments(&self.path);
        for (_, seg_path) in &segments {
            let _ = fs::remove_file(seg_path);
        }

        // Re-create base file and reset state
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| FusionError::Storage(format!("Failed to recreate WAL: {}", e)))?;

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
        let snap_path = format!("{}.snap", self.path);
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

        {
            let mut ws = self
                .state
                .lock()
                .map_err(|_| FusionError::Storage("WAL Lock poisoned".to_string()))?;

            // Delete all old segments first
            let old_segments = find_segments(&self.path);
            ws.writer = None;
            for (_, seg_path) in &old_segments {
                let _ = fs::remove_file(seg_path);
            }

            // Rename snapshot to base path
            if let Err(e) = fs::rename(&snap_path, &self.path) {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)
                    .map_err(Self::io_err)?;
                ws.writer = Some(BufWriter::new(file));
                return Err(Self::io_err(e));
            }

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
        find_segments(&self.path).len()
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
    fn test_find_segments_empty() {
        let segments = find_segments("nonexistent_test_path_xyz.wal");
        assert!(segments.is_empty());
    }

    #[test]
    fn test_wal_segment_list_preallocates_base_segment() {
        let segments = wal_segment_list();
        assert!(segments.capacity() >= 1);
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
