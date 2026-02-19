use crate::common::{FusionError, Result};
use crate::monitor;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
// use std::time::Duration;
use tokio::sync::oneshot;

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

pub struct WalManager {
    file: Arc<Mutex<Option<BufWriter<File>>>>,
    tx: mpsc::Sender<WalJob>,
    path: String,
}

impl WalManager {
    pub fn new(path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| FusionError::Storage(format!("Failed to open WAL: {}", e)))?;

        let file_mutex = Arc::new(Mutex::new(Some(BufWriter::new(file))));
        let (tx, rx) = mpsc::channel();

        let writer_file = file_mutex.clone();

        thread::Builder::new()
            .name("wal-writer".to_string())
            .spawn(move || {
                Self::wal_writer_loop(rx, writer_file);
            })
            .map_err(|e| FusionError::Storage(format!("Failed to spawn WAL thread: {}", e)))?;

        Ok(Self {
            file: file_mutex,
            tx,
            path: path.to_string(),
        })
    }

    fn wal_writer_loop(rx: Receiver<WalJob>, file_mutex: Arc<Mutex<Option<BufWriter<File>>>>) {
        loop {
            // Blocking wait for the first job
            let first_job = match rx.recv() {
                Ok(job) => job,
                Err(_) => break, // Channel closed, exit
            };

            let mut batch = vec![first_job];

            // Try to collect more jobs (Group Commit)
            // Sleep briefly to allow more jobs to accumulate if we have high concurrency but are processing faster than arrival
            // This is a tradeoff: slightly higher latency for single requests, but much better throughput for many
            // Given the user report of performance degradation, maybe we are not batching enough.
            // Let's add a tiny sleep if we got a job, to see if more arrive.
            // But doing this for *every* job adds latency.
            // Instead of sleep, just use try_recv loop. If it's empty, we write.

            for _ in 0..1000 {
                match rx.try_recv() {
                    Ok(job) => batch.push(job),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => break,
                }
            }

            // Write Batch
            // We use a block to scope the lock
            let mut write_result = Ok(());
            {
                if let Ok(mut writer_guard) = file_mutex.lock() {
                    if let Some(writer) = writer_guard.as_mut() {
                        for job in &batch {
                            let WalJob::Append { entries, .. } = job;
                            for entry in entries {
                                if let Err(e) = Self::write_entry(writer, entry) {
                                    write_result = Err(e);
                                    break;
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
                                monitor::inc_wal_write(); // 1 sync for the batch
                            }
                        }
                    } else {
                        write_result = Err(FusionError::Storage("WAL closed".to_string()));
                    }
                } else {
                    write_result = Err(FusionError::Storage("WAL Lock poisoned".to_string()));
                }
            }

            // Notify clients
            for job in batch {
                match job {
                    WalJob::Append { resp, .. } => {
                        // Clone the error if there is one
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

    fn write_entry(writer: &mut BufWriter<File>, entry: &WalEntry) -> Result<()> {
        match entry {
            WalEntry::Put(k, v) => {
                writer.write_all(&[1u8]).map_err(Self::io_err)?; // OpCode 1: Put

                let k_len = (k.len() as u32).to_le_bytes();
                writer.write_all(&k_len).map_err(Self::io_err)?;
                writer.write_all(k).map_err(Self::io_err)?;

                let v_len = (v.len() as u32).to_le_bytes();
                writer.write_all(&v_len).map_err(Self::io_err)?;
                writer.write_all(v).map_err(Self::io_err)?;

                monitor::add_wal_bytes((1 + 4 + k.len() + 4 + v.len()) as u64);
            }
            WalEntry::Delete(k) => {
                writer.write_all(&[2u8]).map_err(Self::io_err)?; // OpCode 2: Delete

                let k_len = (k.len() as u32).to_le_bytes();
                writer.write_all(&k_len).map_err(Self::io_err)?;
                writer.write_all(k).map_err(Self::io_err)?;

                monitor::add_wal_bytes((1 + 4 + k.len()) as u64);
            }
        }
        Ok(())
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
        let entries_vec: Vec<WalEntry> = entries
            .iter()
            .map(|e| match e {
                WalEntry::Put(k, v) => WalEntry::Put(k.clone(), v.clone()),
                WalEntry::Delete(k) => WalEntry::Delete(k.clone()),
            })
            .collect();

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

    pub fn replay(&self) -> Result<Vec<WalEntry>> {
        // Replay runs before any writes, so it's safe to read the file directly.
        // But we should take the lock just in case.
        let mut guard = self
            .file
            .lock()
            .map_err(|_| FusionError::Storage("WAL Lock poisoned".to_string()))?;

        let mut file = File::open(&self.path)
            .map_err(|e| FusionError::Storage(format!("Failed to open WAL for replay: {}", e)))?;
        
        let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut valid_pos = 0;

        loop {
            // Check if we are at EOF before reading
            // This prevents UnexpectedEof if file ends cleanly
            // But BufReader doesn't have EOF check easily without fill_buf.
            // We rely on read_exact returning UnexpectedEof for the first byte.

            let mut opcode = [0u8; 1];
            match reader.read_exact(&mut opcode) {
                Ok(_) => {},
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(FusionError::Storage(format!("WAL Replay Error reading OpCode: {}", e))),
            }

            // We read OpCode (1 byte). Now try to read the rest.
            // If any of these fail with UnexpectedEof, it's a partial record.
            
            let mut current_record_len = 1; // OpCode

            let read_u32 = |r: &mut BufReader<File>, len_counter: &mut usize| -> Result<u32> {
                let mut buf = [0u8; 4];
                r.read_exact(&mut buf).map_err(|e| {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        FusionError::Storage("Partial Record".to_string())
                    } else {
                        FusionError::Storage(format!("WAL Read Error: {}", e))
                    }
                })?;
                *len_counter += 4;
                Ok(u32::from_le_bytes(buf))
            };

            let read_vec = |r: &mut BufReader<File>, len: usize, len_counter: &mut usize| -> Result<Vec<u8>> {
                let mut buf = vec![0u8; len];
                r.read_exact(&mut buf).map_err(|e| {
                     if e.kind() == io::ErrorKind::UnexpectedEof {
                        FusionError::Storage("Partial Record".to_string())
                    } else {
                        FusionError::Storage(format!("WAL Read Error: {}", e))
                    }
                })?;
                *len_counter += len;
                Ok(buf)
            };

            match opcode[0] {
                1 => {
                    // Put
                    let res = (|| -> Result<WalEntry> {
                        let k_len = read_u32(&mut reader, &mut current_record_len)? as usize;
                        let k = read_vec(&mut reader, k_len, &mut current_record_len)?;
                        let v_len = read_u32(&mut reader, &mut current_record_len)? as usize;
                        let v = read_vec(&mut reader, v_len, &mut current_record_len)?;
                        Ok(WalEntry::Put(k, v))
                    })();

                    match res {
                        Ok(entry) => {
                            entries.push(entry);
                            valid_pos += current_record_len as u64;
                        }
                        Err(FusionError::Storage(msg)) if msg == "Partial Record" => {
                            println!("WAL Replay: Found partial record at end. Truncating.");
                            break;
                        }
                        Err(e) => return Err(e),
                    }
                }
                2 => {
                    // Delete
                    let res = (|| -> Result<WalEntry> {
                         let k_len = read_u32(&mut reader, &mut current_record_len)? as usize;
                         let k = read_vec(&mut reader, k_len, &mut current_record_len)?;
                         Ok(WalEntry::Delete(k))
                    })();

                    match res {
                        Ok(entry) => {
                            entries.push(entry);
                            valid_pos += current_record_len as u64;
                        }
                        Err(FusionError::Storage(msg)) if msg == "Partial Record" => {
                             println!("WAL Replay: Found partial record at end. Truncating.");
                             break;
                        }
                        Err(e) => return Err(e),
                    }
                }
                _ => {
                    return Err(FusionError::Storage(format!(
                        "Unknown WAL OpCode: {}",
                        opcode[0]
                    )))
                }
            }
        }
        
        // If we found partial data (valid_pos < file_len), truncate the file
        if valid_pos < file_len {
            println!("Truncating WAL from {} to {} bytes", file_len, valid_pos);
            // We need to re-open or use the file handle?
            // BufReader took ownership. We can use the file path.
            // But we hold the lock on `guard` (which is the writer).
            // We should update the writer too?
            // The writer is in `self.file`. `guard` is the MutexGuard.
            // *guard is Option<BufWriter<File>>.
            
            // Drop reader to close file handle
            drop(reader);
            
            let file = OpenOptions::new()
                .write(true)
                .open(&self.path)
                .map_err(|e| FusionError::Storage(format!("Failed to open WAL for truncation: {}", e)))?;
            file.set_len(valid_pos).map_err(|e| FusionError::Storage(format!("Failed to truncate WAL: {}", e)))?;
            
            // Re-initialize the writer in the mutex
             let file_append = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .map_err(|e| FusionError::Storage(format!("Failed to re-open WAL: {}", e)))?;
            *guard = Some(BufWriter::new(file_append));
        }

        Ok(entries)
    }

    pub fn truncate(&self) -> Result<()> {
        let mut writer_guard = self
            .file
            .lock()
            .map_err(|_| FusionError::Storage("WAL Lock poisoned".to_string()))?;

        // Re-open with truncate
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| FusionError::Storage(format!("Failed to truncate WAL: {}", e)))?;

        *writer_guard = Some(BufWriter::new(file));
        Ok(())
    }

    pub fn create_checkpoint<I>(&self, iter: I) -> Result<()>
    where
        I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        // 1. Write to temporary file
        let snap_path = format!("{}.snap", self.path);
        let mut file = BufWriter::new(File::create(&snap_path).map_err(Self::io_err)?);

        for (k, v) in iter {
            // Write as WAL Put entries
            file.write_all(&[1u8]).map_err(Self::io_err)?; // OpCode 1: Put

            let k_len = (k.len() as u32).to_le_bytes();
            file.write_all(&k_len).map_err(Self::io_err)?;
            file.write_all(&k).map_err(Self::io_err)?;

            let v_len = (v.len() as u32).to_le_bytes();
            file.write_all(&v_len).map_err(Self::io_err)?;
            file.write_all(&v).map_err(Self::io_err)?;
        }
        file.flush().map_err(Self::io_err)?;

        // 2. Rename snapshot to WAL (Atomic replace)
        {
            let mut writer_guard = self
                .file
                .lock()
                .map_err(|_| FusionError::Storage("WAL Lock poisoned".to_string()))?;
            // Drop current writer to close the file handle
            *writer_guard = None;

            // Replace the file on disk
            if let Err(e) = std::fs::rename(&snap_path, &self.path) {
                // Try to restore writer if fail?
                // Re-open old path (might still be there if rename failed)
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)
                    .map_err(Self::io_err)?;
                *writer_guard = Some(BufWriter::new(file));
                return Err(Self::io_err(e));
            }

            // Re-open the file for appending
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .map_err(Self::io_err)?;
            *writer_guard = Some(BufWriter::new(file));
        }

        Ok(())
    }

    fn io_err(e: io::Error) -> FusionError {
        FusionError::Storage(format!("WAL IO Error: {}", e))
    }
}
