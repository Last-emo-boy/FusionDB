use super::wal::{WalEntry, WalManager};
use super::{Storage, Transaction};
use crate::common::Result;
use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

const TS_SIZE: usize = 8;

#[derive(Clone)]
pub struct MvccStorage {
    // Map: (Key + Timestamp) -> Value (with flag)
    // Key encoding: UserKey + (u64::MAX - Version).to_be_bytes()
    // Value encoding: [Flag(1b)] + [Data]
    data: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
    wal: Arc<WalManager>,
    // Global Logical Clock
    current_ts: Arc<AtomicU64>,
}

impl MvccStorage {
    pub fn new(path: &str) -> Result<Self> {
        let wal = WalManager::new(path)?;
        let data = SkipMap::new();
        let current_ts = AtomicU64::new(0);

        // Replay WAL
        // For MVCC, we need to recover the latest timestamp as well.
        let entries = wal.replay()?;
        let mut max_ts = 0;

        for entry in entries {
            // In a real MVCC WAL, entries should probably have timestamps.
            // But for compatibility with existing WAL (which doesn't have TS),
            // we can assign strictly increasing TS during replay.
            // Or, if we change WAL format, we can persist TS.
            // For now, let's just assign new TS for each entry to rebuild state.
            // This is acceptable for crash recovery as long as order is preserved.

            max_ts += 1;
            match entry {
                WalEntry::Put(k, v) => {
                    let key = Self::encode_key(&k, max_ts);
                    let val = Self::encode_value(true, &v);
                    data.insert(key, val);
                }
                WalEntry::Delete(k) => {
                    let key = Self::encode_key(&k, max_ts);
                    let val = Self::encode_value(false, &[]);
                    data.insert(key, val);
                }
            }
        }

        current_ts.store(max_ts, Ordering::SeqCst);

        println!(
            "Recovered {} operations from WAL. Current TS: {}",
            data.len(),
            max_ts
        );

        Ok(Self {
            data: Arc::new(data),
            wal: Arc::new(wal),
            current_ts: Arc::new(current_ts),
        })
    }

    fn encode_key(user_key: &[u8], ts: u64) -> Vec<u8> {
        let mut k = Vec::with_capacity(user_key.len() + TS_SIZE);
        k.extend_from_slice(user_key);
        // Sort order: Key ASC, Version DESC (MAX - ts)
        k.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
        k
    }

    fn decode_key(internal_key: &[u8]) -> (&[u8], u64) {
        let len = internal_key.len();
        if len < TS_SIZE {
            return (internal_key, 0); // Should not happen
        }
        let (k, ts_bytes) = internal_key.split_at(len - TS_SIZE);
        let inverted_ts = u64::from_be_bytes(ts_bytes.try_into().unwrap());
        (k, u64::MAX - inverted_ts)
    }

    fn encode_value(is_put: bool, data: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(1 + data.len());
        v.push(if is_put { 1 } else { 0 });
        v.extend_from_slice(data);
        v
    }

    fn decode_value(data: &[u8]) -> (bool, &[u8]) {
        if data.is_empty() {
            return (false, &[]);
        }
        (data[0] == 1, &data[1..])
    }

    // Read logic: Find latest version <= read_ts
    fn get_version(&self, key: &[u8], read_ts: u64) -> Option<Vec<u8>> {
        // We want the first entry where InternalKey >= Key + (MAX - read_ts)
        // Because keys are (Key, MAX-V1), (Key, MAX-V2)... where V1 > V2.
        // MAX-V1 < MAX-V2.
        // So (Key, MAX-V_latest) is the smallest key for this user key.
        // We start scanning from Key (effectively Key + 0000...)
        // We iterate until we find a match or key changes.

        // Wait, "Key + (MAX - read_ts)"?
        // If we have versions 100, 90, 80.
        // Keys: K+(MAX-100), K+(MAX-90), K+(MAX-80).
        // If read_ts = 95. We want version 90.
        // MAX-100 < MAX-95 < MAX-90.
        // We want the first entry where Version <= read_ts.
        // i.e., (MAX - Version) >= (MAX - read_ts).
        // So we want InternalKey >= Key + (MAX - read_ts).

        let search_key = Self::encode_key(key, read_ts);

        // SkipMap range is (Bound, Bound).
        // We use range(search_key..)
        let entry = self.data.range(search_key..).next();

        if let Some(ent) = entry {
            let (k, _ts) = Self::decode_key(ent.key());
            if k == key {
                // Found a version for this key.
                // Since we started searching at MAX-read_ts, this version implies
                // MAX-Version >= MAX-read_ts  => Version <= read_ts.
                // And since it's the first one, it's the largest Version <= read_ts (because smaller MAX-Version comes first).
                // Wait.
                // K+(MAX-100) < K+(MAX-90).
                // If read_ts=95, search_key = K+(MAX-95).
                // range(K+(MAX-95)..) will skip K+(MAX-100).
                // It will land on K+(MAX-90).
                // Version 90 <= 95. Correct.

                let (is_put, val) = Self::decode_value(ent.value());
                if is_put {
                    return Some(val.to_vec());
                } else {
                    return None; // Tombstone
                }
            }
        }

        None
    }
}

pub struct MvccTransaction {
    storage: MvccStorage,
    write_buffer: Vec<(Vec<u8>, Option<Vec<u8>>)>, // Key, Value (None=Delete)
    read_ts: u64,
}

#[async_trait]
impl Transaction for MvccTransaction {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. Check write buffer (read-your-own-writes)
        // Scan backwards to find latest write in this txn
        for (k, v) in self.write_buffer.iter().rev() {
            if k == key {
                return Ok(v.clone());
            }
        }

        // 2. Read from storage snapshot
        Ok(self.storage.get_version(key, self.read_ts))
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_buffer.push((key.to_vec(), Some(value.to_vec())));
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write_buffer.push((key.to_vec(), None));
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Optimization: Stream and filter directly without intermediate HashMap
        // SkipMap is already sorted by Key ASC, Version DESC.
        // So for a given Key, we encounter the newest version first.

        let mut storage_res = Vec::new();
        let iter_key = Vec::from(prefix);

        let mut last_key: Option<Vec<u8>> = None;

        for entry in self.storage.data.range(iter_key..) {
            let (k, ts) = MvccStorage::decode_key(entry.key());
            if !k.starts_with(prefix) {
                break;
            }

            // Check if we already found a visible version for this key
            if let Some(last) = &last_key {
                if last == k {
                    continue; // Already processed the latest visible version for this key
                }
            }

            // Check visibility
            if ts <= self.read_ts {
                // Found the latest visible version
                let (is_put, val) = MvccStorage::decode_value(entry.value());
                if is_put {
                    storage_res.push((k.to_vec(), val.to_vec()));
                }
                // If deleted, we effectively "found" it (as deleted), so we shouldn't look for older versions.
                // So we update last_key regardless of is_put.
                last_key = Some(k.to_vec());
            }
        }

        // 2. Write Buffer Overlay
        // Since write buffer is usually small in this system (per request), we can just overlay.
        // If write buffer is large, this is O(N*M).
        // For correctness with minimal code change:
        // Put storage results into a Map if write buffer is present, or just update the list.

        if self.write_buffer.is_empty() {
            return Ok(storage_res);
        }

        // Merge logic
        let mut result_map: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
            storage_res.into_iter().collect();

        for (k, v) in &self.write_buffer {
            if k.starts_with(prefix) {
                match v {
                    Some(val) => {
                        result_map.insert(k.clone(), val.clone());
                    }
                    None => {
                        result_map.remove(k);
                    }
                }
            }
        }

        Ok(result_map.into_iter().collect())
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        // 1. Get Commit Timestamp
        let commit_ts = self.storage.current_ts.fetch_add(1, Ordering::SeqCst) + 1;

        // 2. Prepare WAL entries
        let mut wal_entries = Vec::with_capacity(self.write_buffer.len());
        for (k, v) in &self.write_buffer {
            match v {
                Some(val) => wal_entries.push(WalEntry::Put(k.clone(), val.clone())),
                None => wal_entries.push(WalEntry::Delete(k.clone())),
            }
        }

        // 3. Write WAL (Group Commit handled by WalManager)
        self.storage.wal.append_batch_async(wal_entries).await?;

        // 4. Apply to Memory (SkipMap)
        for (k, v) in self.write_buffer {
            let key = MvccStorage::encode_key(&k, commit_ts);
            let val = match v {
                Some(data) => MvccStorage::encode_value(true, &data),
                None => MvccStorage::encode_value(false, &[]),
            };
            self.storage.data.insert(key, val);
        }

        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Storage for MvccStorage {
    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        let read_ts = self.current_ts.load(Ordering::SeqCst);
        Ok(Box::new(MvccTransaction {
            storage: self.clone(),
            write_buffer: Vec::new(),
            read_ts,
        }))
    }

    async fn create_snapshot(&self) -> Result<()> {
        // MVCC snapshot is implicit.
        // But for persistence checkpointing, we need to iterate all latest keys.
        // Implementation: Scan all keys, pick latest for each, write to snapshot file.
        // TODO for next step.
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
