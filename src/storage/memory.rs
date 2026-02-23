/// In-memory storage engine for **testing only**.
/// For production use, prefer `FusionStorage` which provides MVCC, LSM-Tree, and WAL.
///
/// This engine stores data in a `BTreeMap` and uses a simple write-buffer in
/// transactions.  All scan/count/first/last operations correctly merge the
/// write buffer with the committed snapshot so that read-your-own-writes
/// semantics are honoured within a transaction.

use super::wal::{WalEntry, WalManager};
use super::{Storage, Transaction};
use crate::common::Result;
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct MemoryStorage {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    wal: Arc<WalManager>,
}

impl MemoryStorage {
    pub fn new(path: &str) -> Result<Self> {
        let wal = WalManager::new(path)?;
        let mut data = BTreeMap::new();

        // Replay WAL
        let entries = wal.replay()?;
        for entry in entries {
            match entry {
                WalEntry::Put(k, v) => {
                    data.insert(k, v);
                }
                WalEntry::Delete(k) => {
                    data.remove(&k);
                }
            }
        }

        Ok(Self {
            data: Arc::new(RwLock::new(data)),
            wal: Arc::new(wal),
        })
    }

    pub fn create_snapshot(&self) -> Result<()> {
        let data = self.data.read();
        self.wal
            .create_checkpoint(data.iter().map(|(k, v)| (k.clone(), v.clone())))?;
        Ok(())
    }
}

pub struct MemoryTransaction {
    storage: MemoryStorage,
    write_buffer: BTreeMap<Vec<u8>, Option<Vec<u8>>>, // None = tombstone
}

impl MemoryTransaction {
    pub fn new(storage: MemoryStorage) -> Self {
        Self {
            storage,
            write_buffer: BTreeMap::new(),
        }
    }

    /// Build a merged snapshot: committed data overlaid with the write buffer.
    /// Returns an iterator-like BTreeMap of live key-value pairs in the given
    /// range `[start, end)`.
    fn merged_range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let data = self.storage.data.read();

        // Merge: iterate both sources in order
        let mut storage_iter = data.range(start.to_vec()..end.to_vec()).peekable();
        let mut wb_iter = self.write_buffer.range(start.to_vec()..end.to_vec()).peekable();

        let mut result = Vec::new();
        loop {
            match (storage_iter.peek(), wb_iter.peek()) {
                (Some(&(sk, _)), Some(&(wk, _))) => {
                    match sk.cmp(wk) {
                        std::cmp::Ordering::Less => {
                            // Storage key comes first; only include if not shadowed by wb
                            if !self.write_buffer.contains_key(sk) {
                                result.push((sk.clone(), data.get(sk).unwrap().clone()));
                            }
                            storage_iter.next();
                        }
                        std::cmp::Ordering::Equal => {
                            // Write buffer wins
                            if let Some(val) = wb_iter.peek().and_then(|(_, v)| v.as_ref()) {
                                result.push((wk.clone(), val.clone()));
                            }
                            // else: tombstone, skip
                            storage_iter.next();
                            wb_iter.next();
                        }
                        std::cmp::Ordering::Greater => {
                            if let Some(val) = wb_iter.peek().and_then(|(_, v)| v.as_ref()) {
                                result.push((wk.clone(), val.clone()));
                            }
                            wb_iter.next();
                        }
                    }
                }
                (Some(&(sk, sv)), None) => {
                    if !self.write_buffer.contains_key(sk) {
                        result.push((sk.clone(), sv.clone()));
                    }
                    storage_iter.next();
                }
                (None, Some(&(wk, wv))) => {
                    if let Some(val) = wv {
                        result.push((wk.clone(), val.clone()));
                    }
                    wb_iter.next();
                }
                (None, None) => break,
            }
        }
        result
    }
}

#[async_trait]
impl Transaction for MemoryTransaction {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. Check write buffer first (read-your-own-writes)
        if let Some(val) = self.write_buffer.get(key) {
            return Ok(val.clone());
        }

        // 2. Check committed storage
        let data = self.storage.data.read();
        Ok(data.get(key).cloned())
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_buffer.insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write_buffer.insert(key.to_vec(), None);
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &[u8], limit: Option<usize>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Compute end bound for prefix range
        let mut end = prefix.to_vec();
        let mut found = false;
        while let Some(last) = end.last_mut() {
            if *last < 255 {
                *last += 1;
                found = true;
                break;
            }
            end.pop();
        }
        if !found {
            return Ok(Vec::new());
        }
        self.scan_range(prefix, &end, limit).await
    }

    async fn scan_range(&self, start: &[u8], end: &[u8], limit: Option<usize>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let merged = self.merged_range(start, end);
        let safe_limit = limit.unwrap_or(usize::MAX);
        Ok(merged.into_iter().take(safe_limit).collect())
    }

    async fn count_prefix(&self, prefix: &[u8]) -> Result<usize> {
        let results = self.scan_prefix(prefix, None).await?;
        Ok(results.len())
    }

    async fn first(&self, start: &[u8], end: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let results = self.scan_range(start, end, Some(1)).await?;
        Ok(results.into_iter().next())
    }

    async fn last(&self, start: &[u8], end: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let merged = self.merged_range(start, end);
        Ok(merged.into_iter().last())
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        // 1. Prepare WAL Batch
        let mut wal_entries = Vec::with_capacity(self.write_buffer.len());
        for (k, v) in &self.write_buffer {
            match v {
                Some(val) => {
                    wal_entries.push(WalEntry::Put(k.clone(), val.clone()));
                }
                None => {
                    wal_entries.push(WalEntry::Delete(k.clone()));
                }
            }
        }

        // 2. Write Batch to WAL
        if !wal_entries.is_empty() {
            self.storage.wal.append_batch_async(wal_entries).await?;
        }

        // 3. Apply to Memory
        let mut data = self.storage.data.write();
        for (k, v) in self.write_buffer {
            match v {
                Some(val) => {
                    data.insert(k, val);
                }
                None => {
                    data.remove(&k);
                }
            }
        }
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        Ok(Box::new(MemoryTransaction::new(self.clone())))
    }

    async fn create_snapshot(&self) -> Result<()> {
        self.create_snapshot()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_wal() -> String {
        format!("test_mem_{}.wal", uuid::Uuid::new_v4())
    }

    #[tokio::test]
    async fn test_get_put_commit() {
        let path = temp_wal();
        let storage = MemoryStorage::new(&path).unwrap();
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"key1", b"value1").await.unwrap();
            assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
            txn.commit().await.unwrap();
        }
        // Read after commit in new transaction
        let txn = storage.begin_transaction().await.unwrap();
        assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_rollback_discards_writes() {
        let path = temp_wal();
        let storage = MemoryStorage::new(&path).unwrap();
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"key1", b"value1").await.unwrap();
            txn.rollback().await.unwrap();
        }
        let txn = storage.begin_transaction().await.unwrap();
        assert_eq!(txn.get(b"key1").await.unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_delete_within_transaction() {
        let path = temp_wal();
        let storage = MemoryStorage::new(&path).unwrap();
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"k1", b"v1").await.unwrap();
            txn.commit().await.unwrap();
        }
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.delete(b"k1").await.unwrap();
            // Within this txn, k1 should be gone
            assert_eq!(txn.get(b"k1").await.unwrap(), None);
            txn.commit().await.unwrap();
        }
        let txn = storage.begin_transaction().await.unwrap();
        assert_eq!(txn.get(b"k1").await.unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_scan_prefix_merges_write_buffer() {
        let path = temp_wal();
        let storage = MemoryStorage::new(&path).unwrap();
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:t:1", b"row1").await.unwrap();
            txn.put(b"data:t:2", b"row2").await.unwrap();
            txn.commit().await.unwrap();
        }
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            // Add a new row and delete an existing one in the write buffer
            txn.put(b"data:t:3", b"row3").await.unwrap();
            txn.delete(b"data:t:1").await.unwrap();

            let results = txn.scan_prefix(b"data:t:", None).await.unwrap();
            // Should see row2 and row3, but NOT row1
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].0, b"data:t:2");
            assert_eq!(results[1].0, b"data:t:3");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_count_prefix_merges_write_buffer() {
        let path = temp_wal();
        let storage = MemoryStorage::new(&path).unwrap();
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:x:1", b"a").await.unwrap();
            txn.put(b"data:x:2", b"b").await.unwrap();
            txn.commit().await.unwrap();
        }
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:x:3", b"c").await.unwrap();
            txn.delete(b"data:x:1").await.unwrap();
            let count = txn.count_prefix(b"data:x:").await.unwrap();
            assert_eq!(count, 2); // x:2 and x:3
        }
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_first_and_last() {
        let path = temp_wal();
        let storage = MemoryStorage::new(&path).unwrap();
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"k1", b"v1").await.unwrap();
            txn.put(b"k2", b"v2").await.unwrap();
            txn.put(b"k3", b"v3").await.unwrap();
            txn.commit().await.unwrap();
        }
        let txn = storage.begin_transaction().await.unwrap();
        let first = txn.first(b"k", b"l").await.unwrap();
        assert_eq!(first, Some((b"k1".to_vec(), b"v1".to_vec())));
        let last = txn.last(b"k", b"l").await.unwrap();
        assert_eq!(last, Some((b"k3".to_vec(), b"v3".to_vec())));
        let _ = std::fs::remove_file(&path);
    }
}
