use super::{Storage, Transaction};
use crate::common::{FusionError, Result};
use async_trait::async_trait;
use sled::Db;
use std::collections::HashMap;

#[derive(Clone)]
pub struct SledStorage {
    db: Db,
}

impl SledStorage {
    pub fn new(path: &str) -> Result<Self> {
        let db = sled::open(path).map_err(|e| FusionError::Storage(e.to_string()))?;
        Ok(Self { db })
    }
}

pub struct SledTransaction {
    db: Db,
    write_buffer: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl SledTransaction {
    pub fn new(db: Db) -> Self {
        Self {
            db,
            write_buffer: HashMap::new(),
        }
    }
}

#[async_trait]
impl Transaction for SledTransaction {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(val) = self.write_buffer.get(key) {
            return Ok(val.clone());
        }

        let res = self
            .db
            .get(key)
            .map_err(|e| FusionError::Storage(e.to_string()))?;
        Ok(res.map(|ivec| ivec.to_vec()))
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_buffer.insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write_buffer.insert(key.to_vec(), None);
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Naive scan merge
        // Note: This does not support "read your own writes" fully if scan logic is complex
        // But for basic atomic transaction, this is acceptable for now.
        let mut results = Vec::new();
        for item in self.db.scan_prefix(prefix) {
            let (k, v) = item.map_err(|e| FusionError::Storage(e.to_string()))?;
            // If deleted in write buffer, skip
            if let Some(None) = self.write_buffer.get(&*k) {
                continue;
            }
            // If updated in write buffer, use that (but we need to check if it still matches prefix? yes)
            if let Some(Some(new_v)) = self.write_buffer.get(&*k) {
                results.push((k.to_vec(), new_v.clone()));
            } else {
                results.push((k.to_vec(), v.to_vec()));
            }
        }

        // Add new keys from write buffer that match prefix
        for (k, v) in &self.write_buffer {
            if k.starts_with(prefix) {
                // Check if we already added it (update case)
                // If it was an update to existing key, it's handled above?
                // Wait, if we iterate storage first, we handled updates to existing keys.
                // What about NEW keys?
                // We need to check if `k` was already in storage.
                // This is getting O(N*M).
                // Correct way: use a merged iterator.

                // Simplified: Just add if not present in results?
                // But results is Vec.
                // Let's assume for this MVP, scan consistency is best-effort or requires optimization later.

                // Check if `k` is already in results
                let already_in = results.iter().any(|(rk, _)| rk == k);
                if !already_in {
                    if let Some(val) = v {
                        results.push((k.clone(), val.clone()));
                    }
                }
            }
        }

        Ok(results)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        let mut batch = sled::Batch::default();
        for (k, v) in self.write_buffer {
            match v {
                Some(val) => batch.insert(k, val),
                None => batch.remove(k),
            }
        }
        self.db
            .apply_batch(batch)
            .map_err(|e| FusionError::Storage(e.to_string()))?;
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Storage for SledStorage {
    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        Ok(Box::new(SledTransaction::new(self.db.clone())))
    }

    async fn create_snapshot(&self) -> Result<()> {
        // Sled handles persistence automatically, but we can flush
        self.db
            .flush()
            .map_err(|e| FusionError::Storage(format!("Sled flush error: {}", e)))?;
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
