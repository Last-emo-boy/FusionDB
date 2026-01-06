use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use parking_lot::RwLock; // Use parking_lot RwLock
use crate::common::{Result, FusionError};
use super::{Storage, Transaction};
use super::wal::{WalManager, WalEntry};

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
                WalEntry::Put(k, v) => { data.insert(k, v); },
                WalEntry::Delete(k) => { data.remove(&k); },
            }
        }
        println!("Recovered {} entries from WAL", data.len());

        Ok(Self {
            data: Arc::new(RwLock::new(data)),
            wal: Arc::new(wal),
        })
    }

    pub fn create_snapshot(&self) -> Result<()> {
        let data = self.data.read();
        self.wal.create_checkpoint(data.iter().map(|(k, v)| (k.clone(), v.clone())))?;
        Ok(())
    }

    // Helper methods for transaction to access data
    pub(crate) fn get_internal(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let data = self.data.read();
        Ok(data.get(key).cloned())
    }

    pub(crate) fn scan_prefix_internal(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let data = self.data.read();
        let mut result = Vec::new();
        for (k, v) in data.range(prefix.to_vec()..) {
            if !k.starts_with(prefix) {
                break;
            }
            result.push((k.clone(), v.clone()));
        }
        Ok(result)
    }
}

pub struct MemoryTransaction {
    storage: MemoryStorage,
    write_buffer: HashMap<Vec<u8>, Option<Vec<u8>>>, // None means deleted
}

impl MemoryTransaction {
    pub fn new(storage: MemoryStorage) -> Self {
        Self {
            storage,
            write_buffer: HashMap::new(),
        }
    }
}

#[async_trait]
impl Transaction for MemoryTransaction {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. Check write buffer
        if let Some(val) = self.write_buffer.get(key) {
            return Ok(val.clone());
        }
        
        // 2. Check storage
        self.storage.get_internal(key)
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
        let results = self.storage.scan_prefix_internal(prefix)?;
        // TODO: Merge write buffer logic if needed (simplified for now)
        Ok(results)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        // 1. Prepare WAL Batch
        let mut wal_entries = Vec::with_capacity(self.write_buffer.len());
        for (k, v) in &self.write_buffer {
            match v {
                Some(val) => { 
                    wal_entries.push(WalEntry::Put(k.clone(), val.clone()));
                },
                None => { 
                    wal_entries.push(WalEntry::Delete(k.clone()));
                },
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
                Some(val) => { data.insert(k, val); },
                None => { data.remove(&k); },
            }
        }
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
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
