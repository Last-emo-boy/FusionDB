use async_trait::async_trait;
use crate::common::Result;

pub mod memory;
pub mod mvcc;
pub mod lsm;
pub mod fusion;
pub mod columnar;
pub mod sled_store;
pub mod sstable;
pub mod inverted_index;
pub mod wal;
pub mod vector_index;

#[async_trait]
pub trait Transaction: Send + Sync {
    /// Get a value by key (from write buffer or storage)
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Put a key-value pair (into write buffer)
    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key (tombstone in write buffer)
    async fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Scan keys with a prefix (merge storage and write buffer)
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Commit the transaction
    async fn commit(self: Box<Self>) -> Result<()>;

    /// Rollback the transaction
    async fn rollback(self: Box<Self>) -> Result<()>;
}

use std::any::Any;

#[async_trait]
pub trait Storage: Send + Sync + Any {
    /// Begin a new transaction
    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>>;
    
    /// Create a checkpoint (snapshot) of the current state
    async fn create_snapshot(&self) -> Result<()>;

    /// Helper for downcasting
    fn as_any(&self) -> &dyn Any;
}
