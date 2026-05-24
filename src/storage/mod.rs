use crate::common::Result;
use async_trait::async_trait;

pub mod backend;
pub mod columnar;
pub mod columnar_analytics;
pub mod fbtree;
pub mod fusion;
pub mod inverted_index;
pub mod memory;
pub mod sstable;
pub mod trigram;
pub mod vector_index;
pub mod wal;

pub use fusion::FusionStorage;
pub use fusion::FusionTransaction;

#[async_trait]
pub trait Transaction: Send + Sync {
    /// Get a value by key (from write buffer or storage)
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Put a key-value pair (into write buffer)
    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key (tombstone in write buffer)
    async fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Scan keys with a prefix (merge storage and write buffer)
    async fn scan_prefix(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Scan keys in a range [start, end) with optional limit
    async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Count keys with a prefix (optimized for COUNT(*))
    async fn count_prefix(&self, prefix: &[u8]) -> Result<usize>;

    /// Get first key-value pair in a range (optimized for MIN)
    async fn first(&self, start: &[u8], end: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>>;

    /// Get last key-value pair in a range (optimized for MAX)
    async fn last(&self, start: &[u8], end: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>>;

    /// Commit the transaction
    async fn commit(self: Box<Self>) -> Result<()>;

    /// Rollback the transaction
    async fn rollback(self: Box<Self>) -> Result<()>;

    /// Helper for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
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
