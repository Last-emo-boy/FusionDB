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

pub trait ScanVisitor: Send {
    fn visit(&mut self, key: &[u8], value: &[u8]) -> bool;
}

impl<F> ScanVisitor for F
where
    F: for<'a, 'b> FnMut(&'a [u8], &'b [u8]) -> bool + Send,
{
    fn visit(&mut self, key: &[u8], value: &[u8]) -> bool {
        self(key, value)
    }
}

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

    /// Like [`scan_prefix`], but the implementation may split a large unbounded prefix scan into
    /// disjoint sub-ranges merged in parallel. Results are identical to `scan_prefix` (same rows,
    /// same key order); engines without a parallel path inherit the serial default.
    async fn scan_prefix_parallel(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix(prefix, limit).await
    }

    /// Visit keys with a prefix without materializing the full result set.
    ///
    /// The visitor returns `false` to stop early. The return value is the number
    /// of visible key-value pairs visited.
    async fn scan_prefix_for_each(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> Result<usize>;

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
