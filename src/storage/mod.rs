use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use async_trait::async_trait;
use base64::Engine as _;
use std::sync::Arc;

pub mod backend;
pub mod columnar;
pub mod columnar_analytics;
pub(crate) mod data_migration;
pub mod fbtree;
pub mod fusion;
pub mod inverted_index;
pub(crate) mod keyspace;
pub(crate) mod manifest_edit;
pub(crate) mod manifest_log;
pub(crate) mod manifest_record;
pub mod memory;
pub mod sstable;
pub mod trigram;
pub mod vector_index;
pub mod wal;

/// Stable, collision-free identity for the derived HNSW index of one table column.
pub(crate) fn hnsw_index_name_for_column(table_name: &str, column_name: &str) -> Result<String> {
    let identity = keyspace::encode_identifier_key(
        keyspace::KeyNamespace::VectorIndex,
        &[table_name.as_bytes(), column_name.as_bytes()],
    )
    .map_err(|error| {
        FusionError::Storage(format!(
            "failed to encode HNSW index identity for '{table_name}.{column_name}': {error}"
        ))
    })?;
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(identity);
    let mut name = String::with_capacity("hnsw_v2_".len() + encoded.len());
    name.push_str("hnsw_v2_");
    name.push_str(&encoded);
    Ok(name)
}

pub use fusion::FusionStorage;
pub use fusion::FusionTransaction;

pub trait ScanVisitor: Send {
    fn visit(&mut self, key: &[u8], value: &[u8]) -> bool;
}

pub const SQL_BLOCK_ZONE_MAP_VALUE_ENCODING_VERSION: u8 = 1;
pub const SQL_BLOCK_ZONE_MAP_TYPE_INTEGER: u8 = 1;
pub const SQL_BLOCK_ZONE_MAP_TYPE_BOOLEAN: u8 = 2;
pub const SQL_BLOCK_ZONE_MAP_TYPE_DATE: u8 = 3;
pub const SQL_BLOCK_ZONE_MAP_TYPE_TIMESTAMP: u8 = 4;
pub const SQL_BLOCK_ZONE_MAP_TYPE_INTERVAL: u8 = 5;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlBlockZoneMapPruningPlan {
    pub table_name: String,
    pub schema_fingerprint: u64,
    pub terms: Vec<SqlBlockZoneMapPredicateTerm>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlBlockZoneMapPredicateTerm {
    pub column_index: u32,
    pub column_name: String,
    pub type_tag: u8,
    pub value_encoding_version: u8,
    pub kind: SqlBlockZoneMapPredicateKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlBlockZoneMapPredicateKind {
    Compare {
        op: SqlBlockZoneMapComparisonOp,
        scalar: i64,
    },
    InList {
        scalars: Vec<i64>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlBlockZoneMapComparisonOp {
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlBlockZoneMapPruningDecision {
    SkipBlock,
    ReadBlock,
    FailOpen(SqlBlockZoneMapFailOpenReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlBlockZoneMapFailOpenReason {
    IncompleteMetadata,
    MissingColumn,
    SchemaMismatch,
    ColumnMismatch,
    TypeMismatch,
    UnsupportedValueEncoding,
    InvalidBounds,
    NullOrTombstone,
    CountMismatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SqlBlockZoneMapTermDecision {
    NoMatch,
    MayMatch,
    FailOpen(SqlBlockZoneMapFailOpenReason),
}

impl SqlBlockZoneMapPruningPlan {
    pub fn evaluate_block_zone_maps(
        &self,
        table_prefix: &[u8],
        sql_zone_maps_complete: bool,
        zone_maps: &[sstable::SsTableSqlZoneMap],
    ) -> SqlBlockZoneMapPruningDecision {
        if !sql_zone_maps_complete {
            return SqlBlockZoneMapPruningDecision::FailOpen(
                SqlBlockZoneMapFailOpenReason::IncompleteMetadata,
            );
        }

        let mut fail_open = None;
        for term in &self.terms {
            match self.evaluate_term(table_prefix, zone_maps, term) {
                SqlBlockZoneMapTermDecision::NoMatch => {
                    return SqlBlockZoneMapPruningDecision::SkipBlock;
                }
                SqlBlockZoneMapTermDecision::MayMatch => {}
                SqlBlockZoneMapTermDecision::FailOpen(reason) => {
                    fail_open.get_or_insert(reason);
                }
            }
        }

        match fail_open {
            Some(reason) => SqlBlockZoneMapPruningDecision::FailOpen(reason),
            None => SqlBlockZoneMapPruningDecision::ReadBlock,
        }
    }

    fn evaluate_term(
        &self,
        table_prefix: &[u8],
        zone_maps: &[sstable::SsTableSqlZoneMap],
        term: &SqlBlockZoneMapPredicateTerm,
    ) -> SqlBlockZoneMapTermDecision {
        let Some(zone_map) = zone_maps.iter().find(|zone_map| {
            zone_map.table_prefix.as_slice() == table_prefix
                && zone_map.column_index == term.column_index
                && zone_map.column_name == term.column_name
        }) else {
            return SqlBlockZoneMapTermDecision::FailOpen(
                SqlBlockZoneMapFailOpenReason::MissingColumn,
            );
        };

        if zone_map.schema_fingerprint != self.schema_fingerprint {
            return SqlBlockZoneMapTermDecision::FailOpen(
                SqlBlockZoneMapFailOpenReason::SchemaMismatch,
            );
        }
        if zone_map.column_index != term.column_index || zone_map.column_name != term.column_name {
            return SqlBlockZoneMapTermDecision::FailOpen(
                SqlBlockZoneMapFailOpenReason::ColumnMismatch,
            );
        }
        if zone_map.type_tag != term.type_tag {
            return SqlBlockZoneMapTermDecision::FailOpen(
                SqlBlockZoneMapFailOpenReason::TypeMismatch,
            );
        }
        if zone_map.value_encoding_version != term.value_encoding_version {
            return SqlBlockZoneMapTermDecision::FailOpen(
                SqlBlockZoneMapFailOpenReason::UnsupportedValueEncoding,
            );
        }
        if !zone_map.bounds_valid || zone_map.min_scalar > zone_map.max_scalar {
            return SqlBlockZoneMapTermDecision::FailOpen(
                SqlBlockZoneMapFailOpenReason::InvalidBounds,
            );
        }
        if zone_map.null_count != 0 || zone_map.tombstone_count != 0 {
            return SqlBlockZoneMapTermDecision::FailOpen(
                SqlBlockZoneMapFailOpenReason::NullOrTombstone,
            );
        }
        if zone_map.row_count == 0
            || zone_map.row_count != zone_map.put_count
            || zone_map.row_count != zone_map.non_null_count
        {
            return SqlBlockZoneMapTermDecision::FailOpen(
                SqlBlockZoneMapFailOpenReason::CountMismatch,
            );
        }

        match &term.kind {
            SqlBlockZoneMapPredicateKind::Compare { op, scalar } => {
                if compare_term_cannot_match(*op, *scalar, zone_map.min_scalar, zone_map.max_scalar)
                {
                    SqlBlockZoneMapTermDecision::NoMatch
                } else {
                    SqlBlockZoneMapTermDecision::MayMatch
                }
            }
            SqlBlockZoneMapPredicateKind::InList { scalars } => {
                if scalars
                    .iter()
                    .all(|scalar| *scalar < zone_map.min_scalar || *scalar > zone_map.max_scalar)
                {
                    SqlBlockZoneMapTermDecision::NoMatch
                } else {
                    SqlBlockZoneMapTermDecision::MayMatch
                }
            }
        }
    }
}

fn compare_term_cannot_match(
    op: SqlBlockZoneMapComparisonOp,
    scalar: i64,
    block_min: i64,
    block_max: i64,
) -> bool {
    match op {
        SqlBlockZoneMapComparisonOp::Eq => scalar < block_min || scalar > block_max,
        SqlBlockZoneMapComparisonOp::Lt => block_min >= scalar,
        SqlBlockZoneMapComparisonOp::LtEq => block_min > scalar,
        SqlBlockZoneMapComparisonOp::Gt => block_max <= scalar,
        SqlBlockZoneMapComparisonOp::GtEq => block_max < scalar,
    }
}

pub fn sql_block_zone_map_schema_fingerprint(schema: &TableSchema) -> u64 {
    fn update(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(0x100000001b3);
        }
        *hash ^= 0xff;
        *hash = hash.wrapping_mul(0x100000001b3);
    }

    let mut hash = 0xcbf29ce484222325u64;
    update(&mut hash, schema.name.as_bytes());
    update(
        &mut hash,
        &u32::try_from(schema.columns.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    for (index, column) in schema.columns.iter().enumerate() {
        update(
            &mut hash,
            &u32::try_from(index).unwrap_or(u32::MAX).to_le_bytes(),
        );
        update(&mut hash, column.name.as_bytes());
        update(
            &mut hash,
            column.data_type.trim().to_ascii_uppercase().as_bytes(),
        );
        update(&mut hash, &[u8::from(column.is_primary)]);
        update(&mut hash, &[u8::from(column.is_nullable)]);
        update(&mut hash, &[u8::from(column.is_indexed)]);
        let index_type_tag = match column.index_type {
            IndexType::None => 0,
            IndexType::BTree => 1,
            IndexType::FTS => 2,
            IndexType::HNSW => 3,
        };
        update(&mut hash, &[index_type_tag]);
        update(&mut hash, &[u8::from(column.is_unique)]);
    }
    hash
}

pub fn sql_block_zone_map_type_tag(data_type: &str) -> Option<u8> {
    let normalized = data_type.trim().to_ascii_uppercase();
    let base = normalized
        .split(|ch: char| ch == '(' || ch.is_whitespace())
        .next()
        .unwrap_or("");
    match base {
        "TINYINT" | "SMALLINT" | "INT2" | "INT" | "INT4" | "INTEGER" | "MEDIUMINT" | "BIGINT"
        | "INT8" | "INT16" | "INT32" | "INT64" | "UINT8" | "UINT16" | "UINT32" | "UINT64" => {
            Some(SQL_BLOCK_ZONE_MAP_TYPE_INTEGER)
        }
        "BOOL" | "BOOLEAN" => Some(SQL_BLOCK_ZONE_MAP_TYPE_BOOLEAN),
        "DATE" | "DATE32" => Some(SQL_BLOCK_ZONE_MAP_TYPE_DATE),
        "TIMESTAMP" | "TIMESTAMPTZ" | "DATETIME" => Some(SQL_BLOCK_ZONE_MAP_TYPE_TIMESTAMP),
        "INTERVAL" => Some(SQL_BLOCK_ZONE_MAP_TYPE_INTERVAL),
        _ => None,
    }
}

pub fn sql_block_zone_map_scalar(value: &Value, type_tag: u8) -> Option<Option<i64>> {
    match value {
        Value::Null => Some(None),
        Value::Integer(value) if type_tag == SQL_BLOCK_ZONE_MAP_TYPE_INTEGER => Some(Some(*value)),
        Value::Boolean(value) if type_tag == SQL_BLOCK_ZONE_MAP_TYPE_BOOLEAN => {
            Some(Some(if *value { 1 } else { 0 }))
        }
        Value::Date(value) if type_tag == SQL_BLOCK_ZONE_MAP_TYPE_DATE => {
            Some(Some(i64::from(*value)))
        }
        Value::Timestamp(value) if type_tag == SQL_BLOCK_ZONE_MAP_TYPE_TIMESTAMP => {
            Some(Some(*value))
        }
        Value::Interval(value) if type_tag == SQL_BLOCK_ZONE_MAP_TYPE_INTERVAL => {
            Some(Some(*value))
        }
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageScanOptions {
    pub fill_cache: bool,
    pub sql_block_zone_map_pruning: bool,
    pub sql_block_zone_map_pruning_plan: Option<Arc<SqlBlockZoneMapPruningPlan>>,
}

impl StorageScanOptions {
    pub fn fill_cache() -> Self {
        Self {
            fill_cache: true,
            sql_block_zone_map_pruning: false,
            sql_block_zone_map_pruning_plan: None,
        }
    }

    pub fn no_fill_cache() -> Self {
        Self {
            fill_cache: false,
            sql_block_zone_map_pruning: false,
            sql_block_zone_map_pruning_plan: None,
        }
    }

    pub fn with_sql_block_zone_map_pruning(mut self, enabled: bool) -> Self {
        self.sql_block_zone_map_pruning = enabled;
        if !enabled {
            self.sql_block_zone_map_pruning_plan = None;
        }
        self
    }

    pub fn with_sql_block_zone_map_pruning_plan(
        mut self,
        plan: Arc<SqlBlockZoneMapPruningPlan>,
    ) -> Self {
        self.sql_block_zone_map_pruning = true;
        self.sql_block_zone_map_pruning_plan = Some(plan);
        self
    }

    pub fn sql_block_zone_map_pruning_enabled(&self) -> bool {
        self.sql_block_zone_map_pruning && self.sql_block_zone_map_pruning_plan.is_some()
    }
}

impl Default for StorageScanOptions {
    fn default() -> Self {
        Self::fill_cache()
    }
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

    /// Pin the Data V2 migration phase this transaction's data-family writes
    /// act on. FusionTransaction re-validates the pin at commit inside the
    /// commit critical section; RecordingTransaction additionally records a
    /// replicated precondition. Engines without migration state ignore it.
    async fn fence_data_migration_phase(&mut self, _phase: u8, _phase_seq: u64) -> Result<()> {
        Ok(())
    }

    /// The pin already established by [`fence_data_migration_phase`], if any.
    ///
    /// Synchronous and allocation-free on purpose: row writers call this once
    /// per row, and after the first row of a statement it lets them skip the
    /// shared fence lock and the boxed async fence call entirely.
    fn data_migration_phase_pin(&self) -> Option<(u8, u64)> {
        None
    }

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

    async fn scan_prefix_with_options(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        _options: StorageScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix(prefix, limit).await
    }

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

    async fn scan_prefix_parallel_with_options(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        _options: StorageScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix_parallel(prefix, limit).await
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

    async fn scan_prefix_for_each_with_options(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        options: StorageScanOptions,
    ) -> Result<usize> {
        let rows = self
            .scan_prefix_with_options(prefix, limit, options)
            .await?;
        let mut visited = 0;
        for (key, value) in rows {
            visited += 1;
            if !visitor.visit(&key, &value) {
                break;
            }
        }
        Ok(visited)
    }

    /// Visit keys with a prefix using a parallel streaming implementation when supported.
    ///
    /// Returns `Some(visited)` when the implementation handled the scan. Returns `None`
    /// when the caller should fall back to an existing serial or materializing path.
    async fn scan_prefix_parallel_for_each(
        &self,
        _prefix: &[u8],
        _limit: Option<usize>,
        _visitor: &mut dyn ScanVisitor,
    ) -> Result<Option<usize>> {
        Ok(None)
    }

    async fn scan_prefix_parallel_for_each_with_options(
        &self,
        _prefix: &[u8],
        _limit: Option<usize>,
        _visitor: &mut dyn ScanVisitor,
        _options: StorageScanOptions,
    ) -> Result<Option<usize>> {
        Ok(None)
    }

    /// Scan keys in a range [start, end) with optional limit
    async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    async fn scan_range_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        _options: StorageScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_range(start, end, limit).await
    }

    /// Visit visible keys in a range [start, end) without requiring callers to materialize rows.
    ///
    /// The visitor returns `false` to stop early. The return value is the number
    /// of visible key-value pairs visited.
    async fn scan_range_for_each(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> Result<usize> {
        let rows = self.scan_range(start, end, limit).await?;
        let mut visited = 0;
        for (key, value) in rows {
            visited += 1;
            if !visitor.visit(&key, &value) {
                break;
            }
        }
        Ok(visited)
    }

    async fn scan_range_for_each_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        options: StorageScanOptions,
    ) -> Result<usize> {
        let rows = self
            .scan_range_with_options(start, end, limit, options)
            .await?;
        let mut visited = 0;
        for (key, value) in rows {
            visited += 1;
            if !visitor.visit(&key, &value) {
                break;
            }
        }
        Ok(visited)
    }

    /// Whether `scan_range_reverse` is implemented as a bounded reverse merge.
    ///
    /// The default `scan_range_reverse` below is a correctness fallback that
    /// materializes the forward range. Planner fast paths must require this
    /// capability before using reverse scans as a performance optimization.
    fn supports_bounded_scan_range_reverse(&self) -> bool {
        false
    }

    /// Scan visible keys in a range [start, end) in descending key order.
    ///
    /// The default implementation is correct but not bounded: it materializes the
    /// forward range before reversing and truncating. Storage engines should
    /// override this when they can merge visible keys in reverse order directly.
    async fn scan_range_reverse(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == Some(0) || start >= end {
            return Ok(Vec::new());
        }

        let mut rows = self.scan_range(start, end, None).await?;
        rows.reverse();
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    async fn scan_range_reverse_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        options: StorageScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == Some(0) || start >= end {
            return Ok(Vec::new());
        }

        let mut rows = self
            .scan_range_with_options(start, end, None, options)
            .await?;
        rows.reverse();
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    /// Visit visible keys in a range [start, end) in descending key order.
    ///
    /// Engines with true bounded reverse scans should override this alongside
    /// `scan_range_reverse`; the default inherits the materializing correctness fallback.
    async fn scan_range_reverse_for_each(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> Result<usize> {
        let rows = self.scan_range_reverse(start, end, limit).await?;
        let mut visited = 0;
        for (key, value) in rows {
            visited += 1;
            if !visitor.visit(&key, &value) {
                break;
            }
        }
        Ok(visited)
    }

    async fn scan_range_reverse_for_each_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        options: StorageScanOptions,
    ) -> Result<usize> {
        let rows = self
            .scan_range_reverse_with_options(start, end, limit, options)
            .await?;
        let mut visited = 0;
        for (key, value) in rows {
            visited += 1;
            if !visitor.visit(&key, &value) {
                break;
            }
        }
        Ok(visited)
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sstable::SsTableSqlZoneMap;

    #[test]
    fn hnsw_index_name_is_stable_ascii() {
        let name = hnsw_index_name_for_column("docs", "embedding").unwrap();

        assert_eq!(name, "hnsw_v2_AEZEQksCBwAAAARkb2NzAAAACWVtYmVkZGluZw");
        assert!(name.is_ascii());
    }

    #[test]
    fn hnsw_index_name_separates_underscore_ambiguous_identifiers() {
        let left = hnsw_index_name_for_column("a_b", "c").unwrap();
        let right = hnsw_index_name_for_column("a", "b_c").unwrap();

        assert_ne!(left, right);
    }

    fn zone_map_plan(kind: SqlBlockZoneMapPredicateKind) -> SqlBlockZoneMapPruningPlan {
        SqlBlockZoneMapPruningPlan {
            table_name: "metrics".to_string(),
            schema_fingerprint: 42,
            terms: vec![SqlBlockZoneMapPredicateTerm {
                column_index: 1,
                column_name: "bucket".to_string(),
                type_tag: SQL_BLOCK_ZONE_MAP_TYPE_INTEGER,
                value_encoding_version: SQL_BLOCK_ZONE_MAP_VALUE_ENCODING_VERSION,
                kind,
            }],
        }
    }

    fn zone_map() -> SsTableSqlZoneMap {
        SsTableSqlZoneMap {
            table_prefix: b"data:metrics:".to_vec(),
            schema_fingerprint: 42,
            column_index: 1,
            column_name: "bucket".to_string(),
            type_tag: SQL_BLOCK_ZONE_MAP_TYPE_INTEGER,
            value_encoding_version: SQL_BLOCK_ZONE_MAP_VALUE_ENCODING_VERSION,
            min_scalar: 10,
            max_scalar: 20,
            row_count: 4,
            null_count: 0,
            non_null_count: 4,
            put_count: 4,
            tombstone_count: 0,
            bounds_valid: true,
        }
    }

    fn evaluate(
        plan: &SqlBlockZoneMapPruningPlan,
        zone_maps: &[SsTableSqlZoneMap],
    ) -> SqlBlockZoneMapPruningDecision {
        plan.evaluate_block_zone_maps(b"data:metrics:", true, zone_maps)
    }

    #[test]
    fn sql_block_zone_map_decision_skips_when_any_safe_term_cannot_match() {
        let maps = vec![zone_map()];

        for (op, scalar) in [
            (SqlBlockZoneMapComparisonOp::Eq, 9),
            (SqlBlockZoneMapComparisonOp::Lt, 10),
            (SqlBlockZoneMapComparisonOp::LtEq, 9),
            (SqlBlockZoneMapComparisonOp::Gt, 20),
            (SqlBlockZoneMapComparisonOp::GtEq, 21),
        ] {
            let plan = zone_map_plan(SqlBlockZoneMapPredicateKind::Compare { op, scalar });
            assert_eq!(
                evaluate(&plan, &maps),
                SqlBlockZoneMapPruningDecision::SkipBlock
            );
        }

        let plan = zone_map_plan(SqlBlockZoneMapPredicateKind::InList {
            scalars: vec![1, 2, 30],
        });
        assert_eq!(
            evaluate(&plan, &maps),
            SqlBlockZoneMapPruningDecision::SkipBlock
        );
    }

    #[test]
    fn sql_block_zone_map_decision_reads_when_terms_may_match() {
        let maps = vec![zone_map()];

        for (op, scalar) in [
            (SqlBlockZoneMapComparisonOp::Eq, 10),
            (SqlBlockZoneMapComparisonOp::Eq, 20),
            (SqlBlockZoneMapComparisonOp::Lt, 11),
            (SqlBlockZoneMapComparisonOp::LtEq, 10),
            (SqlBlockZoneMapComparisonOp::Gt, 19),
            (SqlBlockZoneMapComparisonOp::GtEq, 20),
        ] {
            let plan = zone_map_plan(SqlBlockZoneMapPredicateKind::Compare { op, scalar });
            assert_eq!(
                evaluate(&plan, &maps),
                SqlBlockZoneMapPruningDecision::ReadBlock
            );
        }

        let plan = zone_map_plan(SqlBlockZoneMapPredicateKind::InList {
            scalars: vec![1, 12, 30],
        });
        assert_eq!(
            evaluate(&plan, &maps),
            SqlBlockZoneMapPruningDecision::ReadBlock
        );
    }

    #[test]
    fn sql_block_zone_map_decision_fails_open_for_untrusted_metadata() {
        let plan = zone_map_plan(SqlBlockZoneMapPredicateKind::Compare {
            op: SqlBlockZoneMapComparisonOp::Eq,
            scalar: 12,
        });

        assert_eq!(
            plan.evaluate_block_zone_maps(b"data:metrics:", false, &[zone_map()]),
            SqlBlockZoneMapPruningDecision::FailOpen(
                SqlBlockZoneMapFailOpenReason::IncompleteMetadata
            )
        );
        assert_eq!(
            evaluate(&plan, &[]),
            SqlBlockZoneMapPruningDecision::FailOpen(SqlBlockZoneMapFailOpenReason::MissingColumn)
        );

        let cases = [
            (
                {
                    let mut map = zone_map();
                    map.schema_fingerprint = 99;
                    map
                },
                SqlBlockZoneMapFailOpenReason::SchemaMismatch,
            ),
            (
                {
                    let mut map = zone_map();
                    map.type_tag = SQL_BLOCK_ZONE_MAP_TYPE_BOOLEAN;
                    map
                },
                SqlBlockZoneMapFailOpenReason::TypeMismatch,
            ),
            (
                {
                    let mut map = zone_map();
                    map.value_encoding_version =
                        SQL_BLOCK_ZONE_MAP_VALUE_ENCODING_VERSION.saturating_add(1);
                    map
                },
                SqlBlockZoneMapFailOpenReason::UnsupportedValueEncoding,
            ),
            (
                {
                    let mut map = zone_map();
                    map.bounds_valid = false;
                    map
                },
                SqlBlockZoneMapFailOpenReason::InvalidBounds,
            ),
            (
                {
                    let mut map = zone_map();
                    map.min_scalar = 20;
                    map.max_scalar = 10;
                    map
                },
                SqlBlockZoneMapFailOpenReason::InvalidBounds,
            ),
            (
                {
                    let mut map = zone_map();
                    map.null_count = 1;
                    map
                },
                SqlBlockZoneMapFailOpenReason::NullOrTombstone,
            ),
            (
                {
                    let mut map = zone_map();
                    map.tombstone_count = 1;
                    map
                },
                SqlBlockZoneMapFailOpenReason::NullOrTombstone,
            ),
            (
                {
                    let mut map = zone_map();
                    map.non_null_count = 3;
                    map
                },
                SqlBlockZoneMapFailOpenReason::CountMismatch,
            ),
        ];

        for (map, reason) in cases {
            assert_eq!(
                evaluate(&plan, &[map]),
                SqlBlockZoneMapPruningDecision::FailOpen(reason)
            );
        }
    }

    #[test]
    fn sql_block_zone_map_decision_valid_no_match_term_can_skip_before_later_fail_open() {
        let mut plan = zone_map_plan(SqlBlockZoneMapPredicateKind::Compare {
            op: SqlBlockZoneMapComparisonOp::Eq,
            scalar: 7,
        });
        plan.terms.push(SqlBlockZoneMapPredicateTerm {
            column_index: 2,
            column_name: "missing".to_string(),
            type_tag: SQL_BLOCK_ZONE_MAP_TYPE_INTEGER,
            value_encoding_version: SQL_BLOCK_ZONE_MAP_VALUE_ENCODING_VERSION,
            kind: SqlBlockZoneMapPredicateKind::Compare {
                op: SqlBlockZoneMapComparisonOp::Eq,
                scalar: 1,
            },
        });
        assert_eq!(
            evaluate(&plan, &[zone_map()]),
            SqlBlockZoneMapPruningDecision::SkipBlock
        );

        plan.terms[0].kind = SqlBlockZoneMapPredicateKind::Compare {
            op: SqlBlockZoneMapComparisonOp::Eq,
            scalar: 12,
        };
        assert_eq!(
            evaluate(&plan, &[zone_map()]),
            SqlBlockZoneMapPruningDecision::FailOpen(SqlBlockZoneMapFailOpenReason::MissingColumn)
        );
    }
}
