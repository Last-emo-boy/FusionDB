mod aggregation;
mod analyze;
mod composite_index;
mod copy;
mod ddl;
mod dml;
mod expr;
mod foreign_key;
mod query;
mod scan;
mod stats;
mod types;

pub(crate) use aggregation::AggregateAccumulator;
pub(crate) use foreign_key::ForeignKeyMeta;

use crate::ai::embedding::EmbeddingRegistry;
use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use crate::config::StorageConfig;
use crate::distributed::sharding::{ShardRoute, ShardRouter};
use crate::monitor;
use crate::parser::parse_sql;
use crate::storage::data_migration::{
    backfill_state_key, migration_phase_key, CachedFenceState, DataBackfillState,
    DataMigrationFence, DataMigrationPhase, DataMigrationPhaseRecord, MAX_ADVANCE_TARGET_PHASE,
    MAX_SUPPORTED_PHASE,
};
use crate::storage::{
    vector_index::VectorIndex, FusionStorage, ScanVisitor, Storage, StorageScanOptions, Transaction,
};
use moka::sync::Cache;
use parking_lot::RwLock;
use sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, LimitClause, ObjectName, ObjectNamePart, ObjectType, OrderByKind, Query,
    SelectItem, SetExpr, Statement, TableFactor,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering as AtomicOrdering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub enum QueryResult {
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Success {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlShardRoutingDecision {
    pub operation: String,
    pub route: ShardRoute,
    pub local_node_id: u64,
}

impl SqlShardRoutingDecision {
    pub(crate) fn is_local_owner(&self) -> bool {
        self.route.owner_node_id == self.local_node_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlShardOwner {
    pub node_id: u64,
    pub addr: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlShardExtremum {
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlShardAvgFanoutPlan {
    pub rewritten_sql: String,
    pub output_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlShardCountDistinctFanoutPlan {
    pub rewritten_sql: String,
    pub output_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlShardDistinctAggregateFanoutPlan {
    pub rewritten_sql: String,
    pub output_column: String,
}

/// Plan for `SELECT g1[, g2 ...], COUNT(*) FROM t [WHERE ...] GROUP BY g1[, g2 ...]` shard-owner
/// fan-out. Each owner runs the original query (no rewrite); results are merged by re-grouping on the
/// composite key (the projection values at `group_indices`, in order) and summing the counts at
/// `count_index`. The output preserves the projection column order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SqlShardGroupCountFanoutPlan {
    pub group_indices: Vec<usize>,
    pub count_index: usize,
    pub output_columns: Vec<String>,
    /// `Some` when the query carries `ORDER BY` / `LIMIT` / `OFFSET` (post-merge top-N — see
    /// [`GroupedPostMerge`]); owners run `per_owner_sql` (clauses stripped) so each returns all groups.
    pub post_merge: Option<GroupedPostMerge>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlShardGroupAggregateKind {
    Sum,
    Min,
    Max,
}

/// Plan for `SELECT g1[, ...], AGG(x) FROM t [WHERE ...] GROUP BY g1[, ...]` shard-owner fan-out for
/// AGG in {SUM, MIN, MAX}. Each owner runs the original query; results are merged by re-grouping on
/// the composite key (`group_indices`) and reducing the aggregate value at `agg_index` per `kind`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SqlShardGroupAggregateFanoutPlan {
    pub group_indices: Vec<usize>,
    pub agg_index: usize,
    pub kind: SqlShardGroupAggregateKind,
    pub output_columns: Vec<String>,
    /// `Some` when the query carries `ORDER BY` / `LIMIT` / `OFFSET`: owners run `per_owner_sql`
    /// (those clauses stripped) so each returns ALL its groups, then the merged rows are globally
    /// sorted and sliced post-merge. `None` for a plain grouped query (owners run the original SQL).
    pub post_merge: Option<GroupedPostMerge>,
}

/// One resolved `ORDER BY` key for a distributed grouped post-merge: the output column index to sort
/// on (rows are emitted in projection/output-column layout), the direction, and NULL placement.
/// Resolution happens at plan time; any `ORDER BY` expression that cannot be mapped to an output
/// column makes the whole extractor return `None` (→ the 449 safety net errors loudly), so a
/// distributed grouped query is never silently returned unsorted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GroupedOrderKey {
    pub col_index: usize,
    pub asc: bool,
    pub nulls_first: bool,
}

/// Comparison operator for a distributed grouped post-merge `HAVING` conjunct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GroupedHavingOp {
    Gt,
    GtEq,
    Lt,
    LtEq,
    Eq,
    NotEq,
}

/// One `HAVING` conjunct resolved against the output row layout: `output[col_index] <op> literal`.
/// A group spans owners, so `HAVING` (like `LIMIT`) must be evaluated post-merge on the GLOBAL groups,
/// never per-owner — a group below the threshold locally may be above it globally.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupedHavingConjunct {
    pub col_index: usize,
    pub op: GroupedHavingOp,
    pub literal: serde_json::Value,
}

/// A resolved `HAVING` predicate: an AND of simple `<output-col> <cmp> <literal>` comparisons. Only
/// this conjunctive shape is supported; anything else (OR, function calls on the row, non-literal RHS)
/// makes resolution return `None` → the 449 net errors loudly rather than returning a wrong filter.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupedHaving {
    pub conjuncts: Vec<GroupedHavingConjunct>,
}

/// Post-merge spec for a distributed grouped fan-out carrying `HAVING` / `ORDER BY` / `LIMIT` /
/// `OFFSET`. A group spans owners, so these clauses can't run per-owner (a partial top-N or partial
/// `HAVING` would be wrong); owners run `per_owner_sql` (clauses stripped, returning ALL groups) and
/// the fully merged rows are filtered by `having`, then globally stable-sorted by `order_keys`, then
/// sliced by `offset`/`limit` — in that SQL order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupedPostMerge {
    pub per_owner_sql: String,
    pub having: Option<GroupedHaving>,
    pub order_keys: Vec<GroupedOrderKey>,
    pub limit: Option<usize>,
    pub offset: usize,
}

/// Apply a distributed grouped post-merge spec to fully-merged rows in place, in SQL order: filter by
/// `having`, globally stable-sort by `order_keys`, then drop `offset` rows and truncate to `limit`.
/// Shared by the HTTP and pgwire grouped fan-out handlers so both transports apply identical semantics.
pub(crate) fn apply_grouped_order_limit(
    rows: &mut Vec<Vec<serde_json::Value>>,
    spec: &GroupedPostMerge,
) {
    if let Some(having) = &spec.having {
        rows.retain(|row| grouped_having_predicate_holds(row, having));
    }
    if !spec.order_keys.is_empty() {
        rows.sort_by(|a, b| {
            for key in &spec.order_keys {
                let null = serde_json::Value::Null;
                let ord = compare_grouped_order_values(
                    a.get(key.col_index).unwrap_or(&null),
                    b.get(key.col_index).unwrap_or(&null),
                    key.asc,
                    key.nulls_first,
                );
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    if spec.offset > 0 {
        if spec.offset >= rows.len() {
            rows.clear();
        } else {
            rows.drain(0..spec.offset);
        }
    }
    if let Some(limit) = spec.limit {
        rows.truncate(limit);
    }
}

/// Compare two output values for `ORDER BY`, honoring direction and NULL placement. NULL placement is
/// absolute (`nulls_first` decides the final slot regardless of `asc`); non-null values are compared
/// by SQL semantics and then reversed for `DESC`.
fn compare_grouped_order_values(
    a: &serde_json::Value,
    b: &serde_json::Value,
    asc: bool,
    nulls_first: bool,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, false) => {
            let base = compare_grouped_non_null(a, b);
            if asc {
                base
            } else {
                base.reverse()
            }
        }
    }
}

fn compare_grouped_non_null(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    use serde_json::Value;
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            // Prefer exact integer comparison; fall back to f64 for floats / mixed int-float columns.
            if let (Some(xi), Some(yi)) = (x.as_i64(), y.as_i64()) {
                xi.cmp(&yi)
            } else if let (Some(xu), Some(yu)) = (x.as_u64(), y.as_u64()) {
                xu.cmp(&yu)
            } else {
                let xf = x.as_f64().unwrap_or(f64::NAN);
                let yf = y.as_f64().unwrap_or(f64::NAN);
                xf.partial_cmp(&yf).unwrap_or(Ordering::Equal)
            }
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        // Mixed/other types should not occur within one output column; fall back to a deterministic,
        // panic-free ordering by type rank then string form so the sort stays total.
        _ => grouped_value_type_rank(a)
            .cmp(&grouped_value_type_rank(b))
            .then_with(|| a.to_string().cmp(&b.to_string())),
    }
}

fn grouped_value_type_rank(value: &serde_json::Value) -> u8 {
    use serde_json::Value;
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

/// Evaluate a resolved `HAVING` predicate against one merged output row: every conjunct must hold
/// (AND). A `NULL` on either side of a comparison is SQL "unknown" → the row is dropped (HAVING keeps
/// only rows where the predicate is TRUE).
fn grouped_having_predicate_holds(row: &[serde_json::Value], having: &GroupedHaving) -> bool {
    having
        .conjuncts
        .iter()
        .all(|conjunct| grouped_having_conjunct_holds(row, conjunct))
}

fn grouped_having_conjunct_holds(
    row: &[serde_json::Value],
    conjunct: &GroupedHavingConjunct,
) -> bool {
    let Some(value) = row.get(conjunct.col_index) else {
        return false;
    };
    if value.is_null() || conjunct.literal.is_null() {
        return false; // SQL: comparison with NULL is unknown, not TRUE
    }
    let ordering = compare_grouped_non_null(value, &conjunct.literal);
    match conjunct.op {
        GroupedHavingOp::Gt => ordering == std::cmp::Ordering::Greater,
        GroupedHavingOp::GtEq => ordering != std::cmp::Ordering::Less,
        GroupedHavingOp::Lt => ordering == std::cmp::Ordering::Less,
        GroupedHavingOp::LtEq => ordering != std::cmp::Ordering::Greater,
        GroupedHavingOp::Eq => ordering == std::cmp::Ordering::Equal,
        GroupedHavingOp::NotEq => ordering != std::cmp::Ordering::Equal,
    }
}

/// Plan for `SELECT g1[, ...], AVG(x) FROM t [WHERE ...] GROUP BY g1[, ...]` shard-owner fan-out.
/// AVG is not directly mergeable, so each owner runs `rewritten_sql`, which replaces the AVG
/// projection item in place with `SUM(x), COUNT(x)`; results are re-grouped on the composite key
/// (`group_indices`, indices into the *rewritten* result) and the partial sums (`sum_index`) and
/// counts (`count_index`) are added per group, then divided. Output rows are rebuilt in the original
/// projection layout: group values plus the AVG value at `avg_output_index`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SqlShardGroupAvgFanoutPlan {
    pub rewritten_sql: String,
    pub group_indices: Vec<usize>,
    pub sum_index: usize,
    pub count_index: usize,
    pub avg_output_index: usize,
    pub output_columns: Vec<String>,
    /// `Some` when the query carries `ORDER BY` / `LIMIT` / `OFFSET`. AVG owners always run the
    /// (already clause-free) `rewritten_sql`, so `per_owner_sql` mirrors it; the global sort + slice is
    /// applied post-merge on the rebuilt AVG rows. See [`GroupedPostMerge`].
    pub post_merge: Option<GroupedPostMerge>,
}

/// One aggregate in a multi-aggregate grouped fan-out: its output column index and how it merges
/// across owners. `COUNT(*)`/`COUNT(col)`/`SUM(col)` all merge by adding partials (kind `Sum`);
/// `MIN`/`MAX` merge by extremum. (`AVG` and `DISTINCT` aggregates are NOT supported here — they need a
/// rewrite / value-set merge — so a projection containing them makes the extractor return `None`.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GroupMultiAggregate {
    pub output_index: usize,
    pub kind: SqlShardGroupAggregateKind,
}

/// Plan for `SELECT g1[, ...], AGG1, AGG2[, ...] FROM t [WHERE ...] GROUP BY g1[, ...]` shard-owner
/// fan-out where each AGG is `COUNT(*)` / `COUNT(col)` / `SUM(col)` / `MIN(col)` / `MAX(col)` (two or
/// more aggregates, or shapes the single-aggregate planners don't cover). Each owner runs the original
/// query (all of these aggregates are directly mergeable — no rewrite); results are merged by
/// re-grouping on the composite key (`group_indices`) and reducing EACH aggregate independently at its
/// `output_index` per its `kind`. The output preserves the projection column order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SqlShardGroupMultiAggregateFanoutPlan {
    pub group_indices: Vec<usize>,
    pub aggregates: Vec<GroupMultiAggregate>,
    pub output_columns: Vec<String>,
    pub post_merge: Option<GroupedPostMerge>,
}

#[derive(Debug, Clone)]
pub struct PreparedStatementRecord {
    pub id: String,
    pub sql: String,
    pub statements: Arc<Vec<Statement>>,
    pub owner: Option<String>,
    pub created_at_epoch_ms: u128,
}

#[derive(Default)]
struct PreparedStatementStore {
    entries: HashMap<String, PreparedStatementRecord>,
    order: VecDeque<String>,
}

#[derive(Clone)]
struct CachedSelectResult {
    epoch: u64,
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

tokio::task_local! {
    static SQL_BLOCK_ZONE_MAP_PRUNING_ENABLED: bool;
    // 472 T1: scoped kill-switch for the columnar single-source aggregate fast
    // path. Defaults to enabled; tests scope it to `false` to force the
    // untouched merge path and assert byte-identical results.
    static COLUMNAR_SINGLE_SOURCE_AGGREGATE_ENABLED: bool;
}

struct StopAwareScanVisitor<'a> {
    inner: &'a mut dyn ScanVisitor,
    stopped: bool,
}

impl ScanVisitor for StopAwareScanVisitor<'_> {
    fn visit(&mut self, key: &[u8], value: &[u8]) -> bool {
        let keep_scanning = self.inner.visit(key, value);
        if !keep_scanning {
            self.stopped = true;
        }
        keep_scanning
    }
}

struct ExactTableDataScanVisitor<'a> {
    executor: &'a Executor,
    table_name: &'a str,
    schema: Option<&'a TableSchema>,
    prefixes: &'a [String],
    inner: &'a mut dyn ScanVisitor,
    accepted: &'a mut usize,
    limit: Option<usize>,
    stopped: bool,
}

impl ScanVisitor for ExactTableDataScanVisitor<'_> {
    fn visit(&mut self, key: &[u8], value: &[u8]) -> bool {
        if !self.executor.routed_data_entry_belongs_to_table(
            self.table_name,
            self.schema,
            self.prefixes,
            key,
            value,
        ) {
            return true;
        }

        if self.limit.is_some_and(|limit| *self.accepted >= limit) {
            self.stopped = true;
            return false;
        }

        *self.accepted += 1;
        let keep_scanning = self.inner.visit(key, value);
        if !keep_scanning || self.limit.is_some_and(|limit| *self.accepted >= limit) {
            self.stopped = true;
            return false;
        }
        true
    }
}

pub struct Executor {
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) shard_router: Option<ShardRouter>,
    statement_cache: Cache<String, Vec<Statement>>,
    query_result_cache: Cache<String, CachedSelectResult>,
    simple_pk_update_fast_path_cache: Cache<String, bool>,
    query_result_epoch: AtomicU64,
    prepared_statements: RwLock<PreparedStatementStore>,
    pub(crate) row_cache: Cache<String, CachedRow>,
    pub(crate) sql_bulk_scan_no_fill: bool,
    structured_data_shadow_v2: bool,
    data_migration_fence: Option<Arc<DataMigrationFence>>,
    pub(crate) vector_index: Arc<VectorIndex>,
    pub(crate) embedding_registry: Arc<EmbeddingRegistry>,
}

/// A decoded row paired with the exact encoded bytes it was decoded from.
///
/// The row cache is validated by byte identity: a cached row may only be
/// used when the caller holds encoded bytes identical to `encoded`. This
/// makes the cache immune to MVCC staleness by construction — a snapshot
/// read that resolves different bytes (an older or newer row version) can
/// never match, so no invalidation protocol is required for correctness.
#[derive(Clone)]
pub(crate) struct CachedRow {
    encoded: Arc<[u8]>,
    row: Vec<Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CacheableAggregateFunction {
    Count,
    Value,
}

fn execution_object_name_eq_ascii(name: &ObjectName, expected: &str) -> bool {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)] => ident.value.eq_ignore_ascii_case(expected),
        [ObjectNamePart::Function(function)] => function.name.value.eq_ignore_ascii_case(expected),
        _ => false,
    }
}

fn cacheable_aggregate_function_kind(name: &ObjectName) -> Option<CacheableAggregateFunction> {
    if execution_object_name_eq_ascii(name, "COUNT") {
        Some(CacheableAggregateFunction::Count)
    } else if execution_object_name_eq_ascii(name, "SUM")
        || execution_object_name_eq_ascii(name, "AVG")
        || execution_object_name_eq_ascii(name, "MIN")
        || execution_object_name_eq_ascii(name, "MAX")
        || execution_object_name_eq_ascii(name, "STRING_AGG")
        || execution_object_name_eq_ascii(name, "GROUP_CONCAT")
    {
        Some(CacheableAggregateFunction::Value)
    } else {
        None
    }
}

impl Executor {
    const MAX_PREPARED_STATEMENTS: usize = 1024;

    fn current_epoch_ms() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or_default()
    }

    fn is_legacy_superuser(username: &str) -> bool {
        username.is_empty() || username.eq_ignore_ascii_case("postgres")
    }

    fn normalize_prepared_owner(owner: Option<&str>) -> Option<String> {
        owner
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string())
    }

    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self::with_config(storage, &StorageConfig::default())
    }

    pub fn with_config(storage: Arc<dyn Storage>, config: &StorageConfig) -> Self {
        Self::with_config_and_shard_router(storage, config, None)
    }

    pub fn with_config_and_shard_router(
        storage: Arc<dyn Storage>,
        config: &StorageConfig,
        shard_router: Option<ShardRouter>,
    ) -> Self {
        let shared_vector_index = storage
            .as_any()
            .downcast_ref::<FusionStorage>()
            .map(|fusion| fusion.vector_index.clone())
            .unwrap_or_else(|| Arc::new(VectorIndex::new()));
        let data_migration_fence = storage
            .as_any()
            .downcast_ref::<FusionStorage>()
            .map(|fusion| fusion.data_migration_fence());

        Self {
            storage,
            shard_router,
            statement_cache: Cache::new(config.statement_cache_capacity),
            query_result_cache: Cache::new(config.statement_cache_capacity.max(1)),
            simple_pk_update_fast_path_cache: Cache::new(config.statement_cache_capacity.max(1)),
            query_result_epoch: AtomicU64::new(0),
            prepared_statements: RwLock::new(PreparedStatementStore::default()),
            row_cache: Cache::new(config.row_cache_capacity),
            sql_bulk_scan_no_fill: config.sql_bulk_scan_no_fill,
            structured_data_shadow_v2: config.structured_data_shadow_v2,
            data_migration_fence,
            vector_index: shared_vector_index,
            embedding_registry: Arc::new(EmbeddingRegistry::new()),
        }
    }

    pub(crate) fn bulk_scan_options(&self) -> StorageScanOptions {
        if self.sql_bulk_scan_no_fill {
            StorageScanOptions::no_fill_cache()
        } else {
            StorageScanOptions::fill_cache()
        }
    }

    /// Byte-identity-validated row cache lookup: returns the cached decoded
    /// row only when `encoded` is exactly the bytes the entry was decoded
    /// from. A mismatch (any other MVCC version of the row) is a miss.
    pub(crate) fn row_cache_lookup(&self, key: &str, encoded: &[u8]) -> Option<Vec<Value>> {
        let cached = self.row_cache.get(key)?;
        if cached.encoded.as_ref() == encoded {
            monitor::inc_row_cache_hit();
            Some(cached.row)
        } else {
            None
        }
    }

    /// Store a decoded row together with the encoded bytes it came from.
    /// Only full (unprojected) rows may be stored.
    pub(crate) fn row_cache_store(&self, key: String, encoded: &[u8], row: &[Value]) {
        self.row_cache.insert(
            key,
            CachedRow {
                encoded: Arc::from(encoded),
                row: row.to_vec(),
            },
        );
    }

    pub(crate) fn sql_block_zone_map_pruning_enabled(&self) -> bool {
        SQL_BLOCK_ZONE_MAP_PRUNING_ENABLED
            .try_with(|enabled| *enabled)
            .unwrap_or(true)
    }

    pub(crate) fn columnar_single_source_aggregate_enabled(&self) -> bool {
        COLUMNAR_SINGLE_SOURCE_AGGREGATE_ENABLED
            .try_with(|enabled| *enabled)
            .unwrap_or(true)
    }

    /// Run `sql` with the 472 T1 columnar single-source aggregate fast path
    /// forced on or off. Used by tests to compare the fast path against the
    /// untouched merge path on the identical table/snapshot.
    #[cfg(test)]
    pub(crate) async fn execute_sql_with_columnar_single_source_aggregate(
        &self,
        sql: &str,
        enabled: bool,
    ) -> Result<Vec<QueryResult>> {
        COLUMNAR_SINGLE_SOURCE_AGGREGATE_ENABLED
            .scope(enabled, async move { self.execute_sql(sql).await })
            .await
    }

    pub(crate) async fn execute_sql_with_sql_block_zone_map_pruning(
        &self,
        sql: &str,
        enabled: bool,
    ) -> Result<Vec<QueryResult>> {
        SQL_BLOCK_ZONE_MAP_PRUNING_ENABLED
            .scope(enabled, async move { self.execute_sql(sql).await })
            .await
    }

    fn legacy_data_key_for_row_id(table_name: &str, row_id: &str) -> String {
        let mut key = String::with_capacity("data:".len() + table_name.len() + 1 + row_id.len());
        key.push_str("data:");
        key.push_str(table_name);
        key.push(':');
        key.push_str(row_id);
        key
    }

    fn legacy_data_prefix_for_table(table_name: &str) -> String {
        let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
        prefix.push_str("data:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix
    }

    fn schema_key_for_table(table_name: &str) -> String {
        let mut key = String::with_capacity("schema:".len() + table_name.len());
        key.push_str("schema:");
        key.push_str(table_name);
        key
    }

    fn sharded_data_prefix_for_table(shard_id: u64, table_name: &str) -> String {
        let shard = shard_id.to_string();
        let mut prefix = String::with_capacity(
            "shard:".len() + shard.len() + ":data:".len() + table_name.len() + 1,
        );
        prefix.push_str("shard:");
        prefix.push_str(&shard);
        prefix.push_str(":data:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix
    }

    fn legacy_index_prefix_for_table(table_name: &str) -> String {
        let mut prefix = String::with_capacity("index:".len() + table_name.len() + 1);
        prefix.push_str("index:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix
    }

    fn legacy_index_prefix_for_column(table_name: &str, column_name: &str) -> String {
        let mut prefix =
            String::with_capacity("index:".len() + table_name.len() + 1 + column_name.len() + 1);
        prefix.push_str("index:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix.push_str(column_name);
        prefix.push(':');
        prefix
    }

    fn sharded_index_prefix_for_table(shard_id: u64, table_name: &str) -> String {
        let shard = shard_id.to_string();
        let mut prefix = String::with_capacity(
            "shard:".len() + shard.len() + ":index:".len() + table_name.len() + 1,
        );
        prefix.push_str("shard:");
        prefix.push_str(&shard);
        prefix.push_str(":index:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix
    }

    fn sharded_index_prefix_for_column(
        shard_id: u64,
        table_name: &str,
        column_name: &str,
    ) -> String {
        let shard = shard_id.to_string();
        let mut prefix = String::with_capacity(
            "shard:".len()
                + shard.len()
                + ":index:".len()
                + table_name.len()
                + 1
                + column_name.len()
                + 1,
        );
        prefix.push_str("shard:");
        prefix.push_str(&shard);
        prefix.push_str(":index:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix.push_str(column_name);
        prefix.push(':');
        prefix
    }

    fn legacy_fts_prefix_for_table(table_name: &str) -> String {
        let mut prefix = String::with_capacity("fts:".len() + table_name.len() + 1);
        prefix.push_str("fts:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix
    }

    fn legacy_fts_prefix_for_column(table_name: &str, column_name: &str) -> String {
        let mut prefix =
            String::with_capacity("fts:".len() + table_name.len() + 1 + column_name.len() + 1);
        prefix.push_str("fts:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix.push_str(column_name);
        prefix.push(':');
        prefix
    }

    fn sharded_fts_prefix_for_table(shard_id: u64, table_name: &str) -> String {
        let shard = shard_id.to_string();
        let mut prefix = String::with_capacity(
            "shard:".len() + shard.len() + ":fts:".len() + table_name.len() + 1,
        );
        prefix.push_str("shard:");
        prefix.push_str(&shard);
        prefix.push_str(":fts:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix
    }

    fn sharded_fts_prefix_for_column(shard_id: u64, table_name: &str, column_name: &str) -> String {
        let shard = shard_id.to_string();
        let mut prefix = String::with_capacity(
            "shard:".len()
                + shard.len()
                + ":fts:".len()
                + table_name.len()
                + 1
                + column_name.len()
                + 1,
        );
        prefix.push_str("shard:");
        prefix.push_str(&shard);
        prefix.push_str(":fts:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix.push_str(column_name);
        prefix.push(':');
        prefix
    }

    pub(crate) fn routed_data_key_for_row_id(&self, table_name: &str, row_id: &str) -> String {
        if let Some(router) = &self.shard_router {
            let route = router.route_key(table_name, row_id);
            let mut key = Self::sharded_data_prefix_for_table(route.shard_id, table_name);
            key.reserve(row_id.len());
            key.push_str(row_id);
            return key;
        }

        Self::legacy_data_key_for_row_id(table_name, row_id)
    }

    pub(crate) fn legacy_delimited_index_row_ids_are_unambiguous(schema: &TableSchema) -> bool {
        schema
            .get_primary_key_index()
            .and_then(|index| schema.columns.get(index))
            .is_none_or(|column| !Self::is_text_type_name(&column.data_type))
    }

    fn structured_data_route_for_row_id(
        &self,
        table_name: &str,
        row_id: &str,
    ) -> crate::storage::keyspace::DataRoute {
        self.shard_router
            .as_ref()
            .map(|router| {
                crate::storage::keyspace::DataRoute::Shard(
                    router.route_key(table_name, row_id).shard_id,
                )
            })
            .unwrap_or(crate::storage::keyspace::DataRoute::Unsharded)
    }

    pub(crate) fn routed_structured_data_key_for_row_id(
        &self,
        table_name: &str,
        row_id: &str,
    ) -> Result<Vec<u8>> {
        crate::storage::keyspace::encode_data_key(
            self.structured_data_route_for_row_id(table_name, row_id),
            table_name.as_bytes(),
            row_id.as_bytes(),
        )
        .map_err(|error| {
            FusionError::Execution(format!("Structured data key encoding failed: {error}"))
        })
    }

    /// The phase this executor acts on when no durable record exists. It is
    /// the executor's own config flag — the pre-record contract — never the
    /// storage-baked one, so a store opened with a different StorageConfig
    /// cannot silently change shadow behavior.
    fn config_default_migration_phase(&self) -> DataMigrationPhase {
        if self.structured_data_shadow_v2 {
            DataMigrationPhase::WriteDeleteShadow
        } else {
            DataMigrationPhase::DeleteOnly
        }
    }

    /// Resolve the Data V2 migration phase this write acts on and pin it on
    /// the transaction. The pin is revalidated at commit (FusionTransaction)
    /// and recorded as a replicated precondition (RecordingTransaction), so a
    /// write can never commit under a phase other than the one it observed.
    pub(crate) async fn observe_data_migration_phase_and_fence(
        &self,
        txn: &mut dyn Transaction,
    ) -> Result<DataMigrationPhase> {
        // Row writers call this once per row. After the first row of a
        // statement the transaction already holds its pin, so the shared
        // fence lock and the boxed async fence call are both skipped.
        if let Some((phase, _)) = txn.data_migration_phase_pin() {
            return DataMigrationPhase::from_byte(phase).ok_or_else(|| {
                FusionError::Execution(format!(
                    "transaction holds an invalid Data V2 migration phase pin {phase}"
                ))
            });
        }

        let (phase, phase_seq) = match self.data_migration_fence.as_ref().map(|f| f.cached_state())
        {
            Some(CachedFenceState::Record(snapshot)) => (snapshot.phase, snapshot.phase_seq),
            Some(CachedFenceState::NoRecord) => (self.config_default_migration_phase(), 0),
            // Unknown (first touch or invalidated), or a non-Fusion engine
            // with no shared cache: read the record through this transaction.
            _ => {
                let record = txn
                    .get(migration_phase_key())
                    .await?
                    .as_deref()
                    .map(DataMigrationPhaseRecord::decode)
                    .transpose()?;
                if let Some(fence) = &self.data_migration_fence {
                    fence.resolve_with(record.as_ref());
                }
                match record {
                    Some(record) => (record.phase, record.phase_seq),
                    None => (self.config_default_migration_phase(), 0),
                }
            }
        };
        if phase > MAX_SUPPORTED_PHASE {
            return Err(FusionError::Execution(format!(
                "the store is at Data V2 migration phase '{}' which this binary does not support (max '{}')",
                phase.name(),
                MAX_SUPPORTED_PHASE.name()
            )));
        }
        txn.fence_data_migration_phase(phase.as_byte(), phase_seq)
            .await?;
        Ok(phase)
    }

    pub(crate) async fn write_routed_data_row(
        &self,
        table_name: &str,
        row_id: &str,
        value: &[u8],
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        let phase = self.observe_data_migration_phase_and_fence(txn).await?;
        let legacy_key = self.routed_data_key_for_row_id(table_name, row_id);
        let shadow_key = phase
            .shadow_writes_enabled()
            .then(|| self.routed_structured_data_key_for_row_id(table_name, row_id))
            .transpose()?;

        txn.put(legacy_key.as_bytes(), value).await?;
        if let Some(shadow_key) = shadow_key {
            txn.put(&shadow_key, value).await?;
        }
        Ok(())
    }

    pub(crate) async fn delete_routed_data_row(
        &self,
        table_name: &str,
        row_id: &str,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        // Deletes behave identically in every T1 phase (blind v2 tombstone),
        // but still fence: delete-only batches must carry the same phase
        // precondition so the later no-precondition apply guard (P10-2.3)
        // does not reject this binary's own deletes.
        self.observe_data_migration_phase_and_fence(txn).await?;
        let legacy_key = self.routed_data_key_for_row_id(table_name, row_id);
        let shadow_key = self.routed_structured_data_key_for_row_id(table_name, row_id)?;
        txn.delete(legacy_key.as_bytes()).await?;
        txn.delete(&shadow_key).await?;
        Ok(())
    }

    /// Remove this table's Data V2 shadow rows from every route that is
    /// physically present, including historical or removed shard routes the
    /// current router would not enumerate.
    ///
    /// Cost is one probe seek per route physically present plus a scan of
    /// only `(route, table)`'s own key range; the doomed keys (keys alone, no
    /// values) are buffered and deleted after the walk. A full scan of the
    /// Data namespace would make every DROP/TRUNCATE O(all shadow rows) once
    /// backfill (P10-2.3) fills that namespace. Note that on FusionStorage
    /// each scan also merges the transaction's write buffer, so a DROP that
    /// has already staged many deletes pays that merge once per route.
    ///
    /// Table identifiers are length-prefixed, so a table's range can never
    /// contain a differently named table — `t` and `t:archive` do not nest.
    pub(crate) async fn delete_structured_data_shadows_for_table(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        self.observe_data_migration_phase_and_fence(txn).await?;
        let namespace_prefix = crate::storage::keyspace::data_namespace_prefix();
        let Some(namespace_end) = crate::storage::keyspace::prefix_end(&namespace_prefix) else {
            return Ok(());
        };

        let mut cursor = namespace_prefix;
        let mut doomed: Vec<Vec<u8>> = Vec::new();
        while cursor < namespace_end {
            // One row: the first key at or after the cursor tells us which
            // route region we are standing in, without reading the region.
            let Some((probe_key, _)) = txn
                .scan_range(&cursor, &namespace_end, Some(1))
                .await?
                .into_iter()
                .next()
            else {
                break;
            };
            let route = crate::storage::keyspace::parse_data_key_exact(&probe_key)
                .map_err(|error| {
                    FusionError::Execution(format!(
                        "Malformed key in the structured Data V2 namespace: {error}"
                    ))
                })?
                .route();

            let table_prefix =
                crate::storage::keyspace::encode_data_prefix(route, table_name.as_bytes())
                    .map_err(|error| {
                        FusionError::Execution(format!(
                            "Structured data prefix encoding failed: {error}"
                        ))
                    })?;
            // Structured keys start with a `\0` magic, so a table prefix is
            // never all-`0xff` and always has a finite bound. Fail loudly
            // rather than silently skipping a route's rows if that ever
            // changes.
            let table_end =
                crate::storage::keyspace::prefix_end(&table_prefix).ok_or_else(|| {
                    FusionError::Execution(
                    "Structured data table prefix has no upper bound; refusing to skip its rows"
                        .to_string(),
                )
                })?;
            {
                let mut malformed = None;
                let mut collect = |key: &[u8], _value: &[u8]| {
                    match crate::storage::keyspace::parse_data_key_exact(key) {
                        Ok(parsed) if parsed.table() == table_name.as_bytes() => {
                            doomed.push(key.to_vec());
                            true
                        }
                        Ok(_) => true,
                        Err(error) => {
                            malformed = Some(error);
                            false
                        }
                    }
                };
                txn.scan_range_for_each(&table_prefix, &table_end, None, &mut collect)
                    .await?;
                if let Some(error) = malformed {
                    return Err(FusionError::Execution(format!(
                        "Malformed key in the structured Data V2 namespace: {error}"
                    )));
                }
            }

            // Skip the rest of this route's region: other tables there are
            // none of this cleanup's business.
            let route_prefix = crate::storage::keyspace::encode_data_route_prefix(route);
            let Some(next_cursor) = crate::storage::keyspace::prefix_end(&route_prefix) else {
                break;
            };
            cursor = next_cursor;
        }

        for key in doomed {
            txn.delete(&key).await?;
        }
        Ok(())
    }

    pub(crate) fn legacy_row_id_from_routed_data_key<'a>(
        &self,
        table_name: &str,
        key: &'a [u8],
    ) -> Result<&'a str> {
        self.routed_data_prefixes_for_table(table_name)
            .iter()
            .find_map(|prefix| key.strip_prefix(prefix.as_bytes()))
            .and_then(|row_id| std::str::from_utf8(row_id).ok())
            .filter(|row_id| !row_id.is_empty())
            .ok_or_else(|| FusionError::Execution("Invalid routed data key".to_string()))
    }

    pub(crate) fn routed_data_prefixes_for_table(&self, table_name: &str) -> Vec<String> {
        if let Some(router) = &self.shard_router {
            let shard_count = router.shard_count();
            let mut prefixes = Vec::with_capacity(shard_count as usize);
            for shard_id in 0..shard_count {
                prefixes.push(Self::sharded_data_prefix_for_table(shard_id, table_name));
            }
            return prefixes;
        }

        vec![Self::legacy_data_prefix_for_table(table_name)]
    }

    async fn load_schema_for_data_prefix_filter(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Option<TableSchema>> {
        let schema_key = Self::schema_key_for_table(table_name);
        let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? else {
            return Ok(None);
        };
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;
        Ok(Some(schema))
    }

    fn routed_data_entry_belongs_to_table(
        &self,
        table_name: &str,
        schema: Option<&TableSchema>,
        prefixes: &[String],
        key: &[u8],
        value: &[u8],
    ) -> bool {
        let Some(suffix) = prefixes
            .iter()
            .find_map(|prefix| key.strip_prefix(prefix.as_bytes()))
            .and_then(|suffix| std::str::from_utf8(suffix).ok())
        else {
            return false;
        };
        if suffix.is_empty() {
            return false;
        }

        if let Some(schema) = schema {
            if let Some(pk_idx) = schema.get_primary_key_index() {
                if let Ok(Some(pk_value)) =
                    crate::common::encoding::RowDecoder::decode_column(value, pk_idx)
                {
                    if let Some(row_id) = Self::value_to_primary_row_id(&pk_value) {
                        return self
                            .routed_data_key_for_row_id(table_name, &row_id)
                            .as_bytes()
                            == key;
                    }
                }
            }
        }

        !suffix.contains(':')
    }

    pub(crate) fn routed_index_key_for_value(
        &self,
        table_name: &str,
        column_name: &str,
        value: &str,
        row_id: &str,
    ) -> String {
        let mut key = if let Some(router) = &self.shard_router {
            let route = router.route_key(table_name, row_id);
            Self::sharded_index_prefix_for_column(route.shard_id, table_name, column_name)
        } else {
            Self::legacy_index_prefix_for_column(table_name, column_name)
        };
        key.reserve(value.len() + 1 + row_id.len());
        key.push_str(value);
        key.push(':');
        key.push_str(row_id);
        key
    }

    /// Unique-constraint sentinel key for a (table, column, value) triple.
    ///
    /// The key carries NO row-id suffix, so two concurrent transactions
    /// writing the same unique value stage the exact same key and the
    /// commit-time OCC validation deterministically aborts the loser —
    /// closing the read-then-write phantom that the scan-based duplicate
    /// check cannot see (BENCHPROD-464). Routing hashes the VALUE (not the
    /// row id), so the same value always maps to the same shard prefix
    /// regardless of where the owning rows live. Cross-node uniqueness in
    /// true multi-node deployments remains best-effort, as before.
    pub(crate) fn routed_unique_sentinel_key_for_value(
        &self,
        table_name: &str,
        column_name: &str,
        value: &str,
    ) -> String {
        let mut key = if let Some(router) = &self.shard_router {
            let route = router.route_key(table_name, value);
            let mut prefix = String::with_capacity(
                "shard:0000:unique:".len() + table_name.len() + 1 + column_name.len() + 1,
            );
            prefix.push_str("shard:");
            prefix.push_str(&route.shard_id.to_string());
            prefix.push_str(":unique:");
            prefix.push_str(table_name);
            prefix.push(':');
            prefix.push_str(column_name);
            prefix.push(':');
            prefix
        } else {
            let mut prefix = String::with_capacity(
                "unique:".len() + table_name.len() + 1 + column_name.len() + 1,
            );
            prefix.push_str("unique:");
            prefix.push_str(table_name);
            prefix.push(':');
            prefix.push_str(column_name);
            prefix.push(':');
            prefix
        };
        key.push_str(value);
        key
    }

    /// All physical prefixes that may hold unique sentinels for a table
    /// (used by TRUNCATE / DROP TABLE cleanup).
    pub(crate) fn routed_unique_sentinel_prefixes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<String> {
        if let Some(router) = &self.shard_router {
            let shard_count = router.shard_count();
            let mut prefixes = Vec::with_capacity(shard_count as usize);
            for shard_id in 0..shard_count {
                prefixes.push(format!("shard:{}:unique:{}:", shard_id, table_name));
            }
            return prefixes;
        }

        vec![format!("unique:{}:", table_name)]
    }

    pub(crate) fn routed_index_prefixes_for_table(&self, table_name: &str) -> Vec<String> {
        if let Some(router) = &self.shard_router {
            let shard_count = router.shard_count();
            let mut prefixes = Vec::with_capacity(shard_count as usize);
            for shard_id in 0..shard_count {
                prefixes.push(Self::sharded_index_prefix_for_table(shard_id, table_name));
            }
            return prefixes;
        }

        vec![Self::legacy_index_prefix_for_table(table_name)]
    }

    pub(crate) fn routed_index_prefixes_for_column(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Vec<String> {
        if let Some(router) = &self.shard_router {
            let shard_count = router.shard_count();
            let mut prefixes = Vec::with_capacity(shard_count as usize);
            for shard_id in 0..shard_count {
                prefixes.push(Self::sharded_index_prefix_for_column(
                    shard_id,
                    table_name,
                    column_name,
                ));
            }
            return prefixes;
        }

        vec![Self::legacy_index_prefix_for_column(
            table_name,
            column_name,
        )]
    }

    pub(crate) fn routed_index_prefixes_for_value(
        &self,
        table_name: &str,
        column_name: &str,
        value: &str,
    ) -> Vec<String> {
        let mut prefixes = self.routed_index_prefixes_for_column(table_name, column_name);
        for prefix in &mut prefixes {
            prefix.reserve(value.len() + 1);
            prefix.push_str(value);
            prefix.push(':');
        }
        prefixes
    }

    pub(crate) fn routed_index_prefixes_for_value_start(
        &self,
        table_name: &str,
        column_name: &str,
        value_prefix: &str,
    ) -> Vec<String> {
        let mut prefixes = self.routed_index_prefixes_for_column(table_name, column_name);
        for prefix in &mut prefixes {
            prefix.reserve(value_prefix.len());
            prefix.push_str(value_prefix);
        }
        prefixes
    }

    pub(crate) fn routed_fts_index_key_for_row(
        &self,
        table_name: &str,
        column_name: &str,
        token: &str,
        row_id: &str,
    ) -> String {
        let mut key = if let Some(router) = &self.shard_router {
            let route = router.route_key(table_name, row_id);
            Self::sharded_fts_prefix_for_column(route.shard_id, table_name, column_name)
        } else {
            Self::legacy_fts_prefix_for_column(table_name, column_name)
        };
        key.reserve(token.len() + 1 + row_id.len());
        key.push_str(token);
        key.push(':');
        key.push_str(row_id);
        key
    }

    pub(crate) fn routed_fts_prefixes_for_table(&self, table_name: &str) -> Vec<String> {
        if let Some(router) = &self.shard_router {
            let shard_count = router.shard_count();
            let mut prefixes = Vec::with_capacity(shard_count as usize);
            for shard_id in 0..shard_count {
                prefixes.push(Self::sharded_fts_prefix_for_table(shard_id, table_name));
            }
            return prefixes;
        }

        vec![Self::legacy_fts_prefix_for_table(table_name)]
    }

    pub(crate) fn routed_fts_prefixes_for_column(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Vec<String> {
        if let Some(router) = &self.shard_router {
            let shard_count = router.shard_count();
            let mut prefixes = Vec::with_capacity(shard_count as usize);
            for shard_id in 0..shard_count {
                prefixes.push(Self::sharded_fts_prefix_for_column(
                    shard_id,
                    table_name,
                    column_name,
                ));
            }
            return prefixes;
        }

        vec![Self::legacy_fts_prefix_for_column(table_name, column_name)]
    }

    pub(crate) fn routed_fts_prefixes_for_token(
        &self,
        table_name: &str,
        column_name: &str,
        token: &str,
    ) -> Vec<String> {
        let mut prefixes = self.routed_fts_prefixes_for_column(table_name, column_name);
        for prefix in &mut prefixes {
            prefix.reserve(token.len() + 1);
            prefix.push_str(token);
            prefix.push(':');
        }
        prefixes
    }

    pub(crate) async fn scan_routed_prefixes(
        &self,
        prefixes: Vec<String>,
        txn: &mut dyn Transaction,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_routed_prefixes_with_options(
            prefixes,
            txn,
            limit,
            StorageScanOptions::fill_cache(),
        )
        .await
    }

    pub(crate) async fn scan_routed_prefixes_with_options(
        &self,
        prefixes: Vec<String>,
        txn: &mut dyn Transaction,
        limit: Option<usize>,
        options: StorageScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut pairs = Vec::new();
        for prefix in prefixes {
            let remaining = limit.map(|limit| limit.saturating_sub(pairs.len()));
            if remaining == Some(0) {
                break;
            }
            let mut shard_pairs = txn
                .scan_prefix_parallel_with_options(prefix.as_bytes(), remaining, options.clone())
                .await?;
            pairs.append(&mut shard_pairs);
            if limit.is_some_and(|limit| pairs.len() >= limit) {
                break;
            }
        }
        Ok(pairs)
    }

    pub(crate) async fn scan_routed_prefixes_for_each(
        &self,
        prefixes: Vec<String>,
        txn: &mut dyn Transaction,
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> Result<usize> {
        self.scan_routed_prefixes_for_each_with_options(
            prefixes,
            txn,
            limit,
            visitor,
            StorageScanOptions::fill_cache(),
        )
        .await
    }

    pub(crate) async fn scan_routed_prefixes_for_each_with_options(
        &self,
        prefixes: Vec<String>,
        txn: &mut dyn Transaction,
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        options: StorageScanOptions,
    ) -> Result<usize> {
        let mut visited = 0usize;
        for prefix in prefixes {
            let remaining = limit.map(|limit| limit.saturating_sub(visited));
            if remaining == Some(0) {
                break;
            }
            let mut stop_aware = StopAwareScanVisitor {
                inner: visitor,
                stopped: false,
            };
            let prefix_bytes = prefix.as_bytes();
            let prefix_visited = match txn
                .scan_prefix_parallel_for_each_with_options(
                    prefix_bytes,
                    remaining,
                    &mut stop_aware,
                    options.clone(),
                )
                .await?
            {
                Some(count) => count,
                None => {
                    txn.scan_prefix_for_each_with_options(
                        prefix_bytes,
                        remaining,
                        &mut stop_aware,
                        options.clone(),
                    )
                    .await?
                }
            };
            visited += prefix_visited;
            if stop_aware.stopped {
                break;
            }
            if limit.is_some_and(|limit| visited >= limit) {
                break;
            }
        }
        Ok(visited)
    }

    pub(crate) async fn scan_routed_data_prefixes_for_table(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_routed_data_prefixes_for_table_with_options(
            table_name,
            txn,
            limit,
            StorageScanOptions::fill_cache(),
        )
        .await
    }

    pub(crate) async fn scan_routed_data_prefixes_for_table_with_options(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
        limit: Option<usize>,
        options: StorageScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // A limited scan must stop the storage scan itself, not read the whole
        // table and slice the result: passing `None` down and truncating
        // afterwards is exactly what an `ORDER BY <pk> LIMIT n` plan pushes a
        // limit down to avoid. `scan_routed_data_prefixes_for_each_with_options`
        // already counts *accepted* rows (post table-ownership filter) and stops
        // the scan, so the row-count semantics are unchanged.
        //
        // Unlimited scans deliberately keep the older materializing path. The
        // visitor API hands out `&[u8]`, so collecting through it would copy
        // every key and value a second time, whereas the parallel materializing
        // scan moves its per-partition Vec into the result. Full scans (CREATE
        // INDEX, ANALYZE, table rebuilds, unqualified DELETE/UPDATE, FK checks)
        // are the hottest path here and must not pay that.
        if let Some(limit) = limit {
            let mut pairs = Vec::with_capacity(limit.min(4096));
            let mut collect = |key: &[u8], value: &[u8]| {
                pairs.push((key.to_vec(), value.to_vec()));
                true
            };
            self.scan_routed_data_prefixes_for_each_with_options(
                table_name,
                txn,
                Some(limit),
                &mut collect,
                options,
            )
            .await?;
            return Ok(pairs);
        }

        let prefixes = self.routed_data_prefixes_for_table(table_name);
        let schema = self
            .load_schema_for_data_prefix_filter(table_name, txn)
            .await?;
        let mut pairs = Vec::new();
        for prefix in &prefixes {
            let shard_pairs = txn
                .scan_prefix_parallel_with_options(prefix.as_bytes(), None, options.clone())
                .await?;
            for (key, value) in shard_pairs {
                if !self.routed_data_entry_belongs_to_table(
                    table_name,
                    schema.as_ref(),
                    &prefixes,
                    &key,
                    &value,
                ) {
                    continue;
                }
                pairs.push((key, value));
            }
        }
        Ok(pairs)
    }

    pub(crate) async fn scan_routed_data_prefixes_for_each(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> Result<usize> {
        self.scan_routed_data_prefixes_for_each_with_options(
            table_name,
            txn,
            limit,
            visitor,
            StorageScanOptions::fill_cache(),
        )
        .await
    }

    pub(crate) async fn scan_routed_data_prefixes_for_each_with_options(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        options: StorageScanOptions,
    ) -> Result<usize> {
        let prefixes = self.routed_data_prefixes_for_table(table_name);
        let schema = self
            .load_schema_for_data_prefix_filter(table_name, txn)
            .await?;
        let mut accepted = 0usize;

        for prefix in &prefixes {
            if limit.is_some_and(|limit| accepted >= limit) {
                break;
            }
            let mut filter_visitor = ExactTableDataScanVisitor {
                executor: self,
                table_name,
                schema: schema.as_ref(),
                prefixes: &prefixes,
                inner: visitor,
                accepted: &mut accepted,
                limit,
                stopped: false,
            };
            let prefix_bytes = prefix.as_bytes();
            // A bounded scan must stay serial: the visitor self-stops after
            // `limit` accepted rows, and the serial path reads exactly that.
            // The parallel path spawns full-range partition scans first (plus
            // a first()/last() split probe whose last() is a reverse scan) —
            // all of it wasted work once the consumer stops early.
            let parallel_visited = if limit.is_some() {
                None
            } else {
                txn.scan_prefix_parallel_for_each_with_options(
                    prefix_bytes,
                    None,
                    &mut filter_visitor,
                    options.clone(),
                )
                .await?
            };
            match parallel_visited {
                Some(_) => {}
                None => {
                    txn.scan_prefix_for_each_with_options(
                        prefix_bytes,
                        None,
                        &mut filter_visitor,
                        options.clone(),
                    )
                    .await?;
                }
            }
            if filter_visitor.stopped {
                break;
            }
        }

        Ok(accepted)
    }

    pub(crate) async fn count_routed_data_prefixes_for_table(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<usize> {
        let mut visitor = |_key: &[u8], _value: &[u8]| true;
        self.scan_routed_data_prefixes_for_each(table_name, txn, None, &mut visitor)
            .await
    }

    pub(crate) async fn shard_routing_decisions_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<SqlShardRoutingDecision>> {
        let statements = self.prepare(sql)?;
        self.shard_routing_decisions_for_statements(&statements, params)
            .await
    }

    pub(crate) async fn shard_routing_decisions_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardRoutingDecision>> {
        let mut txn = self.storage.begin_transaction().await?;
        self.shard_routing_decisions_for_statements_in_transaction(statements, &mut *txn, params)
            .await
    }

    pub(crate) async fn shard_routing_decisions_for_statements_in_transaction(
        &self,
        statements: &[Statement],
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Vec<SqlShardRoutingDecision>> {
        let Some(router) = self.shard_router.clone() else {
            return Ok(Vec::new());
        };

        let mut decisions = Vec::new();
        for statement in statements {
            for (operation, table_name, row_id) in self
                .shard_point_write_targets_for_statement(statement, txn, params)
                .await?
            {
                decisions.push(Self::shard_routing_decision_for_row_id(
                    &router,
                    operation,
                    &table_name,
                    row_id,
                ));
            }
        }
        Ok(decisions)
    }

    pub(crate) async fn shard_read_route_decision_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Option<SqlShardRoutingDecision>> {
        let statements = self.prepare(sql)?;
        self.shard_read_route_decision_for_statements(&statements, params)
            .await
    }

    pub(crate) async fn shard_read_route_decision_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Option<SqlShardRoutingDecision>> {
        let mut txn = self.storage.begin_transaction().await?;
        self.shard_read_route_decision_for_statements_in_transaction(statements, &mut *txn, params)
            .await
    }

    pub(crate) async fn shard_read_route_decision_for_statements_in_transaction(
        &self,
        statements: &[Statement],
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Option<SqlShardRoutingDecision>> {
        let Some(router) = self.shard_router.clone() else {
            return Ok(None);
        };
        let [statement] = statements else {
            return Ok(None);
        };
        let Some((table_name, row_id)) = self
            .shard_point_read_target_for_statement(statement, txn, params)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(Self::shard_routing_decision_for_row_id(
            &router,
            "SELECT",
            &table_name,
            row_id,
        )))
    }

    pub(crate) async fn shard_select_fanout_owners_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let statements = self.prepare(sql)?;
        self.shard_select_fanout_owners_for_statements(&statements, params)
            .await
    }

    pub(crate) async fn shard_select_fanout_owners_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        if !Self::simple_select_fanout_eligible(statements) {
            return Ok(Vec::new());
        }
        self.shard_select_fanout_owners_for_prechecked_statements(statements, params)
            .await
    }

    pub(crate) async fn shard_count_select_fanout_owners_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let statements = self.prepare(sql)?;
        self.shard_count_select_fanout_owners_for_statements(&statements, params)
            .await
    }

    pub(crate) async fn shard_count_select_fanout_owners_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        if !Self::count_star_select_fanout_eligible(statements) {
            return Ok(Vec::new());
        }
        self.shard_select_fanout_owners_for_prechecked_statements(statements, params)
            .await
    }

    pub(crate) fn shard_group_count_select_fanout_plan_for_sql(
        &self,
        sql: &str,
    ) -> Result<Option<SqlShardGroupCountFanoutPlan>> {
        let statements = self.prepare(sql)?;
        Ok(Self::group_count_select_fanout_plan(&statements))
    }

    pub(crate) fn shard_group_count_select_fanout_plan_for_statements(
        statements: &[Statement],
    ) -> Option<SqlShardGroupCountFanoutPlan> {
        Self::group_count_select_fanout_plan(statements)
    }

    pub(crate) async fn shard_group_count_select_fanout_owners_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let statements = self.prepare(sql)?;
        self.shard_group_count_select_fanout_owners_for_statements(&statements, params)
            .await
    }

    pub(crate) async fn shard_group_count_select_fanout_owners_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        if Self::group_count_select_fanout_plan(statements).is_none() {
            return Ok(Vec::new());
        }
        self.shard_select_fanout_owners_for_prechecked_statements(statements, params)
            .await
    }

    pub(crate) fn shard_group_aggregate_select_fanout_plan_for_sql(
        &self,
        sql: &str,
    ) -> Result<Option<SqlShardGroupAggregateFanoutPlan>> {
        let statements = self.prepare(sql)?;
        Ok(Self::group_aggregate_select_fanout_target(&statements).map(|(_, _, plan)| plan))
    }

    pub(crate) fn shard_group_aggregate_select_fanout_plan_for_statements(
        statements: &[Statement],
    ) -> Option<SqlShardGroupAggregateFanoutPlan> {
        Self::group_aggregate_select_fanout_target(statements).map(|(_, _, plan)| plan)
    }

    pub(crate) async fn shard_group_aggregate_select_fanout_owners_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let statements = self.prepare(sql)?;
        self.shard_group_aggregate_select_fanout_owners_for_statements(&statements, params)
            .await
    }

    pub(crate) async fn shard_group_aggregate_select_fanout_owners_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let Some((table_name, column_name, _)) =
            Self::group_aggregate_select_fanout_target(statements)
        else {
            return Ok(Vec::new());
        };
        let mut txn = self.storage.begin_transaction().await?;
        let Some(schema) = self
            .load_table_schema_for_shard_routing(&table_name, &mut *txn)
            .await?
        else {
            return Ok(Vec::new());
        };
        let Some(column) = schema
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&column_name))
        else {
            return Ok(Vec::new());
        };
        if !Self::is_integer_type_name(&column.data_type)
            && !Self::is_float_type_name(&column.data_type)
            && !Self::is_decimal_type_name(&column.data_type)
        {
            return Ok(Vec::new());
        }
        drop(txn);
        self.shard_select_fanout_owners_for_prechecked_statements(statements, params)
            .await
    }

    pub(crate) fn shard_group_avg_select_fanout_plan_for_sql(
        &self,
        sql: &str,
    ) -> Result<Option<SqlShardGroupAvgFanoutPlan>> {
        let statements = self.prepare(sql)?;
        Ok(Self::group_avg_select_fanout_target(&statements).map(|(_, _, plan)| plan))
    }

    pub(crate) fn shard_group_avg_select_fanout_plan_for_statements(
        statements: &[Statement],
    ) -> Option<SqlShardGroupAvgFanoutPlan> {
        Self::group_avg_select_fanout_target(statements).map(|(_, _, plan)| plan)
    }

    pub(crate) async fn shard_group_avg_select_fanout_owners_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let statements = self.prepare(sql)?;
        self.shard_group_avg_select_fanout_owners_for_statements(&statements, params)
            .await
    }

    pub(crate) async fn shard_group_avg_select_fanout_owners_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let Some((table_name, column_name, _)) = Self::group_avg_select_fanout_target(statements)
        else {
            return Ok(Vec::new());
        };
        let mut txn = self.storage.begin_transaction().await?;
        let Some(schema) = self
            .load_table_schema_for_shard_routing(&table_name, &mut *txn)
            .await?
        else {
            return Ok(Vec::new());
        };
        let Some(column) = schema
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&column_name))
        else {
            return Ok(Vec::new());
        };
        if !Self::is_integer_type_name(&column.data_type)
            && !Self::is_float_type_name(&column.data_type)
            && !Self::is_decimal_type_name(&column.data_type)
        {
            return Ok(Vec::new());
        }
        drop(txn);
        self.shard_select_fanout_owners_for_prechecked_statements(statements, params)
            .await
    }

    pub(crate) async fn shard_unsupported_group_by_fanout_error_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Option<String>> {
        let statements = self.prepare(sql)?;
        self.shard_unsupported_group_by_fanout_error_for_statements(&statements, params)
            .await
    }

    /// Safety net for distributed `GROUP BY`. A single-table grouped SELECT that scatters across more
    /// than one shard owner but matches none of the supported grouped fan-out plans (`COUNT(*)`,
    /// `SUM/MIN/MAX(col)`, `AVG(col)`) would, if run locally, silently return only the local owner's
    /// groups. Detect that gap and return `Some(message)` so the caller can fail loudly instead of
    /// emitting incomplete results; return `None` when the query is safe (supported shape, single
    /// owner, unknown table, or no shard router).
    pub(crate) async fn shard_unsupported_group_by_fanout_error_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Option<String>> {
        let Some(table_name) = Self::single_table_group_by_select_table(statements) else {
            return Ok(None);
        };
        // Only `COUNT(*)` grouped fan-out may be short-circuited as "supported" on a purely STRUCTURAL
        // match: it has no column-type requirement, so a structural match always means its dispatcher
        // can fan it out. The SUM/MIN/MAX/AVG (and multi-aggregate) planners additionally require their
        // argument column to be numeric — a structurally-matching query over a NON-numeric column has
        // its dispatcher decline (empty owners), so short-circuiting it here as "supported" would let a
        // genuinely-scattering query fall through to silent local-only results. Those shapes therefore
        // fall through to the scatter check below, which fails loudly instead of returning incomplete
        // results. (An eligible, scattering aggregate is already answered by its own dispatcher before
        // this safety net is ever reached, so this never produces a false error for a supported query.)
        if Self::group_count_select_fanout_plan(statements).is_some() {
            return Ok(None);
        }
        // Only guard tables that actually exist locally; otherwise let the normal (loud) error path
        // surface "table not found" rather than masking it with a fan-out message.
        {
            let mut txn = self.storage.begin_transaction().await?;
            if self
                .load_table_schema_for_shard_routing(&table_name, &mut *txn)
                .await?
                .is_none()
            {
                return Ok(None);
            }
        }
        // Fire only when the query genuinely scatters: `shard_select_fanout_owners_for_prechecked_
        // statements` returns empty when a recognized point-read pins it to one owner (or there is no
        // router / single node), so a single-owner query is forwarded or run locally as usual. When
        // the pin is real but the point-read recognizer is too narrow to forward it, a loud error is
        // deliberately preferred over silently running local-only (which would be incomplete if the
        // pinned row lives on a remote shard).
        let owners = self
            .shard_select_fanout_owners_for_prechecked_statements(statements, params)
            .await?;
        if owners.is_empty() {
            return Ok(None);
        }
        Ok(Some(
            "Distributed GROUP BY across multiple shard owners is not supported for this query \
             shape; results would be incomplete. Supported grouped fan-out: one or more of COUNT(*), \
             COUNT(col), SUM(col), MIN(col), MAX(col) (or a single AVG(col)) with GROUP BY col[, ...], \
             optionally with HAVING / ORDER BY / LIMIT."
                .to_string(),
        ))
    }

    fn single_table_group_by_select_table(statements: &[Statement]) -> Option<String> {
        let [Statement::Query(query)] = statements else {
            return None;
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return None;
        }
        let TableFactor::Table { name, .. } = &select.from[0].relation else {
            return None;
        };
        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by else {
            return None;
        };
        if group_exprs.is_empty() {
            return None;
        }
        Some(name.to_string())
    }

    pub(crate) async fn shard_count_distinct_select_fanout_owners_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let statements = self.prepare(sql)?;
        self.shard_count_distinct_select_fanout_owners_for_statements(&statements, params)
            .await
    }

    pub(crate) fn shard_count_distinct_select_fanout_plan_for_sql(
        &self,
        sql: &str,
    ) -> Result<Option<SqlShardCountDistinctFanoutPlan>> {
        let statements = self.prepare(sql)?;
        Ok(Self::count_distinct_select_fanout_plan(&statements))
    }

    pub(crate) fn shard_count_distinct_select_fanout_plan_for_statements(
        statements: &[Statement],
    ) -> Option<SqlShardCountDistinctFanoutPlan> {
        Self::count_distinct_select_fanout_plan(statements)
    }

    pub(crate) async fn shard_count_distinct_select_fanout_owners_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        if Self::count_distinct_select_fanout_plan(statements).is_none() {
            return Ok(Vec::new());
        }
        self.shard_select_fanout_owners_for_prechecked_statements(statements, params)
            .await
    }

    pub(crate) async fn shard_sum_select_fanout_owners_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let statements = self.prepare(sql)?;
        self.shard_sum_select_fanout_owners_for_statements(&statements, params)
            .await
    }

    pub(crate) async fn shard_sum_select_fanout_owners_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        if !Self::sum_select_fanout_eligible(statements) {
            return Ok(Vec::new());
        }
        self.shard_select_fanout_owners_for_prechecked_statements(statements, params)
            .await
    }

    pub(crate) async fn shard_min_max_select_fanout_owners_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let statements = self.prepare(sql)?;
        self.shard_min_max_select_fanout_owners_for_statements(&statements, params)
            .await
    }

    pub(crate) async fn shard_avg_select_fanout_owners_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let statements = self.prepare(sql)?;
        self.shard_avg_select_fanout_owners_for_statements(&statements, params)
            .await
    }

    pub(crate) fn shard_avg_select_fanout_plan_for_sql(
        &self,
        sql: &str,
    ) -> Result<Option<SqlShardAvgFanoutPlan>> {
        let statements = self.prepare(sql)?;
        Ok(Self::avg_select_fanout_target(&statements).map(|(_, _, plan)| plan))
    }

    pub(crate) fn shard_avg_select_fanout_plan_for_statements(
        statements: &[Statement],
    ) -> Option<SqlShardAvgFanoutPlan> {
        Self::avg_select_fanout_target(statements).map(|(_, _, plan)| plan)
    }

    pub(crate) async fn shard_avg_select_fanout_owners_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let Some((table_name, column_name, _)) = Self::avg_select_fanout_target(statements) else {
            return Ok(Vec::new());
        };
        let mut txn = self.storage.begin_transaction().await?;
        let Some(schema) = self
            .load_table_schema_for_shard_routing(&table_name, &mut *txn)
            .await?
        else {
            return Ok(Vec::new());
        };
        let Some(column) = schema
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&column_name))
        else {
            return Ok(Vec::new());
        };
        if !Self::is_integer_type_name(&column.data_type)
            && !Self::is_float_type_name(&column.data_type)
            && !Self::is_decimal_type_name(&column.data_type)
        {
            return Ok(Vec::new());
        }
        drop(txn);
        self.shard_select_fanout_owners_for_prechecked_statements(statements, params)
            .await
    }

    pub(crate) fn shard_sum_distinct_select_fanout_plan_for_sql(
        &self,
        sql: &str,
    ) -> Result<Option<SqlShardDistinctAggregateFanoutPlan>> {
        let statements = self.prepare(sql)?;
        Ok(
            Self::distinct_aggregate_select_fanout_target(&statements, "SUM")
                .map(|(_, _, plan)| plan),
        )
    }

    pub(crate) fn shard_sum_distinct_select_fanout_plan_for_statements(
        statements: &[Statement],
    ) -> Option<SqlShardDistinctAggregateFanoutPlan> {
        Self::distinct_aggregate_select_fanout_target(statements, "SUM").map(|(_, _, plan)| plan)
    }

    pub(crate) async fn shard_sum_distinct_select_fanout_owners_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let statements = self.prepare(sql)?;
        self.shard_sum_distinct_select_fanout_owners_for_statements(&statements, params)
            .await
    }

    pub(crate) async fn shard_sum_distinct_select_fanout_owners_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        self.shard_distinct_aggregate_select_fanout_owners(statements, params, "SUM")
            .await
    }

    pub(crate) fn shard_avg_distinct_select_fanout_plan_for_sql(
        &self,
        sql: &str,
    ) -> Result<Option<SqlShardDistinctAggregateFanoutPlan>> {
        let statements = self.prepare(sql)?;
        Ok(
            Self::distinct_aggregate_select_fanout_target(&statements, "AVG")
                .map(|(_, _, plan)| plan),
        )
    }

    pub(crate) fn shard_avg_distinct_select_fanout_plan_for_statements(
        statements: &[Statement],
    ) -> Option<SqlShardDistinctAggregateFanoutPlan> {
        Self::distinct_aggregate_select_fanout_target(statements, "AVG").map(|(_, _, plan)| plan)
    }

    pub(crate) async fn shard_avg_distinct_select_fanout_owners_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let statements = self.prepare(sql)?;
        self.shard_avg_distinct_select_fanout_owners_for_statements(&statements, params)
            .await
    }

    pub(crate) async fn shard_avg_distinct_select_fanout_owners_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        self.shard_distinct_aggregate_select_fanout_owners(statements, params, "AVG")
            .await
    }

    async fn shard_distinct_aggregate_select_fanout_owners(
        &self,
        statements: &[Statement],
        params: &[Value],
        function_name: &str,
    ) -> Result<Vec<SqlShardOwner>> {
        let Some((table_name, column_name, _)) =
            Self::distinct_aggregate_select_fanout_target(statements, function_name)
        else {
            return Ok(Vec::new());
        };
        let mut txn = self.storage.begin_transaction().await?;
        let Some(schema) = self
            .load_table_schema_for_shard_routing(&table_name, &mut *txn)
            .await?
        else {
            return Ok(Vec::new());
        };
        let Some(column) = schema
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&column_name))
        else {
            return Ok(Vec::new());
        };
        if !Self::is_integer_type_name(&column.data_type)
            && !Self::is_float_type_name(&column.data_type)
            && !Self::is_decimal_type_name(&column.data_type)
        {
            return Ok(Vec::new());
        }
        drop(txn);
        self.shard_select_fanout_owners_for_prechecked_statements(statements, params)
            .await
    }

    pub(crate) fn shard_min_max_select_fanout_kind_for_sql(
        &self,
        sql: &str,
    ) -> Result<Option<SqlShardExtremum>> {
        let statements = self.prepare(sql)?;
        Ok(Self::min_max_select_fanout_target(&statements).map(|(kind, _, _)| kind))
    }

    pub(crate) fn shard_min_max_select_fanout_kind_for_statements(
        statements: &[Statement],
    ) -> Option<SqlShardExtremum> {
        Self::min_max_select_fanout_target(statements).map(|(kind, _, _)| kind)
    }

    pub(crate) async fn shard_min_max_select_fanout_owners_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let Some((_, table_name, column_name)) = Self::min_max_select_fanout_target(statements)
        else {
            return Ok(Vec::new());
        };
        let mut txn = self.storage.begin_transaction().await?;
        let Some(schema) = self
            .load_table_schema_for_shard_routing(&table_name, &mut *txn)
            .await?
        else {
            return Ok(Vec::new());
        };
        let Some(column) = schema
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&column_name))
        else {
            return Ok(Vec::new());
        };
        if !Self::is_integer_type_name(&column.data_type)
            && !Self::is_float_type_name(&column.data_type)
            && !Self::is_decimal_type_name(&column.data_type)
        {
            return Ok(Vec::new());
        }
        drop(txn);
        self.shard_select_fanout_owners_for_prechecked_statements(statements, params)
            .await
    }

    async fn shard_select_fanout_owners_for_prechecked_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let Some(router) = self.shard_router.clone() else {
            return Ok(Vec::new());
        };
        let mut txn = self.storage.begin_transaction().await?;
        if self
            .shard_read_route_decision_for_statements_in_transaction(statements, &mut *txn, params)
            .await?
            .is_some()
        {
            return Ok(Vec::new());
        }

        let map = router.describe();
        let mut owners = Vec::new();
        for assignment in map.assignments {
            if assignment.owner_node_id == map.local_node_id {
                continue;
            }
            if owners.iter().any(|owner: &SqlShardOwner| {
                owner.node_id == assignment.owner_node_id && owner.addr == assignment.owner_addr
            }) {
                continue;
            }
            owners.push(SqlShardOwner {
                node_id: assignment.owner_node_id,
                addr: assignment.owner_addr,
            });
        }
        Ok(owners)
    }

    fn simple_select_fanout_eligible(statements: &[Statement]) -> bool {
        let [Statement::Query(query)] = statements else {
            return false;
        };
        if query.with.is_some() || query.order_by.is_some() || query.limit_clause.is_some() {
            return false;
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return false;
        };
        if select.distinct.is_some()
            || select.having.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
        {
            return false;
        }
        if !matches!(select.from[0].relation, TableFactor::Table { .. }) {
            return false;
        }
        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by else {
            return false;
        };
        if !group_exprs.is_empty() {
            return false;
        }
        if !select
            .projection
            .iter()
            .all(Self::select_projection_is_fanout_safe)
        {
            return false;
        }
        select
            .selection
            .as_ref()
            .is_none_or(Self::select_predicate_is_fanout_local)
    }

    fn count_star_select_fanout_eligible(statements: &[Statement]) -> bool {
        let [Statement::Query(query)] = statements else {
            return false;
        };
        if query.with.is_some() || query.order_by.is_some() || query.limit_clause.is_some() {
            return false;
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return false;
        };
        if select.distinct.is_some()
            || select.having.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || select.projection.len() != 1
        {
            return false;
        }
        if !matches!(select.from[0].relation, TableFactor::Table { .. }) {
            return false;
        }
        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by else {
            return false;
        };
        if !group_exprs.is_empty() {
            return false;
        }
        if !Self::select_projection_is_count_star(&select.projection[0]) {
            return false;
        }
        select
            .selection
            .as_ref()
            .is_none_or(Self::select_predicate_is_fanout_local)
    }

    fn select_projection_is_count_star(item: &SelectItem) -> bool {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return false,
        };
        let Expr::Function(func) = expr else {
            return false;
        };
        if func.name.0.len() != 1
            || !func.name.0[0]
                .as_ident()
                .is_some_and(|ident| ident.value.eq_ignore_ascii_case("COUNT"))
        {
            return false;
        }
        let FunctionArguments::List(args) = &func.args else {
            return false;
        };
        args.args.len() == 1
            && matches!(
                args.args[0],
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
            )
    }

    fn count_distinct_select_fanout_plan(
        statements: &[Statement],
    ) -> Option<SqlShardCountDistinctFanoutPlan> {
        let [Statement::Query(query)] = statements else {
            return None;
        };
        if query.with.is_some() || query.order_by.is_some() || query.limit_clause.is_some() {
            return None;
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        if select.distinct.is_some()
            || select.having.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || select.projection.len() != 1
        {
            return None;
        }
        if !matches!(select.from[0].relation, TableFactor::Table { .. }) {
            return None;
        }
        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by else {
            return None;
        };
        if !group_exprs.is_empty()
            || !select
                .selection
                .as_ref()
                .is_none_or(Self::select_predicate_is_fanout_local)
        {
            return None;
        }
        let (_, column_sql) = Self::select_projection_distinct_column_function_arg(
            &select.projection[0],
            &["COUNT"],
        )?;
        let output_column = Self::select_projection_output_name(&select.projection[0])?;
        let relation_sql = select.from[0].relation.to_string();
        let selection_sql = select
            .selection
            .as_ref()
            .map(|expr| format!(" WHERE {}", expr))
            .unwrap_or_default();
        Some(SqlShardCountDistinctFanoutPlan {
            rewritten_sql: format!(
                "SELECT DISTINCT {} FROM {}{}",
                column_sql, relation_sql, selection_sql
            ),
            output_column,
        })
    }

    fn group_count_select_fanout_plan(
        statements: &[Statement],
    ) -> Option<SqlShardGroupCountFanoutPlan> {
        let [Statement::Query(query)] = statements else {
            return None;
        };
        // `ORDER BY` / `LIMIT` / `OFFSET` are supported via post-merge; CTEs (`WITH`) and the separate
        // `FETCH FIRST n ROWS` clause are not (rejected → 449 loud error). `HAVING` is still rejected.
        if query.with.is_some() || query.fetch.is_some() {
            return None;
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        if select.distinct.is_some() || select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return None;
        }
        // `HAVING` is supported via post-merge (resolved below); it is NOT rejected here.
        if !matches!(select.from[0].relation, TableFactor::Table { .. }) {
            return None;
        }
        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, group_modifiers) =
            &select.group_by
        else {
            return None;
        };
        if group_exprs.is_empty() || !group_modifiers.is_empty() {
            return None;
        }
        // Projection must be exactly the group columns (any order) plus one COUNT(*).
        if select.projection.len() != group_exprs.len() + 1 {
            return None;
        }
        let mut group_set = Vec::with_capacity(group_exprs.len());
        for group_expr in group_exprs {
            let name = Self::fanout_group_column_name(group_expr)?;
            if group_set
                .iter()
                .any(|existing: &String| existing.eq_ignore_ascii_case(&name))
            {
                return None; // duplicate group column
            }
            group_set.push(name);
        }
        if !select
            .selection
            .as_ref()
            .is_none_or(Self::select_predicate_is_fanout_local)
        {
            return None;
        }
        let mut count_index = None;
        let mut group_indices = Vec::with_capacity(group_exprs.len());
        let mut matched: Vec<String> = Vec::with_capacity(group_exprs.len());
        for (index, item) in select.projection.iter().enumerate() {
            if Self::select_projection_is_count_star(item) {
                if count_index.is_some() {
                    return None;
                }
                count_index = Some(index);
            } else if let Some(name) = Self::fanout_projection_column_name(item) {
                if !group_set.iter().any(|g| g.eq_ignore_ascii_case(&name))
                    || matched.iter().any(|m| m.eq_ignore_ascii_case(&name))
                {
                    return None; // not a group column, or duplicated in projection
                }
                matched.push(name);
                group_indices.push(index);
            } else {
                return None;
            }
        }
        let count_index = count_index?;
        if group_indices.len() != group_exprs.len() {
            return None;
        }
        let output_columns = select
            .projection
            .iter()
            .map(Self::select_projection_output_name)
            .collect::<Option<Vec<_>>>()?;
        let post_merge = if query.order_by.is_some()
            || query.limit_clause.is_some()
            || select.having.is_some()
        {
            let (order_keys, limit, offset, having) = Self::resolve_grouped_order_limit(
                query,
                select.having.as_ref(),
                &select.projection,
                &output_columns,
            )?;
            Some(GroupedPostMerge {
                per_owner_sql: Self::strip_grouped_post_merge_clauses(query),
                having,
                order_keys,
                limit,
                offset,
            })
        } else {
            None
        };
        Some(SqlShardGroupCountFanoutPlan {
            group_indices,
            count_index,
            output_columns,
            post_merge,
        })
    }

    fn group_aggregate_select_fanout_target(
        statements: &[Statement],
    ) -> Option<(String, String, SqlShardGroupAggregateFanoutPlan)> {
        let [Statement::Query(query)] = statements else {
            return None;
        };
        // `ORDER BY` / `LIMIT` / `OFFSET` are supported via post-merge (see `post_merge` below); CTEs
        // (`WITH`) and the separate `FETCH FIRST n ROWS` clause are not. Rejecting `fetch` here routes
        // such queries to the 449 loud-error net rather than silently dropping the row limit (it lives
        // in `Query.fetch`, distinct from `limit_clause`, so the post-merge path would never see it).
        // `HAVING` is still rejected (handled later, in 452).
        if query.with.is_some() || query.fetch.is_some() {
            return None;
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        if select.distinct.is_some() || select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return None;
        }
        // `HAVING` is supported via post-merge (resolved below); it is NOT rejected here.
        let TableFactor::Table { name, .. } = &select.from[0].relation else {
            return None;
        };
        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, group_modifiers) =
            &select.group_by
        else {
            return None;
        };
        if group_exprs.is_empty() || !group_modifiers.is_empty() {
            return None;
        }
        if select.projection.len() != group_exprs.len() + 1 {
            return None;
        }
        let mut group_set = Vec::with_capacity(group_exprs.len());
        for group_expr in group_exprs {
            let n = Self::fanout_group_column_name(group_expr)?;
            if group_set
                .iter()
                .any(|e: &String| e.eq_ignore_ascii_case(&n))
            {
                return None;
            }
            group_set.push(n);
        }
        if !select
            .selection
            .as_ref()
            .is_none_or(Self::select_predicate_is_fanout_local)
        {
            return None;
        }
        let mut agg: Option<(usize, String, SqlShardGroupAggregateKind)> = None;
        let mut group_indices = Vec::with_capacity(group_exprs.len());
        let mut matched: Vec<String> = Vec::with_capacity(group_exprs.len());
        for (index, item) in select.projection.iter().enumerate() {
            if let Some((col_name, _)) =
                Self::select_projection_column_function_arg(item, &["SUM", "MIN", "MAX"])
            {
                if agg.is_some() {
                    return None;
                }
                let kind = match Self::select_projection_function_name(item) {
                    Some(n) if n.eq_ignore_ascii_case("SUM") => SqlShardGroupAggregateKind::Sum,
                    Some(n) if n.eq_ignore_ascii_case("MIN") => SqlShardGroupAggregateKind::Min,
                    Some(n) if n.eq_ignore_ascii_case("MAX") => SqlShardGroupAggregateKind::Max,
                    _ => return None,
                };
                agg = Some((index, col_name, kind));
            } else if let Some(name) = Self::fanout_projection_column_name(item) {
                if !group_set.iter().any(|g| g.eq_ignore_ascii_case(&name))
                    || matched.iter().any(|m| m.eq_ignore_ascii_case(&name))
                {
                    return None;
                }
                matched.push(name);
                group_indices.push(index);
            } else {
                return None;
            }
        }
        let (agg_index, agg_column, kind) = agg?;
        if group_indices.len() != group_exprs.len() {
            return None;
        }
        let output_columns = select
            .projection
            .iter()
            .map(Self::select_projection_output_name)
            .collect::<Option<Vec<_>>>()?;
        // Resolve any ORDER BY / LIMIT / OFFSET into a post-merge spec. When clauses are present but
        // cannot be resolved to output columns / numeric literals, return None so the 449 safety net
        // errors loudly rather than emitting a silently-unsorted distributed result.
        let post_merge = if query.order_by.is_some()
            || query.limit_clause.is_some()
            || select.having.is_some()
        {
            let (order_keys, limit, offset, having) = Self::resolve_grouped_order_limit(
                query,
                select.having.as_ref(),
                &select.projection,
                &output_columns,
            )?;
            Some(GroupedPostMerge {
                per_owner_sql: Self::strip_grouped_post_merge_clauses(query),
                having,
                order_keys,
                limit,
                offset,
            })
        } else {
            None
        };
        Some((
            name.to_string(),
            agg_column,
            SqlShardGroupAggregateFanoutPlan {
                group_indices,
                agg_index,
                kind,
                output_columns,
                post_merge,
            },
        ))
    }

    /// Resolve a grouped query's `HAVING` + `ORDER BY` keys + `LIMIT`/`OFFSET` into
    /// `(order_keys, limit, offset, having)`, or `None` if any part is unsupported (so the caller
    /// rejects the whole plan → 449 loud error). Over-rejection is deliberate: a wrong distributed
    /// answer is worse than a loud "unsupported". The per-owner SQL is built by each variant separately
    /// (clause-stripped original, or — for AVG — the already clause-free rewritten SUM/COUNT query).
    #[allow(clippy::type_complexity)]
    fn resolve_grouped_order_limit(
        query: &Query,
        having: Option<&Expr>,
        projection: &[SelectItem],
        output_columns: &[String],
    ) -> Option<(
        Vec<GroupedOrderKey>,
        Option<usize>,
        usize,
        Option<GroupedHaving>,
    )> {
        let mut order_keys = Vec::new();
        if let Some(order_by) = &query.order_by {
            if order_by.interpolate.is_some() {
                return None;
            }
            let OrderByKind::Expressions(exprs) = &order_by.kind else {
                return None; // ORDER BY ALL (DuckDB/ClickHouse) unsupported
            };
            if exprs.is_empty() {
                return None;
            }
            for order_expr in exprs {
                if order_expr.with_fill.is_some() {
                    return None;
                }
                let col_index = Self::resolve_grouped_order_column(
                    &order_expr.expr,
                    projection,
                    output_columns,
                )?;
                let asc = order_expr.options.asc.unwrap_or(true);
                // SQL default NULL placement: NULLS LAST for ASC, NULLS FIRST for DESC.
                let nulls_first = order_expr.options.nulls_first.unwrap_or(!asc);
                order_keys.push(GroupedOrderKey {
                    col_index,
                    asc,
                    nulls_first,
                });
            }
        }
        let (limit, offset) = match &query.limit_clause {
            Some(LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) => {
                if !limit_by.is_empty() {
                    return None; // ClickHouse LIMIT BY unsupported
                }
                let limit = match limit {
                    Some(expr) => Some(Self::grouped_usize_literal(expr)?),
                    None => None,
                };
                let offset = match offset {
                    Some(offset) => Self::grouped_usize_literal(&offset.value)?,
                    None => 0,
                };
                (limit, offset)
            }
            Some(LimitClause::OffsetCommaLimit { .. }) => return None, // MySQL `LIMIT a, b` form
            None => (None, 0),
        };
        let having = match having {
            Some(expr) => Some(Self::resolve_grouped_having(
                expr,
                projection,
                output_columns,
            )?),
            None => None,
        };
        Some((order_keys, limit, offset, having))
    }

    /// Resolve a `HAVING` predicate into an AND of `<output-col> <cmp> <literal>` conjuncts, or `None`
    /// if it is not that conjunctive comparison shape (e.g. contains `OR`, a non-literal RHS, or a
    /// reference that is not an output column). `None` ⇒ the caller rejects the plan ⇒ 449 loud error.
    fn resolve_grouped_having(
        expr: &Expr,
        projection: &[SelectItem],
        output_columns: &[String],
    ) -> Option<GroupedHaving> {
        let mut conjuncts = Vec::new();
        Self::collect_grouped_having_conjuncts(expr, projection, output_columns, &mut conjuncts)?;
        if conjuncts.is_empty() {
            return None;
        }
        Some(GroupedHaving { conjuncts })
    }

    fn collect_grouped_having_conjuncts(
        expr: &Expr,
        projection: &[SelectItem],
        output_columns: &[String],
        out: &mut Vec<GroupedHavingConjunct>,
    ) -> Option<()> {
        match expr {
            Expr::Nested(inner) => {
                Self::collect_grouped_having_conjuncts(inner, projection, output_columns, out)
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                Self::collect_grouped_having_conjuncts(left, projection, output_columns, out)?;
                Self::collect_grouped_having_conjuncts(right, projection, output_columns, out)
            }
            Expr::BinaryOp { left, op, right } => {
                let conjunct = Self::resolve_grouped_having_comparison(
                    left,
                    op,
                    right,
                    projection,
                    output_columns,
                )?;
                out.push(conjunct);
                Some(())
            }
            _ => None,
        }
    }

    /// Resolve one comparison to a conjunct. Exactly one side must be a literal and the other an output
    /// column; the operator is normalized so the column is on the left (flipping `<`/`>` when the literal
    /// is on the left). The literal side is extracted FIRST so a bare integer is never misread as a
    /// positional column reference (HAVING — unlike ORDER BY — has no positional semantics); a
    /// comparison with no literal side, or with two literals, resolves to `None` → 449 loud error.
    fn resolve_grouped_having_comparison(
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        projection: &[SelectItem],
        output_columns: &[String],
    ) -> Option<GroupedHavingConjunct> {
        let base_op = match op {
            BinaryOperator::Gt => GroupedHavingOp::Gt,
            BinaryOperator::GtEq => GroupedHavingOp::GtEq,
            BinaryOperator::Lt => GroupedHavingOp::Lt,
            BinaryOperator::LtEq => GroupedHavingOp::LtEq,
            BinaryOperator::Eq => GroupedHavingOp::Eq,
            BinaryOperator::NotEq => GroupedHavingOp::NotEq,
            _ => return None,
        };
        // `<column> <op> <literal>`: extract the literal on the right, resolve the column on the left.
        if let Some(literal) = Self::grouped_having_literal(right) {
            let col_index = Self::resolve_grouped_having_column(left, projection, output_columns)?;
            return Some(GroupedHavingConjunct {
                col_index,
                op: base_op,
                literal,
            });
        }
        // `<literal> <op> <column>`: extract the literal on the left, flip the operator.
        if let Some(literal) = Self::grouped_having_literal(left) {
            let col_index = Self::resolve_grouped_having_column(right, projection, output_columns)?;
            return Some(GroupedHavingConjunct {
                col_index,
                op: Self::flip_grouped_having_op(base_op),
                literal,
            });
        }
        None
    }

    /// Resolve a HAVING comparison operand to an output column index. Unlike ORDER BY, HAVING has no
    /// positional column references, so a bare literal (number/string/bool/NULL, or a negated number)
    /// is never a column — reject it before delegating to the shared resolver (whose positional branch
    /// would otherwise read a bare integer as a 1-based column index).
    fn resolve_grouped_having_column(
        expr: &Expr,
        projection: &[SelectItem],
        output_columns: &[String],
    ) -> Option<usize> {
        if matches!(expr, Expr::Value(_))
            || matches!(
                expr,
                Expr::UnaryOp {
                    op: sqlparser::ast::UnaryOperator::Minus,
                    ..
                }
            )
        {
            return None;
        }
        Self::resolve_grouped_order_column(expr, projection, output_columns)
    }

    fn flip_grouped_having_op(op: GroupedHavingOp) -> GroupedHavingOp {
        match op {
            GroupedHavingOp::Gt => GroupedHavingOp::Lt,
            GroupedHavingOp::GtEq => GroupedHavingOp::LtEq,
            GroupedHavingOp::Lt => GroupedHavingOp::Gt,
            GroupedHavingOp::LtEq => GroupedHavingOp::GtEq,
            GroupedHavingOp::Eq => GroupedHavingOp::Eq,
            GroupedHavingOp::NotEq => GroupedHavingOp::NotEq,
        }
    }

    /// Extract a scalar literal (number incl. negative, single-quoted string, boolean, NULL) as the
    /// `serde_json::Value` the merged output row carries, so HAVING compares like-for-like. Returns
    /// `None` for anything non-literal (a column, function call, parameter, etc.).
    fn grouped_having_literal(expr: &Expr) -> Option<serde_json::Value> {
        match expr {
            Expr::Value(sqlparser::ast::ValueWithSpan { value, .. }) => match value {
                sqlparser::ast::Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Some(serde_json::Value::Number(i.into()))
                    } else {
                        let f = n.parse::<f64>().ok()?;
                        serde_json::Number::from_f64(f).map(serde_json::Value::Number)
                    }
                }
                sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::DoubleQuotedString(s) => {
                    Some(serde_json::Value::String(s.clone()))
                }
                sqlparser::ast::Value::Boolean(b) => Some(serde_json::Value::Bool(*b)),
                sqlparser::ast::Value::Null => Some(serde_json::Value::Null),
                _ => None,
            },
            Expr::UnaryOp {
                op: sqlparser::ast::UnaryOperator::Minus,
                expr,
            } => match Self::grouped_having_literal(expr)? {
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Some(serde_json::Value::Number((-i).into()))
                    } else {
                        let f = -n.as_f64()?;
                        serde_json::Number::from_f64(f).map(serde_json::Value::Number)
                    }
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// Resolve one `ORDER BY` expression to an output column index: positional (`ORDER BY 2`), a bare
    /// identifier matching an output column name/alias, or a projection expression (e.g. the
    /// `SUM(amount)` aggregate item). Returns `None` for anything else.
    fn resolve_grouped_order_column(
        expr: &Expr,
        projection: &[SelectItem],
        output_columns: &[String],
    ) -> Option<usize> {
        if let Expr::Value(sqlparser::ast::ValueWithSpan {
            value: sqlparser::ast::Value::Number(n, _),
            ..
        }) = expr
        {
            let index = n.parse::<usize>().ok()?.checked_sub(1)?;
            return (index < output_columns.len()).then_some(index);
        }
        if let Expr::Identifier(ident) = expr {
            if let Some(index) = output_columns
                .iter()
                .position(|column| column.eq_ignore_ascii_case(&ident.value))
            {
                return Some(index);
            }
        }
        projection.iter().enumerate().find_map(|(index, item)| match item {
            SelectItem::UnnamedExpr(proj_expr) if proj_expr == expr => Some(index),
            SelectItem::ExprWithAlias { expr: proj_expr, alias }
                if proj_expr == expr
                    || matches!(expr, Expr::Identifier(ident) if alias.value.eq_ignore_ascii_case(&ident.value)) =>
            {
                Some(index)
            }
            _ => None,
        })
    }

    fn grouped_usize_literal(expr: &Expr) -> Option<usize> {
        match expr {
            Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::Number(n, _),
                ..
            }) => n.parse::<usize>().ok(),
            _ => None,
        }
    }

    /// Produce the per-owner SQL: the original grouped query with `ORDER BY` / `LIMIT` / `OFFSET` and
    /// the inner `HAVING` removed, so every owner returns ALL its (partial) groups. The clauses are
    /// re-applied once, post-merge, on the globally combined rows.
    fn strip_grouped_post_merge_clauses(query: &Query) -> String {
        let mut stripped = query.clone();
        stripped.order_by = None;
        stripped.limit_clause = None;
        stripped.fetch = None;
        if let SetExpr::Select(select) = stripped.body.as_mut() {
            select.having = None;
        }
        stripped.to_string()
    }

    fn group_avg_select_fanout_target(
        statements: &[Statement],
    ) -> Option<(String, String, SqlShardGroupAvgFanoutPlan)> {
        let [Statement::Query(query)] = statements else {
            return None;
        };
        // `ORDER BY` / `LIMIT` / `OFFSET` are supported via post-merge; CTEs (`WITH`) and the separate
        // `FETCH FIRST n ROWS` clause are not (rejected → 449 loud error). `HAVING` is still rejected.
        if query.with.is_some() || query.fetch.is_some() {
            return None;
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        if select.distinct.is_some() || select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return None;
        }
        // `HAVING` is supported via post-merge (resolved below); it is NOT rejected here.
        let TableFactor::Table { name, .. } = &select.from[0].relation else {
            return None;
        };
        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, group_modifiers) =
            &select.group_by
        else {
            return None;
        };
        if group_exprs.is_empty() || !group_modifiers.is_empty() {
            return None;
        }
        if select.projection.len() != group_exprs.len() + 1 {
            return None;
        }
        let mut group_set = Vec::with_capacity(group_exprs.len());
        for group_expr in group_exprs {
            let n = Self::fanout_group_column_name(group_expr)?;
            if group_set
                .iter()
                .any(|e: &String| e.eq_ignore_ascii_case(&n))
            {
                return None;
            }
            group_set.push(n);
        }
        if !select
            .selection
            .as_ref()
            .is_none_or(Self::select_predicate_is_fanout_local)
        {
            return None;
        }
        let mut avg: Option<(usize, String, String)> = None;
        let mut group_indices = Vec::with_capacity(group_exprs.len());
        let mut matched: Vec<String> = Vec::with_capacity(group_exprs.len());
        for (index, item) in select.projection.iter().enumerate() {
            if let Some((col_name, col_sql)) =
                Self::select_projection_column_function_arg(item, &["AVG"])
            {
                if avg.is_some() {
                    return None;
                }
                avg = Some((index, col_name, col_sql));
            } else if let Some(name) = Self::fanout_projection_column_name(item) {
                if !group_set.iter().any(|g| g.eq_ignore_ascii_case(&name))
                    || matched.iter().any(|m| m.eq_ignore_ascii_case(&name))
                {
                    return None;
                }
                matched.push(name);
                group_indices.push(index);
            } else {
                return None;
            }
        }
        let (avg_index, column_name, arg_sql) = avg?;
        if group_indices.len() != group_exprs.len() {
            return None;
        }
        // AVG expands into SUM + COUNT in the rewritten projection, so group columns positioned after
        // the AVG slot shift by +1 in the rewritten result.
        let group_indices: Vec<usize> = group_indices
            .into_iter()
            .map(|i| if i > avg_index { i + 1 } else { i })
            .collect();
        let output_columns = select
            .projection
            .iter()
            .map(Self::select_projection_output_name)
            .collect::<Option<Vec<_>>>()?;

        // Rewrite the per-owner query: keep every projection item verbatim except the AVG slot, which
        // becomes `SUM(arg), COUNT(arg)` so partial sums and (non-null) counts can be merged across
        // owners. FROM / WHERE / GROUP BY are preserved verbatim.
        let mut proj_parts = Vec::with_capacity(select.projection.len() + 1);
        for (index, item) in select.projection.iter().enumerate() {
            if index == avg_index {
                proj_parts.push(format!("SUM({}), COUNT({})", arg_sql, arg_sql));
            } else {
                proj_parts.push(item.to_string());
            }
        }
        let relation_sql = select.from[0].relation.to_string();
        let selection_sql = select
            .selection
            .as_ref()
            .map(|expr| format!(" WHERE {}", expr))
            .unwrap_or_default();
        let group_by_sql = group_exprs
            .iter()
            .map(|expr| expr.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let rewritten_sql = format!(
            "SELECT {} FROM {}{} GROUP BY {}",
            proj_parts.join(", "),
            relation_sql,
            selection_sql,
            group_by_sql
        );

        // ORDER BY keys / HAVING resolve against `output_columns` (the ORIGINAL projection layout),
        // which is exactly the layout of the rebuilt AVG rows — so post-merge filter/sort/slice index
        // correctly. AVG owners always run the (already clause-free) `rewritten_sql`, so `per_owner_sql`
        // mirrors it; HAVING is evaluated post-merge on the divided AVG value, not on the partials.
        let post_merge = if query.order_by.is_some()
            || query.limit_clause.is_some()
            || select.having.is_some()
        {
            let (order_keys, limit, offset, having) = Self::resolve_grouped_order_limit(
                query,
                select.having.as_ref(),
                &select.projection,
                &output_columns,
            )?;
            Some(GroupedPostMerge {
                per_owner_sql: rewritten_sql.clone(),
                having,
                order_keys,
                limit,
                offset,
            })
        } else {
            None
        };

        Some((
            name.to_string(),
            column_name,
            SqlShardGroupAvgFanoutPlan {
                rewritten_sql,
                group_indices,
                sum_index: avg_index,
                count_index: avg_index + 1,
                avg_output_index: avg_index,
                output_columns,
                post_merge,
            },
        ))
    }

    /// Match `SELECT g1[, ...], AGG1, AGG2[, ...] FROM t [WHERE ...] GROUP BY g1[, ...]` where every
    /// AGG is `COUNT(*)` / `COUNT(col)` / `SUM(col)` / `MIN(col)` / `MAX(col)` (no `AVG`, no `DISTINCT`).
    /// Returns `(table, numeric_required_columns, plan)`; `numeric_required_columns` are the `SUM`/`MIN`/
    /// `MAX` argument columns the owner-eligibility check must verify are numeric. Dispatched AFTER the
    /// single-aggregate planners, so it effectively handles the multi-aggregate and mixed shapes.
    fn group_multi_aggregate_select_fanout_target(
        statements: &[Statement],
    ) -> Option<(String, Vec<String>, SqlShardGroupMultiAggregateFanoutPlan)> {
        let [Statement::Query(query)] = statements else {
            return None;
        };
        if query.with.is_some() || query.fetch.is_some() {
            return None;
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        if select.distinct.is_some() || select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return None;
        }
        let TableFactor::Table { name, .. } = &select.from[0].relation else {
            return None;
        };
        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, group_modifiers) =
            &select.group_by
        else {
            return None;
        };
        if group_exprs.is_empty() || !group_modifiers.is_empty() {
            return None;
        }
        // Projection must be the group columns plus at least one aggregate.
        if select.projection.len() <= group_exprs.len() {
            return None;
        }
        let mut group_set = Vec::with_capacity(group_exprs.len());
        for group_expr in group_exprs {
            let n = Self::fanout_group_column_name(group_expr)?;
            if group_set
                .iter()
                .any(|e: &String| e.eq_ignore_ascii_case(&n))
            {
                return None;
            }
            group_set.push(n);
        }
        if !select
            .selection
            .as_ref()
            .is_none_or(Self::select_predicate_is_fanout_local)
        {
            return None;
        }
        let mut group_indices = Vec::with_capacity(group_exprs.len());
        let mut matched: Vec<String> = Vec::with_capacity(group_exprs.len());
        let mut aggregates = Vec::new();
        let mut numeric_columns = Vec::new();
        for (index, item) in select.projection.iter().enumerate() {
            if Self::select_projection_is_count_star(item) {
                // COUNT(*) merges by summing the per-owner counts.
                aggregates.push(GroupMultiAggregate {
                    output_index: index,
                    kind: SqlShardGroupAggregateKind::Sum,
                });
            } else if let Some(func_name) = Self::select_projection_function_name(item) {
                if func_name.eq_ignore_ascii_case("COUNT") {
                    // COUNT(col): reject DISTINCT (returns None), merge by summing partial counts.
                    Self::select_projection_column_function_arg(item, &["COUNT"])?;
                    aggregates.push(GroupMultiAggregate {
                        output_index: index,
                        kind: SqlShardGroupAggregateKind::Sum,
                    });
                } else if func_name.eq_ignore_ascii_case("SUM")
                    || func_name.eq_ignore_ascii_case("MIN")
                    || func_name.eq_ignore_ascii_case("MAX")
                {
                    let (column, _) =
                        Self::select_projection_column_function_arg(item, &["SUM", "MIN", "MAX"])?;
                    let kind = if func_name.eq_ignore_ascii_case("SUM") {
                        SqlShardGroupAggregateKind::Sum
                    } else if func_name.eq_ignore_ascii_case("MIN") {
                        SqlShardGroupAggregateKind::Min
                    } else {
                        SqlShardGroupAggregateKind::Max
                    };
                    numeric_columns.push(column);
                    aggregates.push(GroupMultiAggregate {
                        output_index: index,
                        kind,
                    });
                } else {
                    return None; // AVG or any other function is not directly mergeable here
                }
            } else if let Some(name) = Self::fanout_projection_column_name(item) {
                if !group_set.iter().any(|g| g.eq_ignore_ascii_case(&name))
                    || matched.iter().any(|m| m.eq_ignore_ascii_case(&name))
                {
                    return None;
                }
                matched.push(name);
                group_indices.push(index);
            } else {
                return None;
            }
        }
        if group_indices.len() != group_exprs.len() || aggregates.is_empty() {
            return None;
        }
        let output_columns = select
            .projection
            .iter()
            .map(Self::select_projection_output_name)
            .collect::<Option<Vec<_>>>()?;
        let post_merge = if query.order_by.is_some()
            || query.limit_clause.is_some()
            || select.having.is_some()
        {
            let (order_keys, limit, offset, having) = Self::resolve_grouped_order_limit(
                query,
                select.having.as_ref(),
                &select.projection,
                &output_columns,
            )?;
            Some(GroupedPostMerge {
                per_owner_sql: Self::strip_grouped_post_merge_clauses(query),
                having,
                order_keys,
                limit,
                offset,
            })
        } else {
            None
        };
        Some((
            name.to_string(),
            numeric_columns,
            SqlShardGroupMultiAggregateFanoutPlan {
                group_indices,
                aggregates,
                output_columns,
                post_merge,
            },
        ))
    }

    pub(crate) fn shard_group_multi_aggregate_select_fanout_plan_for_sql(
        &self,
        sql: &str,
    ) -> Result<Option<SqlShardGroupMultiAggregateFanoutPlan>> {
        let statements = self.prepare(sql)?;
        Ok(Self::group_multi_aggregate_select_fanout_target(&statements).map(|(_, _, plan)| plan))
    }

    pub(crate) fn shard_group_multi_aggregate_select_fanout_plan_for_statements(
        statements: &[Statement],
    ) -> Option<SqlShardGroupMultiAggregateFanoutPlan> {
        Self::group_multi_aggregate_select_fanout_target(statements).map(|(_, _, plan)| plan)
    }

    pub(crate) async fn shard_group_multi_aggregate_select_fanout_owners_for_sql(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let statements = self.prepare(sql)?;
        self.shard_group_multi_aggregate_select_fanout_owners_for_statements(&statements, params)
            .await
    }

    pub(crate) async fn shard_group_multi_aggregate_select_fanout_owners_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> Result<Vec<SqlShardOwner>> {
        let Some((table_name, numeric_columns, _)) =
            Self::group_multi_aggregate_select_fanout_target(statements)
        else {
            return Ok(Vec::new());
        };
        if !numeric_columns.is_empty() {
            let mut txn = self.storage.begin_transaction().await?;
            let Some(schema) = self
                .load_table_schema_for_shard_routing(&table_name, &mut *txn)
                .await?
            else {
                return Ok(Vec::new());
            };
            for column_name in &numeric_columns {
                let Some(column) = schema
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(column_name))
                else {
                    return Ok(Vec::new());
                };
                if !Self::is_integer_type_name(&column.data_type)
                    && !Self::is_float_type_name(&column.data_type)
                    && !Self::is_decimal_type_name(&column.data_type)
                {
                    return Ok(Vec::new());
                }
            }
        }
        self.shard_select_fanout_owners_for_prechecked_statements(statements, params)
            .await
    }

    fn fanout_group_column_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.clone()),
            _ => None,
        }
    }

    fn fanout_projection_column_name(item: &SelectItem) -> Option<String> {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return None,
        };
        match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.clone()),
            _ => None,
        }
    }

    fn sum_select_fanout_eligible(statements: &[Statement]) -> bool {
        let [Statement::Query(query)] = statements else {
            return false;
        };
        if query.with.is_some() || query.order_by.is_some() || query.limit_clause.is_some() {
            return false;
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return false;
        };
        if select.distinct.is_some()
            || select.having.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || select.projection.len() != 1
        {
            return false;
        }
        if !matches!(select.from[0].relation, TableFactor::Table { .. }) {
            return false;
        }
        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by else {
            return false;
        };
        if !group_exprs.is_empty() {
            return false;
        }
        if Self::select_projection_is_column_function(&select.projection[0], &["SUM"]).is_none() {
            return false;
        }
        select
            .selection
            .as_ref()
            .is_none_or(Self::select_predicate_is_fanout_local)
    }

    fn avg_select_fanout_target(
        statements: &[Statement],
    ) -> Option<(String, String, SqlShardAvgFanoutPlan)> {
        let [Statement::Query(query)] = statements else {
            return None;
        };
        if query.with.is_some() || query.order_by.is_some() || query.limit_clause.is_some() {
            return None;
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        if select.distinct.is_some()
            || select.having.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || select.projection.len() != 1
        {
            return None;
        }
        let TableFactor::Table { name, .. } = &select.from[0].relation else {
            return None;
        };
        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by else {
            return None;
        };
        if !group_exprs.is_empty()
            || !select
                .selection
                .as_ref()
                .is_none_or(Self::select_predicate_is_fanout_local)
        {
            return None;
        }
        let (column_name, column_sql) =
            Self::select_projection_column_function_arg(&select.projection[0], &["AVG"])?;
        let output_column = Self::select_projection_output_name(&select.projection[0])?;
        let relation_sql = select.from[0].relation.to_string();
        let selection_sql = select
            .selection
            .as_ref()
            .map(|expr| format!(" WHERE {}", expr))
            .unwrap_or_default();
        let rewritten_sql = format!(
            "SELECT SUM({}), COUNT({}) FROM {}{}",
            column_sql, column_sql, relation_sql, selection_sql
        );
        Some((
            name.to_string(),
            column_name,
            SqlShardAvgFanoutPlan {
                rewritten_sql,
                output_column,
            },
        ))
    }

    fn distinct_aggregate_select_fanout_target(
        statements: &[Statement],
        function_name: &str,
    ) -> Option<(String, String, SqlShardDistinctAggregateFanoutPlan)> {
        let [Statement::Query(query)] = statements else {
            return None;
        };
        if query.with.is_some() || query.order_by.is_some() || query.limit_clause.is_some() {
            return None;
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        if select.distinct.is_some()
            || select.having.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || select.projection.len() != 1
        {
            return None;
        }
        let TableFactor::Table { name, .. } = &select.from[0].relation else {
            return None;
        };
        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by else {
            return None;
        };
        if !group_exprs.is_empty()
            || !select
                .selection
                .as_ref()
                .is_none_or(Self::select_predicate_is_fanout_local)
        {
            return None;
        }
        let (column_name, column_sql) = Self::select_projection_distinct_column_function_arg(
            &select.projection[0],
            std::slice::from_ref(&function_name),
        )?;
        let output_column = Self::select_projection_output_name(&select.projection[0])?;
        let relation_sql = select.from[0].relation.to_string();
        let selection_sql = select
            .selection
            .as_ref()
            .map(|expr| format!(" WHERE {}", expr))
            .unwrap_or_default();
        Some((
            name.to_string(),
            column_name,
            SqlShardDistinctAggregateFanoutPlan {
                rewritten_sql: format!(
                    "SELECT DISTINCT {} FROM {}{}",
                    column_sql, relation_sql, selection_sql
                ),
                output_column,
            },
        ))
    }

    fn min_max_select_fanout_target(
        statements: &[Statement],
    ) -> Option<(SqlShardExtremum, String, String)> {
        let [Statement::Query(query)] = statements else {
            return None;
        };
        if query.with.is_some() || query.order_by.is_some() || query.limit_clause.is_some() {
            return None;
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        if select.distinct.is_some()
            || select.having.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || select.projection.len() != 1
        {
            return None;
        }
        let TableFactor::Table { name, .. } = &select.from[0].relation else {
            return None;
        };
        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by else {
            return None;
        };
        if !group_exprs.is_empty() {
            return None;
        }
        if !select
            .selection
            .as_ref()
            .is_none_or(Self::select_predicate_is_fanout_local)
        {
            return None;
        }
        let column_name =
            Self::select_projection_is_column_function(&select.projection[0], &["MIN", "MAX"])?;
        let kind =
            Self::select_projection_function_name(&select.projection[0]).and_then(|name| {
                if name.eq_ignore_ascii_case("MIN") {
                    Some(SqlShardExtremum::Min)
                } else if name.eq_ignore_ascii_case("MAX") {
                    Some(SqlShardExtremum::Max)
                } else {
                    None
                }
            })?;
        Some((kind, name.to_string(), column_name))
    }

    fn select_projection_function_name(item: &SelectItem) -> Option<&str> {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return None,
        };
        let Expr::Function(func) = expr else {
            return None;
        };
        if func.name.0.len() != 1 {
            return None;
        }
        func.name.0[0].as_ident().map(|ident| ident.value.as_str())
    }

    fn select_projection_is_column_function(
        item: &SelectItem,
        function_names: &[&str],
    ) -> Option<String> {
        Self::select_projection_column_function_arg(item, function_names)
            .map(|(column_name, _)| column_name)
    }

    fn select_projection_column_function_arg(
        item: &SelectItem,
        function_names: &[&str],
    ) -> Option<(String, String)> {
        Self::select_projection_column_function_arg_with_duplicate_treatment(
            item,
            function_names,
            None,
        )
    }

    fn select_projection_distinct_column_function_arg(
        item: &SelectItem,
        function_names: &[&str],
    ) -> Option<(String, String)> {
        Self::select_projection_column_function_arg_with_duplicate_treatment(
            item,
            function_names,
            Some(DuplicateTreatment::Distinct),
        )
    }

    fn select_projection_column_function_arg_with_duplicate_treatment(
        item: &SelectItem,
        function_names: &[&str],
        duplicate_treatment: Option<DuplicateTreatment>,
    ) -> Option<(String, String)> {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return None,
        };
        let Expr::Function(func) = expr else {
            return None;
        };
        if func.name.0.len() != 1
            || !func.name.0[0].as_ident().is_some_and(|ident| {
                function_names
                    .iter()
                    .any(|name| ident.value.eq_ignore_ascii_case(name))
            })
        {
            return None;
        }
        let FunctionArguments::List(args) = &func.args else {
            return None;
        };
        if args.duplicate_treatment.as_ref() != duplicate_treatment.as_ref() || args.args.len() != 1
        {
            return None;
        }
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &args.args[0] else {
            return None;
        };
        match expr {
            Expr::Identifier(ident) => Some((ident.value.clone(), expr.to_string())),
            Expr::CompoundIdentifier(idents) => idents
                .last()
                .map(|ident| (ident.value.clone(), expr.to_string())),
            _ => None,
        }
    }

    fn select_projection_output_name(item: &SelectItem) -> Option<String> {
        match item {
            SelectItem::UnnamedExpr(expr) => Some(expr.to_string()),
            SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => None,
        }
    }

    fn select_projection_is_fanout_safe(item: &SelectItem) -> bool {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => true,
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
            }
        }
    }

    fn select_predicate_is_fanout_local(expr: &Expr) -> bool {
        match expr {
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Value(_)
            | Expr::TypedString { .. } => true,
            Expr::Nested(expr)
            | Expr::UnaryOp { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr) => Self::select_predicate_is_fanout_local(expr),
            Expr::BinaryOp { left, right, .. } => {
                Self::select_predicate_is_fanout_local(left)
                    && Self::select_predicate_is_fanout_local(right)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::select_predicate_is_fanout_local(expr)
                    && Self::select_predicate_is_fanout_local(low)
                    && Self::select_predicate_is_fanout_local(high)
            }
            Expr::InList { expr, list, .. } => {
                Self::select_predicate_is_fanout_local(expr)
                    && list.iter().all(Self::select_predicate_is_fanout_local)
            }
            _ => false,
        }
    }

    async fn shard_point_write_targets_for_statement(
        &self,
        statement: &Statement,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Vec<(&'static str, String, String)>> {
        match statement {
            Statement::Insert(insert) => self
                .shard_point_write_targets_for_insert(insert, txn, params)
                .await
                .map(|targets| {
                    targets
                        .into_iter()
                        .map(|(table, row_id)| ("INSERT", table, row_id))
                        .collect()
                }),
            Statement::Update(update) => self
                .shard_point_write_target_for_update(update, txn, params)
                .await
                .map(|target| {
                    target
                        .map(|(table, row_id)| vec![("UPDATE", table, row_id)])
                        .unwrap_or_default()
                }),
            Statement::Delete(delete) => self
                .shard_point_write_target_for_delete(delete, txn, params)
                .await
                .map(|target| {
                    target
                        .map(|(table, row_id)| vec![("DELETE", table, row_id)])
                        .unwrap_or_default()
                }),
            _ => Ok(Vec::new()),
        }
    }

    async fn shard_point_read_target_for_statement(
        &self,
        statement: &Statement,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Option<(String, String)>> {
        let Statement::Query(query) = statement else {
            return Ok(None);
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(None);
        };
        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return Ok(None);
        }
        let relation = &select.from[0].relation;
        let table_name = if let TableFactor::Table { name, .. } = relation {
            name.to_string()
        } else {
            return Ok(None);
        };

        let Some(schema) = self
            .load_table_schema_for_shard_routing(&table_name, txn)
            .await?
        else {
            return Ok(None);
        };
        let allowed_qualifiers = Self::primary_key_qualifiers(relation);
        Ok(self
            .primary_key_row_id_from_eq_selection(
                select.selection.as_ref(),
                &schema,
                params,
                &allowed_qualifiers,
            )
            .map(|row_id| (table_name, row_id)))
    }

    async fn shard_point_write_targets_for_insert(
        &self,
        insert: &sqlparser::ast::Insert,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Vec<(String, String)>> {
        let table_name = insert.table.to_string();
        let Some(schema) = self
            .load_table_schema_for_shard_routing(&table_name, txn)
            .await?
        else {
            return Ok(Vec::new());
        };
        let Some(pk_idx) = schema.get_primary_key_index() else {
            return Ok(Vec::new());
        };

        let composite_unique_indexes = self
            .load_composite_unique_indexes_for_table(&table_name, txn)
            .await?;
        if composite_unique_indexes
            .iter()
            .any(|index| index.name.ends_with("_pkey"))
        {
            return Ok(Vec::new());
        }

        let Some(query) = insert.source.as_ref() else {
            return Ok(Vec::new());
        };
        let SetExpr::Values(values) = query.body.as_ref() else {
            return Ok(Vec::new());
        };

        let Some(pk_expr_idx) = Self::insert_primary_key_expr_index(insert, &schema, pk_idx) else {
            return Ok(Vec::new());
        };

        let mut targets = Vec::with_capacity(values.rows.len());
        for row in &values.rows {
            if !Self::insert_values_row_shape_can_be_routed(insert, &schema, row) {
                return Ok(Vec::new());
            }
            let Some(value_expr) = row.get(pk_expr_idx) else {
                return Ok(Vec::new());
            };
            if self.expr_has_column_reference(value_expr) {
                return Ok(Vec::new());
            }
            let value = self.evaluate_value(value_expr, &[], &schema, params)?;
            let value =
                Self::coerce_value_to_column_type(value, &schema.columns[pk_idx].data_type)?;
            let Some(row_id) = Self::value_to_primary_row_id(&value) else {
                return Ok(Vec::new());
            };
            targets.push((table_name.clone(), row_id));
        }

        Ok(targets)
    }

    fn insert_primary_key_expr_index(
        insert: &sqlparser::ast::Insert,
        schema: &TableSchema,
        pk_idx: usize,
    ) -> Option<usize> {
        if insert.columns.is_empty() {
            return Some(pk_idx);
        }

        let pk_column = &schema.columns[pk_idx].name;
        insert
            .columns
            .iter()
            .position(|column| column.value.eq_ignore_ascii_case(pk_column))
    }

    fn insert_values_row_shape_can_be_routed(
        insert: &sqlparser::ast::Insert,
        schema: &TableSchema,
        row: &[Expr],
    ) -> bool {
        if insert.columns.is_empty() {
            row.len() == schema.columns.len()
        } else {
            row.len() == insert.columns.len()
        }
    }

    async fn shard_point_write_target_for_update(
        &self,
        update: &sqlparser::ast::Update,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Option<(String, String)>> {
        let sqlparser::ast::TableWithJoins { relation, .. } = &update.table;
        let table_name = if let TableFactor::Table { name, .. } = relation {
            name.to_string()
        } else {
            return Ok(None);
        };

        let Some(schema) = self
            .load_table_schema_for_shard_routing(&table_name, txn)
            .await?
        else {
            return Ok(None);
        };
        let allowed_qualifiers = Self::primary_key_qualifiers(relation);
        Ok(self
            .primary_key_row_id_from_eq_selection(
                update.selection.as_ref(),
                &schema,
                params,
                &allowed_qualifiers,
            )
            .map(|row_id| (table_name, row_id)))
    }

    async fn shard_point_write_target_for_delete(
        &self,
        delete: &sqlparser::ast::Delete,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Option<(String, String)>> {
        let Some(relation) = Self::delete_target_relation(delete) else {
            return Ok(None);
        };
        let table_name = if let TableFactor::Table { name, .. } = relation {
            name.to_string()
        } else {
            return Ok(None);
        };

        let Some(schema) = self
            .load_table_schema_for_shard_routing(&table_name, txn)
            .await?
        else {
            return Ok(None);
        };
        let allowed_qualifiers = Self::primary_key_qualifiers(relation);
        Ok(self
            .primary_key_row_id_from_eq_selection(
                delete.selection.as_ref(),
                &schema,
                params,
                &allowed_qualifiers,
            )
            .map(|row_id| (table_name, row_id)))
    }

    fn delete_target_relation(delete: &sqlparser::ast::Delete) -> Option<&TableFactor> {
        match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables)
            | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                tables.first().map(|table| &table.relation)
            }
        }
    }

    async fn load_table_schema_for_shard_routing(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Option<TableSchema>> {
        let schema_key = format!("schema:{}", table_name);
        let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? else {
            return Ok(None);
        };
        bincode::deserialize(&schema_bytes)
            .map(Some)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))
    }

    fn shard_routing_decision_for_row_id(
        router: &ShardRouter,
        operation: &'static str,
        table_name: &str,
        row_id: String,
    ) -> SqlShardRoutingDecision {
        let route = router.route_key(table_name, &row_id);
        SqlShardRoutingDecision {
            operation: operation.to_string(),
            route,
            local_node_id: router.local_node_id(),
        }
    }

    pub(crate) fn invalidate_query_result_cache(&self) {
        self.query_result_epoch.fetch_add(1, AtomicOrdering::AcqRel);
        self.query_result_cache.invalidate_all();
        crate::monitor::inc_query_result_cache_invalidation();
    }

    pub(crate) fn invalidate_storage_caches(&self) {
        self.invalidate_query_result_cache();
        self.invalidate_update_fast_path_cache();
        self.row_cache.invalidate_all();
        // Covers both existing callers: Raft apply and snapshot install. The
        // fence re-reads the durable phase record on the next observation.
        if let Some(fence) = &self.data_migration_fence {
            fence.invalidate();
        }
    }

    fn invalidate_update_fast_path_cache(&self) {
        self.simple_pk_update_fast_path_cache.invalidate_all();
    }

    fn current_query_result_epoch(&self) -> u64 {
        self.query_result_epoch.load(AtomicOrdering::Acquire)
    }

    fn query_result_cache_key(sql: &str) -> String {
        sql.trim().trim_end_matches(';').trim().to_string()
    }

    pub(crate) fn statement_may_change_query_results(stmt: &Statement) -> bool {
        match stmt {
            Statement::Insert(_) | Statement::Delete(_) | Statement::Update(_) => true,
            Statement::CreateTable(_)
            | Statement::CreateIndex(_)
            | Statement::AlterTable(_)
            | Statement::Truncate(_)
            | Statement::CreateView(_)
            | Statement::Analyze(_)
            | Statement::Drop { .. } => true,
            Statement::Copy { to, .. } => !*to,
            // Migration procedures mutate the durable phase record; other
            // CALL names keep today's non-mutating classification and fail
            // loudly at execution.
            Statement::Call(function) => {
                Self::is_data_migration_call(function)
                    && !Self::is_data_backfill_status_call(function)
            }
            Statement::Explain { statement, .. } => {
                Self::statement_may_change_query_results(statement)
            }
            _ => false,
        }
    }

    fn statement_may_change_update_fast_path_metadata(stmt: &Statement) -> bool {
        match stmt {
            Statement::CreateTable(_)
            | Statement::CreateIndex(_)
            | Statement::AlterTable(_)
            | Statement::Truncate(_)
            | Statement::Drop { .. } => true,
            Statement::Explain { statement, .. } => {
                Self::statement_may_change_update_fast_path_metadata(statement)
            }
            _ => false,
        }
    }

    /// True when `stmt` is a grouped-aggregate SELECT eligible for the query-result cache (the same
    /// predicate `execute_sql` uses on the HTTP path). Exposed so the pgwire path can opt the same
    /// statements into the cache (BENCHPROD-458).
    pub(crate) fn is_query_result_cacheable_statement(stmt: &Statement) -> bool {
        Self::is_cacheable_group_aggregate_statement(stmt)
            || Self::is_cacheable_join_group_aggregate_statement(stmt)
    }

    /// Execute a grouped-aggregate SELECT through the shared query-result cache, mirroring the cache
    /// path in `execute_sql` so pgwire autocommit queries get the same cache hits as HTTP `/query`.
    /// Caller must ensure the statement is cacheable (see `is_query_result_cacheable_statement`),
    /// runs outside an explicit transaction, and has no bind params. Entries carry the current epoch,
    /// which any write bumps via `invalidate_query_result_cache`, so cached reads never go stale.
    pub(crate) async fn execute_cached_select(&self, stmt: &Statement) -> Result<QueryResult> {
        let sql = stmt.to_string();
        let cache_key = Self::query_result_cache_key(&sql);
        let epoch = self.current_query_result_epoch();
        let start = std::time::Instant::now();
        crate::monitor::inc_query_result_cache_eligible();
        if let Some(cached) = self.query_result_cache.get(&cache_key) {
            if cached.epoch == epoch {
                crate::monitor::inc_query_result_cache_hit();
                crate::monitor::record_query(&sql, start.elapsed());
                return Ok(QueryResult::Select {
                    columns: cached.columns,
                    rows: cached.rows,
                });
            }
            crate::monitor::inc_query_result_cache_stale();
        }
        crate::monitor::inc_query_result_cache_miss();
        let result = self.execute(stmt).await?;
        if let QueryResult::Select { columns, rows } = &result {
            self.query_result_cache.insert(
                cache_key,
                CachedSelectResult {
                    epoch,
                    columns: columns.clone(),
                    rows: rows.clone(),
                },
            );
            crate::monitor::inc_query_result_cache_insert();
        }
        Ok(result)
    }

    fn is_cacheable_group_aggregate_statement(stmt: &Statement) -> bool {
        let Statement::Query(query) = stmt else {
            return false;
        };

        let SetExpr::Select(select) = query.body.as_ref() else {
            return false;
        };

        if select.distinct.is_some()
            || select.having.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
        {
            return false;
        }

        if !matches!(select.from[0].relation, TableFactor::Table { .. }) {
            return false;
        }

        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by else {
            return false;
        };
        if group_exprs.is_empty() {
            return false;
        }
        if !group_exprs.iter().all(Self::is_cacheable_column_expr) {
            return false;
        }

        if let Some(selection) = &select.selection {
            if !Self::is_cacheable_predicate_expr(selection) {
                return false;
            }
        }

        if let Some(order_by) = &query.order_by {
            let OrderByKind::Expressions(exprs) = &order_by.kind else {
                return false;
            };
            if !exprs
                .iter()
                .all(|expr| Self::is_cacheable_order_expr(&expr.expr))
            {
                return false;
            }
        }

        if select.projection.len() <= group_exprs.len() {
            return false;
        }

        for (item, group_expr) in select
            .projection
            .iter()
            .take(group_exprs.len())
            .zip(group_exprs)
        {
            if !Self::projection_matches_group_expr(item, group_expr) {
                return false;
            }
        }

        select
            .projection
            .iter()
            .skip(group_exprs.len())
            .all(Self::is_cacheable_aggregate_projection)
    }

    fn is_cacheable_join_group_aggregate_statement(stmt: &Statement) -> bool {
        let Statement::Query(query) = stmt else {
            return false;
        };

        let SetExpr::Select(select) = query.body.as_ref() else {
            return false;
        };

        if select.distinct.is_some()
            || select.having.is_some()
            || select.selection.is_some()
            || select.from.len() != 1
            || select.from[0].joins.len() != 1
        {
            return false;
        }

        if !matches!(select.from[0].relation, TableFactor::Table { .. }) {
            return false;
        }

        let join = &select.from[0].joins[0];
        if !matches!(join.relation, TableFactor::Table { .. }) {
            return false;
        }
        // The ON predicate must be deterministic (columns/literals/comparisons/AND only). A volatile
        // function in ON (e.g. `... AND e.ts > NOW()`) would otherwise be cached and frozen, since the
        // result cache only invalidates on writes, not on wall-clock time.
        let on_expr = match &join.join_operator {
            sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::Join(sqlparser::ast::JoinConstraint::On(expr)) => expr,
            _ => return false,
        };
        if !Self::is_cacheable_join_on_expr(on_expr) {
            return false;
        }

        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by else {
            return false;
        };
        if group_exprs.is_empty() || !group_exprs.iter().all(Self::is_cacheable_column_expr) {
            return false;
        }

        if let Some(order_by) = &query.order_by {
            let OrderByKind::Expressions(exprs) = &order_by.kind else {
                return false;
            };
            if !exprs
                .iter()
                .all(|expr| Self::is_cacheable_order_expr(&expr.expr))
            {
                return false;
            }
        }

        if select.projection.len() <= group_exprs.len() {
            return false;
        }

        for (item, group_expr) in select
            .projection
            .iter()
            .take(group_exprs.len())
            .zip(group_exprs)
        {
            if !Self::projection_matches_group_expr(item, group_expr) {
                return false;
            }
        }

        select
            .projection
            .iter()
            .skip(group_exprs.len())
            .all(Self::is_cacheable_aggregate_projection)
    }

    fn projection_matches_group_expr(item: &SelectItem, group_expr: &Expr) -> bool {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                expr == group_expr && Self::is_cacheable_column_expr(expr)
            }
            _ => false,
        }
    }

    fn is_cacheable_aggregate_projection(item: &SelectItem) -> bool {
        match item {
            SelectItem::UnnamedExpr(Expr::Function(func))
            | SelectItem::ExprWithAlias {
                expr: Expr::Function(func),
                ..
            } => Self::is_cacheable_aggregate_function(func),
            _ => false,
        }
    }

    fn is_cacheable_order_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Nested(inner) => Self::is_cacheable_order_expr(inner),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => true,
            Expr::Function(func) => Self::is_cacheable_aggregate_function(func),
            _ => false,
        }
    }

    fn is_cacheable_column_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Nested(inner) => Self::is_cacheable_column_expr(inner),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => true,
            _ => false,
        }
    }

    fn is_cacheable_literal_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Nested(inner) => Self::is_cacheable_literal_expr(inner),
            Expr::Value(value) => !matches!(
                value.value,
                sqlparser::ast::Value::Placeholder(_)
                    | sqlparser::ast::Value::SingleQuotedByteStringLiteral(_)
                    | sqlparser::ast::Value::DoubleQuotedByteStringLiteral(_)
            ),
            _ => false,
        }
    }

    /// A JOIN `ON` predicate safe to admit into the result cache: only AND-combined comparisons whose
    /// operands are column references or literals (so `a.id = b.id`, `a.x > 5` are fine). Rejects any
    /// function call (e.g. `NOW()`, `CURRENT_DATE`) and anything else, since a volatile predicate would
    /// be cached and frozen.
    fn is_cacheable_join_on_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Nested(inner) => Self::is_cacheable_join_on_expr(inner),
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
                Self::is_cacheable_join_on_expr(left) && Self::is_cacheable_join_on_expr(right)
            }
            Expr::BinaryOp { left, op, right }
                if matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Gt
                        | BinaryOperator::Lt
                        | BinaryOperator::GtEq
                        | BinaryOperator::LtEq
                ) =>
            {
                Self::is_cacheable_join_operand(left) && Self::is_cacheable_join_operand(right)
            }
            _ => false,
        }
    }

    fn is_cacheable_join_operand(expr: &Expr) -> bool {
        Self::is_cacheable_column_expr(expr) || Self::is_cacheable_literal_expr(expr)
    }

    fn is_cacheable_predicate_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Nested(inner) => Self::is_cacheable_predicate_expr(inner),
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
                Self::is_cacheable_predicate_expr(left) && Self::is_cacheable_predicate_expr(right)
            }
            Expr::BinaryOp { left, op, right }
                if matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Gt
                        | BinaryOperator::Lt
                        | BinaryOperator::GtEq
                        | BinaryOperator::LtEq
                ) =>
            {
                (Self::is_cacheable_column_expr(left) && Self::is_cacheable_literal_expr(right))
                    || (Self::is_cacheable_literal_expr(left)
                        && Self::is_cacheable_column_expr(right))
            }
            _ => false,
        }
    }

    fn is_cacheable_aggregate_function(func: &sqlparser::ast::Function) -> bool {
        let Some(function) = cacheable_aggregate_function_kind(&func.name) else {
            return false;
        };
        let FunctionArguments::List(args) = &func.args else {
            return false;
        };
        if args.args.len() != 1 {
            return false;
        }

        match function {
            CacheableAggregateFunction::Count => {
                matches!(
                    args.args[0],
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                ) || matches!(
                    &args.args[0],
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                        if Self::is_cacheable_column_expr(expr)
                )
            }
            CacheableAggregateFunction::Value => {
                args.duplicate_treatment.is_none()
                    && matches!(
                        &args.args[0],
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                            if Self::is_cacheable_column_expr(expr)
                    )
            }
        }
    }

    pub fn prepare(&self, sql: &str) -> Result<Vec<Statement>> {
        // Custom statements not supported by sqlparser return empty vec;
        // they are handled by execute_sql() instead.
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_uppercase();
        if upper == "SHOW VIEWS"
            || upper == "SHOW INDEXES"
            || upper.starts_with("SHOW INDEXES FROM ")
            || upper == "SHOW ALL"
            || upper == "SHOW USERS"
            || upper.starts_with("CREATE USER ")
            || upper.starts_with("DROP USER ")
            || upper.starts_with("GRANT ")
            || upper.starts_with("REVOKE ")
        {
            return Ok(vec![]);
        }

        if let Some(stmts) = self.statement_cache.get(sql) {
            return Ok(stmts.clone());
        }

        monitor::inc_parse();
        let stmts = parse_sql(sql)?;
        self.statement_cache.insert(sql.to_string(), stmts.clone());

        Ok(stmts)
    }

    pub fn sql_requires_raft_write(&self, sql: &str) -> Result<bool> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_uppercase();

        if upper == "SHOW VIEWS"
            || upper == "SHOW INDEXES"
            || upper.starts_with("SHOW INDEXES FROM ")
            || upper == "SHOW ALL"
            || upper == "SHOW USERS"
        {
            return Ok(false);
        }

        if upper.starts_with("CREATE USER ")
            || upper.starts_with("DROP USER ")
            || upper.starts_with("GRANT ")
            || upper.starts_with("REVOKE ")
            || upper.starts_with("VACUUM")
        {
            return Ok(true);
        }

        let statements = self.prepare(sql)?;
        Ok(statements
            .iter()
            .any(Self::statement_may_change_query_results))
    }

    pub fn register_prepared_statement(
        &self,
        sql: &str,
        owner: Option<&str>,
    ) -> Result<PreparedStatementRecord> {
        let statements = Arc::new(self.prepare(sql)?);
        let id = uuid::Uuid::new_v4().to_string();
        let record = PreparedStatementRecord {
            id: id.clone(),
            sql: sql.to_string(),
            statements,
            owner: Self::normalize_prepared_owner(owner),
            created_at_epoch_ms: Self::current_epoch_ms(),
        };

        let mut store = self.prepared_statements.write();
        store.order.push_back(id.clone());
        store.entries.insert(id.clone(), record.clone());

        while store.entries.len() > Self::MAX_PREPARED_STATEMENTS {
            if let Some(oldest_id) = store.order.pop_front() {
                if oldest_id != id {
                    store.entries.remove(&oldest_id);
                }
            } else {
                break;
            }
        }

        Ok(record)
    }

    pub fn get_prepared_statement(&self, id: &str) -> Result<PreparedStatementRecord> {
        let store = self.prepared_statements.read();
        if let Some(record) = store.entries.get(id) {
            monitor::inc_plan();
            Ok(record.clone())
        } else {
            Err(FusionError::Execution(format!(
                "Prepared statement {} not found",
                id
            )))
        }
    }

    pub fn get_prepared_statement_for_owner(
        &self,
        id: &str,
        requester: Option<&str>,
    ) -> Result<PreparedStatementRecord> {
        let record = self.get_prepared_statement(id)?;
        if let Some(owner) = &record.owner {
            let requester = Self::normalize_prepared_owner(requester).unwrap_or_default();
            if requester != *owner {
                return Err(FusionError::Execution(format!(
                    "Prepared statement {} belongs to '{}'",
                    id, owner
                )));
            }
        }
        Ok(record)
    }

    pub fn remove_prepared_statement(
        &self,
        id: &str,
        requester: Option<&str>,
    ) -> Result<PreparedStatementRecord> {
        let mut store = self.prepared_statements.write();
        let Some(record) = store.entries.get(id).cloned() else {
            return Err(FusionError::Execution(format!(
                "Prepared statement {} not found",
                id
            )));
        };

        if let Some(owner) = &record.owner {
            let requester = Self::normalize_prepared_owner(requester).unwrap_or_default();
            if requester != *owner {
                return Err(FusionError::Execution(format!(
                    "Prepared statement {} belongs to '{}'",
                    id, owner
                )));
            }
        }

        store.entries.remove(id);
        if let Some(position) = store.order.iter().position(|existing| existing == id) {
            store.order.remove(position);
        }

        Ok(record)
    }

    pub fn list_prepared_statements(&self, owner: Option<&str>) -> Vec<PreparedStatementRecord> {
        let normalized_owner = Self::normalize_prepared_owner(owner);
        let store = self.prepared_statements.read();
        store
            .order
            .iter()
            .filter_map(|id| store.entries.get(id))
            .filter(|record| record.owner.as_deref() == normalized_owner.as_deref())
            .cloned()
            .collect()
    }

    pub async fn execute_in_transaction(
        &self,
        stmt: &Statement,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        self.execute_in_transaction_with_params(stmt, txn, &[])
            .await
    }

    pub async fn execute_in_transaction_with_params(
        &self,
        stmt: &Statement,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<QueryResult> {
        if Self::statement_may_change_update_fast_path_metadata(stmt) {
            self.invalidate_update_fast_path_cache();
        }

        match stmt {
            Statement::CreateTable(create_table) => {
                self.handle_create_table(
                    &create_table.name,
                    &create_table.columns,
                    &create_table.constraints,
                    create_table.if_not_exists,
                    txn,
                )
                .await
            }
            Statement::Insert(insert) => {
                self.handle_insert(
                    insert.table.to_string(),
                    &insert.columns,
                    &insert.source,
                    &insert.returning,
                    &insert.on,
                    txn,
                    params,
                )
                .await
            }
            Statement::Query(query) => self.handle_query(query, txn, params).await,
            Statement::CreateIndex(create_index) => {
                self.handle_create_index(
                    &create_index.name,
                    &create_index.table_name,
                    &create_index.columns,
                    &create_index.include,
                    create_index.unique,
                    create_index.nulls_distinct,
                    &create_index.index_options,
                    txn,
                )
                .await
            }
            Statement::Delete(delete) => self.handle_delete(delete, txn, params).await,
            Statement::Update(update) => self.handle_update(update, txn, params).await,
            Statement::Explain {
                statement, analyze, ..
            } => self.handle_explain(statement, *analyze, txn, params).await,
            Statement::Vacuum(vacuum) => self.handle_vacuum(vacuum).await,
            Statement::Analyze(analyze) => self.handle_analyze(analyze, txn).await,
            Statement::Copy {
                source,
                to,
                target,
                options,
                legacy_options,
                values,
            } => {
                self.handle_copy(source, *to, target, options, legacy_options, values, txn)
                    .await
            }
            Statement::Drop {
                names,
                object_type: sqlparser::ast::ObjectType::View,
                if_exists,
                ..
            } => self.handle_drop_view(names, *if_exists, txn).await,
            Statement::Drop {
                names,
                object_type: sqlparser::ast::ObjectType::Index,
                if_exists,
                ..
            } => self.handle_drop_index(names, *if_exists, txn).await,
            Statement::Drop {
                names,
                if_exists,
                object_type,
                ..
            } => {
                self.handle_drop_table(names, *if_exists, *object_type, txn)
                    .await
            }
            Statement::ShowTables { .. } => self.handle_show_tables(txn).await,
            Statement::ShowCreate { obj_type, obj_name } => {
                if matches!(obj_type, sqlparser::ast::ShowCreateObject::Table) {
                    self.handle_show_create_table(obj_name, txn).await
                } else {
                    Err(crate::common::FusionError::Execution(format!(
                        "SHOW CREATE {} not supported",
                        obj_type
                    )))
                }
            }
            Statement::ExplainTable { table_name, .. } => {
                self.handle_describe_table(table_name, txn).await
            }
            Statement::AlterTable(alter_table) => {
                self.handle_alter_table(&alter_table.name, &alter_table.operations, txn)
                    .await
            }
            Statement::Truncate(truncate) => self.handle_truncate(&truncate.table_names, txn).await,
            Statement::CreateView(cv) => {
                self.handle_create_view(&cv.name, &cv.query, cv.or_replace, txn)
                    .await
            }
            Statement::Call(function) => self.handle_call(function, txn).await,
            stmt if Self::is_show_data_migration_phase(stmt) => {
                self.handle_show_data_migration_phase(txn).await
            }
            _ => Err(FusionError::Execution(format!(
                "Unsupported SQL statement: {stmt}"
            ))),
        }
    }

    /// True for the two Data V2 migration procedures. Only these CALL names
    /// mutate state; they must route through the Raft write path in
    /// distributed mode exactly like DML, and require superuser.
    pub(crate) fn is_data_migration_call(function: &Function) -> bool {
        execution_object_name_eq_ascii(&function.name, "fusiondb_data_migration_init")
            || execution_object_name_eq_ascii(&function.name, "fusiondb_data_migration_advance")
            || execution_object_name_eq_ascii(&function.name, "fusiondb_data_backfill_step")
            || Self::is_data_backfill_status_call(function)
    }

    pub(crate) fn is_data_backfill_step_call(function: &Function) -> bool {
        execution_object_name_eq_ascii(&function.name, "fusiondb_data_backfill_step")
    }

    /// Read-only status is not a mutating procedure: it must not route to the
    /// Raft write path, but it is still operator-only.
    pub(crate) fn is_data_backfill_status_call(function: &Function) -> bool {
        execution_object_name_eq_ascii(&function.name, "fusiondb_data_backfill_status")
    }

    pub(crate) fn statement_is_data_migration_call(stmt: &Statement) -> bool {
        matches!(stmt, Statement::Call(function) if Self::is_data_migration_call(function))
    }

    fn call_string_arguments(function: &Function) -> Result<Vec<String>> {
        match &function.args {
            FunctionArguments::None => Ok(vec![]),
            FunctionArguments::List(list) => list
                .args
                .iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) => {
                        match &value.value {
                            sqlparser::ast::Value::SingleQuotedString(text) => Ok(text.clone()),
                            other => Err(FusionError::Execution(format!(
                                "CALL arguments must be string literals, got {other}"
                            ))),
                        }
                    }
                    other => Err(FusionError::Execution(format!(
                        "CALL arguments must be string literals, got {other}"
                    ))),
                })
                .collect(),
            FunctionArguments::Subquery(_) => Err(FusionError::Execution(
                "CALL does not accept a subquery argument".to_string(),
            )),
        }
    }

    async fn handle_call(
        &self,
        function: &Function,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let args = Self::call_string_arguments(function)?;
        if execution_object_name_eq_ascii(&function.name, "fusiondb_data_migration_init") {
            if !args.is_empty() {
                return Err(FusionError::Execution(
                    "CALL fusiondb_data_migration_init() takes no arguments".to_string(),
                ));
            }
            return self.handle_data_migration_init(txn).await;
        }
        if execution_object_name_eq_ascii(&function.name, "fusiondb_data_backfill_step") {
            if !args.is_empty() {
                return Err(FusionError::Execution(
                    "CALL fusiondb_data_backfill_step() takes no arguments".to_string(),
                ));
            }
            return self.run_backfill_chunk(txn).await;
        }
        if execution_object_name_eq_ascii(&function.name, "fusiondb_data_backfill_status") {
            if !args.is_empty() {
                return Err(FusionError::Execution(
                    "CALL fusiondb_data_backfill_status() takes no arguments".to_string(),
                ));
            }
            return self.handle_backfill_status(txn).await;
        }
        if execution_object_name_eq_ascii(&function.name, "fusiondb_data_migration_advance") {
            let [target] = args.as_slice() else {
                return Err(FusionError::Execution(
                    "CALL fusiondb_data_migration_advance('<phase>') takes exactly one phase-name argument"
                        .to_string(),
                ));
            };
            return self.handle_data_migration_advance(target, txn).await;
        }
        Err(FusionError::Execution(format!(
            "Unsupported SQL statement: CALL {}",
            function.name
        )))
    }

    fn data_migration_phase_result(record: &DataMigrationPhaseRecord) -> QueryResult {
        QueryResult::Select {
            columns: vec![
                "phase".to_string(),
                "phase_seq".to_string(),
                "updated_at_unix_ms".to_string(),
            ],
            rows: vec![vec![
                Value::String(record.phase.name().to_string()),
                Value::Integer(record.phase_seq as i64),
                Value::Integer(record.updated_at_unix_ms as i64),
            ]],
        }
    }

    /// `CALL fusiondb_data_migration_init()`: create the durable phase record
    /// from the config-flag default. Idempotent — an existing record is
    /// returned unchanged with zero writes, so operator/crash retries are
    /// safe. A record is never created implicitly: initialization is an
    /// explicit operator decision.
    async fn handle_data_migration_init(&self, txn: &mut dyn Transaction) -> Result<QueryResult> {
        if let Some(raw) = txn.get(migration_phase_key()).await? {
            let record = DataMigrationPhaseRecord::decode(&raw)?;
            return Ok(Self::data_migration_phase_result(&record));
        }
        let record = DataMigrationPhaseRecord {
            phase: self.config_default_migration_phase(),
            phase_seq: 1,
            updated_at_unix_ms: u64::try_from(Self::current_epoch_ms()).unwrap_or(u64::MAX),
        };
        txn.put(migration_phase_key(), &record.encode()).await?;
        Ok(Self::data_migration_phase_result(&record))
    }

    /// `CALL fusiondb_data_migration_advance('<phase>')`: move the record
    /// exactly one phase up the ladder. Re-targeting the current phase is an
    /// idempotent no-op (retry safety); anything else — downgrade, skip,
    /// unknown name, or a target beyond this binary's advance gate — fails
    /// loudly with zero writes.
    async fn handle_data_migration_advance(
        &self,
        target_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let Some(target) = DataMigrationPhase::parse_name(target_name) else {
            return Err(FusionError::Execution(format!(
                "unknown Data V2 migration phase '{target_name}' (valid: delete-only, write-delete-shadow, backfill, validated, v2-readable, v2-only, legacy-gc)"
            )));
        };
        let Some(raw) = txn.get(migration_phase_key()).await? else {
            return Err(FusionError::Execution(
                "no Data V2 migration phase record exists; run CALL fusiondb_data_migration_init() first"
                    .to_string(),
            ));
        };
        let current = DataMigrationPhaseRecord::decode(&raw)?;
        if target == current.phase {
            return Ok(Self::data_migration_phase_result(&current));
        }
        if Some(target) != current.phase.next() {
            return Err(FusionError::Execution(format!(
                "Data V2 migration phase can only advance one step: current '{}', next '{}', requested '{}'",
                current.phase.name(),
                current
                    .phase
                    .next()
                    .map(DataMigrationPhase::name)
                    .unwrap_or("<none: ladder complete>"),
                target.name()
            )));
        }
        if target > MAX_ADVANCE_TARGET_PHASE {
            return Err(FusionError::Execution(format!(
                "Data V2 migration phase '{}' is not supported by this build (advance gate: '{}')",
                target.name(),
                MAX_ADVANCE_TARGET_PHASE.name()
            )));
        }
        let record = DataMigrationPhaseRecord {
            phase: target,
            phase_seq: current.phase_seq + 1,
            updated_at_unix_ms: u64::try_from(Self::current_epoch_ms()).unwrap_or(u64::MAX),
        };
        txn.put(migration_phase_key(), &record.encode()).await?;
        Ok(Self::data_migration_phase_result(&record))
    }

    /// True for `SHOW DATA MIGRATION PHASE` in its parsed form. pgwire parses
    /// the text before `execute_sql` can intercept it, so the diagnostic must
    /// also be reachable as a statement.
    fn is_show_data_migration_phase(stmt: &Statement) -> bool {
        let Statement::ShowVariable { variable } = stmt else {
            return false;
        };
        let parts: Vec<String> = variable
            .iter()
            .map(|ident| ident.value.to_ascii_uppercase())
            .collect();
        parts == ["DATA", "MIGRATION", "PHASE"]
    }

    /// `SHOW DATA MIGRATION PHASE`: local read-only diagnostic.
    async fn handle_show_data_migration_phase(
        &self,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        match txn.get(migration_phase_key()).await? {
            Some(raw) => {
                let record = DataMigrationPhaseRecord::decode(&raw)?;
                Ok(Self::data_migration_phase_result(&record))
            }
            None => Ok(QueryResult::Select {
                columns: vec![
                    "phase".to_string(),
                    "phase_seq".to_string(),
                    "updated_at_unix_ms".to_string(),
                ],
                rows: vec![vec![
                    Value::String(format!(
                        "no record (config-derived: {})",
                        self.config_default_migration_phase().name()
                    )),
                    Value::Null,
                    Value::Null,
                ]],
            }),
        }
    }

    // ---- Data V2 backfill engine (P10-2.3) ----

    /// Rows copied per chunk. Every key in a chunk costs one
    /// `latest_committed_timestamp` probe under the global commit lock, so a
    /// larger chunk buys throughput by stalling every other commit for longer.
    const BACKFILL_CHUNK_ROWS: usize = 256;
    /// Byte ceiling for one chunk's copied values, so a table of large rows
    /// cannot turn a chunk into an unbounded write buffer.
    const BACKFILL_CHUNK_BYTES: usize = 1 << 20;

    /// The legacy base-row key ranges, in byte order: unsharded rows first
    /// (`data:`), then every shard route (`shard:{N}:data:`). Walking the
    /// physical keyspace — rather than asking the router — is what makes the
    /// backfill see historical shard ids and pre-router unsharded rows, which
    /// `routed_data_prefixes_for_table` would silently skip.
    fn legacy_backfill_ranges() -> [(Vec<u8>, Vec<u8>); 2] {
        [
            (b"data:".to_vec(), b"data;".to_vec()),
            (b"shard:".to_vec(), b"shard;".to_vec()),
        ]
    }

    /// Split a legacy base-row key into `(table, row_id)` using the set of
    /// tables that actually exist.
    ///
    /// Both table names and row ids may contain `:`, so the split is resolved
    /// against known table names first. When several known tables match (say
    /// `orders` and `orders:archive`), the row's own primary key breaks the
    /// tie. `Ok(None)` means the key belongs to no known table — an orphan
    /// that must not be copied into a table's shadow set. A genuinely
    /// ambiguous key is a loud error rather than a guess.
    fn split_legacy_backfill_suffix(
        suffix: &str,
        value: &[u8],
        schemas: &HashMap<String, TableSchema>,
    ) -> Result<Option<(String, String)>> {
        let mut candidates: Vec<(&str, &str)> = Vec::new();
        for (index, _) in suffix.match_indices(':') {
            let (table, row_id) = (&suffix[..index], &suffix[index + 1..]);
            if table.is_empty() || row_id.is_empty() {
                continue;
            }
            if schemas.contains_key(table) {
                candidates.push((table, row_id));
            }
        }
        match candidates.as_slice() {
            [] => Ok(None),
            [(table, row_id)] => Ok(Some((table.to_string(), row_id.to_string()))),
            many => {
                for (table, row_id) in many {
                    let Some(schema) = schemas.get(*table) else {
                        continue;
                    };
                    let Some(pk_index) = schema.get_primary_key_index() else {
                        continue;
                    };
                    if let Ok(Some(pk_value)) =
                        crate::common::encoding::RowDecoder::decode_column(value, pk_index)
                    {
                        if Self::value_to_primary_row_id(&pk_value).as_deref() == Some(*row_id) {
                            return Ok(Some((table.to_string(), row_id.to_string())));
                        }
                    }
                }
                Err(FusionError::Execution(format!(
                    "Data V2 backfill cannot resolve the table/row split of legacy key suffix '{suffix}': it matches several tables and no primary key confirms one"
                )))
            }
        }
    }

    /// Classify a key found in the legacy ranges. `Ok(None)` means the key is
    /// not a base row of a known table (a sharded index key, or an orphan) and
    /// is skipped.
    fn legacy_backfill_row_identity(
        key: &[u8],
        value: &[u8],
        schemas: &HashMap<String, TableSchema>,
    ) -> Result<Option<(String, String)>> {
        let Ok(text) = std::str::from_utf8(key) else {
            return Ok(None);
        };
        let suffix = if let Some(rest) = text.strip_prefix("data:") {
            rest
        } else if let Some(rest) = text.strip_prefix("shard:") {
            // shard:{N}:data:{table}:{row_id} — anything else in the shard
            // region belongs to another key family.
            let Some((shard, tail)) = rest.split_once(':') else {
                return Ok(None);
            };
            if shard.is_empty() || !shard.bytes().all(|byte| byte.is_ascii_digit()) {
                return Ok(None);
            }
            let Some(tail) = tail.strip_prefix("data:") else {
                return Ok(None);
            };
            tail
        } else {
            return Ok(None);
        };
        if suffix.is_empty() {
            return Ok(None);
        }
        Self::split_legacy_backfill_suffix(suffix, value, schemas)
    }

    /// Every table schema, loaded once per chunk so the scan visitor (which is
    /// synchronous) can resolve table names without further storage reads.
    async fn load_all_table_schemas(
        txn: &mut dyn Transaction,
    ) -> Result<HashMap<String, TableSchema>> {
        let mut schemas = HashMap::new();
        for (key, value) in txn.scan_prefix(b"schema:", None).await? {
            let Some(name) = key
                .strip_prefix(b"schema:".as_slice())
                .and_then(|name| std::str::from_utf8(name).ok())
            else {
                continue;
            };
            if let Ok(schema) = bincode::deserialize::<TableSchema>(&value) {
                schemas.insert(name.to_string(), schema);
            }
        }
        Ok(schemas)
    }

    async fn read_backfill_state(txn: &mut dyn Transaction) -> Result<Option<DataBackfillState>> {
        txn.get(backfill_state_key())
            .await?
            .as_deref()
            .map(DataBackfillState::decode)
            .transpose()
    }

    /// Rewrite the backfill state record so this transaction collides with any
    /// in-flight chunk. Called by DROP/TRUNCATE once the phase reaches
    /// `Backfill`: their v2 cleanup only tombstones keys their own snapshot
    /// saw, so without a shared key a concurrent chunk's fresh puts would
    /// commit alongside the drop and strand orphan shadow rows.
    ///
    /// The write is unconditional — including when no record exists yet —
    /// because the very first chunk is the one that creates the record, and a
    /// conditional rewrite would leave exactly that chunk unguarded.
    pub(crate) async fn touch_backfill_state_for_ddl(
        &self,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        let phase = self.observe_data_migration_phase_and_fence(txn).await?;
        if phase < DataMigrationPhase::Backfill {
            return Ok(());
        }
        let mut state =
            Self::read_backfill_state(txn)
                .await?
                .unwrap_or_else(|| DataBackfillState {
                    shard_count_at_start: self
                        .shard_router
                        .as_ref()
                        .map(|router| router.shard_count()),
                    chunks_done: 0,
                    rows_done: 0,
                    updated_at_unix_ms: 0,
                    complete: false,
                    cursor: None,
                });
        state.updated_at_unix_ms = u64::try_from(Self::current_epoch_ms()).unwrap_or(u64::MAX);
        txn.put(backfill_state_key(), &state.encode()?).await?;
        Ok(())
    }

    /// Copy one chunk of legacy base rows into the Data V2 keyspace and
    /// advance the durable cursor, all in the caller's single transaction.
    ///
    /// Idempotent: a redone chunk rewrites identical shadow values. Converges
    /// with concurrent DML by construction — OCC validates the write set, and
    /// a row write and this chunk both target the same v2 key, so one of them
    /// aborts and retries against fresh state.
    async fn run_backfill_chunk(&self, txn: &mut dyn Transaction) -> Result<QueryResult> {
        let phase = self.observe_data_migration_phase_and_fence(txn).await?;
        if phase < DataMigrationPhase::Backfill {
            return Err(FusionError::Execution(format!(
                "Data V2 backfill requires migration phase '{}'; current phase is '{}'",
                DataMigrationPhase::Backfill.name(),
                phase.name()
            )));
        }

        let current_shard_count = self
            .shard_router
            .as_ref()
            .map(|router| router.shard_count());
        let mut state =
            Self::read_backfill_state(txn)
                .await?
                .unwrap_or_else(|| DataBackfillState {
                    shard_count_at_start: current_shard_count,
                    chunks_done: 0,
                    rows_done: 0,
                    updated_at_unix_ms: 0,
                    complete: false,
                    cursor: None,
                });
        if state.shard_count_at_start != current_shard_count {
            return Err(FusionError::Execution(format!(
                "Data V2 backfill cannot resume across a shard topology change (started with {:?}, now {:?}); the cursor is invalid",
                state.shard_count_at_start, current_shard_count
            )));
        }
        if state.complete {
            return Ok(Self::backfill_status_result(&state, "complete"));
        }

        // Resume from the last copied key, inclusive, and skip that one key
        // while visiting. Appending a `\0` to build an exclusive bound does
        // NOT work here: FusionStorage range bounds are compared against
        // internal keys (`user_key + inverted commit ts`), and the timestamp
        // bytes sort above `\0`, so the cursor row would slip back in.
        let schemas = Self::load_all_table_schemas(txn).await?;
        let mut resume_from = state.cursor.clone();
        let resume_boundary = state.cursor.clone();
        let mut copied = 0usize;
        let mut copied_bytes = 0usize;
        let mut last_key: Option<Vec<u8>> = None;
        let mut pending: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        'ranges: for (range_start, range_end) in Self::legacy_backfill_ranges() {
            let start = match &resume_from {
                Some(cursor) if cursor.as_slice() >= range_end.as_slice() => continue,
                Some(cursor) if cursor.as_slice() > range_start.as_slice() => cursor.clone(),
                _ => range_start.clone(),
            };
            let mut batch_error = None;
            let mut visit = |key: &[u8], value: &[u8]| {
                if resume_boundary.as_deref() == Some(key) {
                    return true;
                }
                match Self::legacy_backfill_row_identity(key, value, &schemas) {
                    Ok(Some((table, row_id))) => {
                        match self.routed_structured_data_key_for_row_id(&table, &row_id) {
                            Ok(shadow_key) => {
                                copied_bytes += value.len();
                                pending.push((shadow_key, value.to_vec()));
                                copied += 1;
                            }
                            Err(error) => {
                                batch_error = Some(error);
                                return false;
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        batch_error = Some(error);
                        return false;
                    }
                }
                last_key = Some(key.to_vec());
                copied < Self::BACKFILL_CHUNK_ROWS && copied_bytes < Self::BACKFILL_CHUNK_BYTES
            };
            txn.scan_range_for_each(&start, &range_end, None, &mut visit)
                .await?;
            if let Some(error) = batch_error {
                return Err(error);
            }
            resume_from = None;
            if copied >= Self::BACKFILL_CHUNK_ROWS || copied_bytes >= Self::BACKFILL_CHUNK_BYTES {
                break 'ranges;
            }
        }

        for (shadow_key, value) in pending {
            txn.put(&shadow_key, &value).await?;
        }

        let exhausted =
            copied < Self::BACKFILL_CHUNK_ROWS && copied_bytes < Self::BACKFILL_CHUNK_BYTES;
        if let Some(key) = last_key {
            state.cursor = Some(key);
        }
        state.rows_done = state.rows_done.saturating_add(copied as u64);
        state.chunks_done = state.chunks_done.saturating_add(1);
        state.updated_at_unix_ms = u64::try_from(Self::current_epoch_ms()).unwrap_or(u64::MAX);
        state.complete = exhausted;
        txn.put(backfill_state_key(), &state.encode()?).await?;

        Ok(Self::backfill_status_result(
            &state,
            if exhausted { "complete" } else { "in-progress" },
        ))
    }

    fn backfill_status_result(state: &DataBackfillState, status: &str) -> QueryResult {
        QueryResult::Select {
            columns: vec![
                "status".to_string(),
                "rows_done".to_string(),
                "chunks_done".to_string(),
                "cursor".to_string(),
            ],
            rows: vec![vec![
                Value::String(status.to_string()),
                Value::Integer(state.rows_done as i64),
                Value::Integer(state.chunks_done as i64),
                match &state.cursor {
                    Some(cursor) => Value::String(String::from_utf8_lossy(cursor).into_owned()),
                    None => Value::Null,
                },
            ]],
        }
    }

    async fn handle_backfill_status(&self, txn: &mut dyn Transaction) -> Result<QueryResult> {
        match Self::read_backfill_state(txn).await? {
            Some(state) => Ok(Self::backfill_status_result(
                &state,
                if state.complete {
                    "complete"
                } else {
                    "in-progress"
                },
            )),
            None => Ok(Self::backfill_status_result(
                &DataBackfillState {
                    shard_count_at_start: None,
                    chunks_done: 0,
                    rows_done: 0,
                    updated_at_unix_ms: 0,
                    complete: false,
                    cursor: None,
                },
                "not-started",
            )),
        }
    }

    /// Check if a user has permission to access a table with a given operation.
    /// Public API for RBAC enforcement.
    pub async fn check_table_permission(
        &self,
        username: &str,
        table: &str,
        operation: &str,
    ) -> Result<()> {
        let mut txn = self.storage.begin_transaction().await?;
        crate::auth::check_permission(&mut *txn, username, table, operation).await
    }

    pub async fn authorize_sql(&self, username: &str, sql: &str) -> Result<()> {
        if Self::is_legacy_superuser(username) {
            return Ok(());
        }

        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_uppercase();

        if upper == "SHOW USERS"
            || upper.starts_with("CREATE USER ")
            || upper.starts_with("DROP USER ")
            || upper.starts_with("GRANT ")
            || upper.starts_with("REVOKE ")
        {
            return self.require_superuser(username).await;
        }

        // CALL is deliberately not string-matched here: a leading comment or
        // tab defeats a prefix test. authorize_statement gates it on the
        // parsed statement instead.
        let statements = self.prepare(sql)?;
        for statement in &statements {
            self.authorize_statement(username, statement).await?;
        }

        Ok(())
    }

    pub async fn require_superuser(&self, username: &str) -> Result<()> {
        if Self::is_legacy_superuser(username) {
            return Ok(());
        }

        let mut txn = self.storage.begin_transaction().await?;
        match crate::auth::get_user(&mut *txn, username).await? {
            Some(user) if user.is_superuser => Ok(()),
            Some(_) => Err(FusionError::Execution(format!(
                "Permission denied: user '{}' must be a superuser for this operation",
                username
            ))),
            None => Err(FusionError::Execution(format!(
                "Permission denied: user '{}' is not registered in RBAC",
                username
            ))),
        }
    }

    pub async fn authorize_statement(&self, username: &str, stmt: &Statement) -> Result<()> {
        if Self::is_legacy_superuser(username) {
            return Ok(());
        }

        if matches!(stmt, Statement::Vacuum(_)) {
            return self.require_superuser(username).await;
        }

        // Statement-shaped, not string-prefix-shaped: a leading comment or
        // an extended-protocol bind must not slip a migration procedure past
        // the superuser gate.
        if Self::statement_is_data_migration_call(stmt) {
            return self.require_superuser(username).await;
        }

        for (table, operation) in Self::statement_permissions(stmt) {
            self.check_table_permission(username, &table, operation)
                .await?;
        }

        Ok(())
    }

    fn statement_permissions(stmt: &Statement) -> Vec<(String, &'static str)> {
        match stmt {
            Statement::Query(query) => {
                let mut tables = Vec::with_capacity(Self::query_table_capacity(query));
                Self::collect_query_tables(query, &mut tables);
                let mut permissions = Vec::with_capacity(tables.len());
                for table in tables {
                    permissions.push((table, "SELECT"));
                }
                permissions
            }
            Statement::Insert(insert) => vec![(insert.table.to_string(), "INSERT")],
            Statement::Delete(delete) => {
                let table = match &delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(tables)
                    | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                        tables.first().and_then(|table| {
                            if let TableFactor::Table { name, .. } = &table.relation {
                                Some(name.to_string())
                            } else {
                                None
                            }
                        })
                    }
                };
                match table {
                    Some(name) => vec![(name, "DELETE")],
                    None => Vec::new(),
                }
            }
            Statement::Update(update) => {
                let sqlparser::ast::TableWithJoins { relation, .. } = &update.table;
                if let TableFactor::Table { name, .. } = relation {
                    vec![(name.to_string(), "UPDATE")]
                } else {
                    Vec::new()
                }
            }
            Statement::CreateTable(create_table) => vec![(create_table.name.to_string(), "ALL")],
            Statement::CreateIndex(create_index) => {
                vec![(create_index.table_name.to_string(), "ALL")]
            }
            Statement::AlterTable(alter_table) => vec![(alter_table.name.to_string(), "ALL")],
            Statement::Truncate(truncate) => {
                let mut permissions = Vec::with_capacity(truncate.table_names.len());
                for name in &truncate.table_names {
                    permissions.push((name.to_string(), "DELETE"));
                }
                permissions
            }
            Statement::CreateView(create_view) => {
                let mut source_tables =
                    Vec::with_capacity(Self::query_table_capacity(&create_view.query));
                Self::collect_query_tables(&create_view.query, &mut source_tables);
                let mut permissions = Vec::with_capacity(source_tables.len() + 1);
                permissions.push((create_view.name.to_string(), "ALL"));
                for table in source_tables {
                    permissions.push((table, "SELECT"));
                }
                permissions
            }
            Statement::Explain { statement, .. } => Self::statement_permissions(statement),
            Statement::ExplainTable { table_name, .. } => vec![(table_name.to_string(), "SELECT")],
            Statement::Analyze(analyze) => vec![(analyze.table_name.to_string(), "SELECT")],
            Statement::Copy { source, to, .. } => match source {
                sqlparser::ast::CopySource::Table { table_name, .. } => {
                    vec![(
                        table_name.to_string(),
                        if *to { "SELECT" } else { "INSERT" },
                    )]
                }
                sqlparser::ast::CopySource::Query(query) => {
                    let mut tables = Vec::with_capacity(Self::query_table_capacity(query));
                    Self::collect_query_tables(query, &mut tables);
                    let mut permissions = Vec::with_capacity(tables.len());
                    for table in tables {
                        permissions.push((table, "SELECT"));
                    }
                    permissions
                }
            },
            Statement::ShowCreate { obj_name, .. } => vec![(obj_name.to_string(), "SELECT")],
            Statement::Drop {
                names, object_type, ..
            } => match object_type {
                ObjectType::Table | ObjectType::View | ObjectType::Index => {
                    let mut permissions = Vec::with_capacity(names.len());
                    for name in names {
                        permissions.push((name.to_string(), "ALL"));
                    }
                    permissions
                }
                _ => Vec::new(),
            },
            _ => Vec::new(),
        }
    }

    fn collect_query_tables(query: &sqlparser::ast::Query, tables: &mut Vec<String>) {
        if let SetExpr::Select(select) = query.body.as_ref() {
            for table_with_joins in &select.from {
                Self::collect_table_factor(&table_with_joins.relation, tables);
                for join in &table_with_joins.joins {
                    Self::collect_table_factor(&join.relation, tables);
                }
            }
        }
    }

    fn query_table_capacity(query: &sqlparser::ast::Query) -> usize {
        if let SetExpr::Select(select) = query.body.as_ref() {
            select
                .from
                .iter()
                .map(|table_with_joins| {
                    Self::table_factor_table_capacity(&table_with_joins.relation)
                        + table_with_joins
                            .joins
                            .iter()
                            .map(|join| Self::table_factor_table_capacity(&join.relation))
                            .sum::<usize>()
                })
                .sum()
        } else {
            0
        }
    }

    fn table_factor_table_capacity(table: &TableFactor) -> usize {
        match table {
            TableFactor::Table { .. } => 1,
            TableFactor::Derived { subquery, .. } => Self::query_table_capacity(subquery),
            _ => 0,
        }
    }

    fn collect_table_factor(table: &TableFactor, tables: &mut Vec<String>) {
        match table {
            TableFactor::Table { name, .. } => tables.push(name.to_string()),
            TableFactor::Derived { subquery, .. } => Self::collect_query_tables(subquery, tables),
            _ => {}
        }
    }

    pub async fn execute(&self, stmt: &Statement) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let mut txn = self.storage.begin_transaction().await?;
        let res = self.execute_in_transaction(stmt, &mut *txn).await;
        if res.is_ok() {
            txn.commit().await?;
            if Self::statement_may_change_update_fast_path_metadata(stmt) {
                self.invalidate_update_fast_path_cache();
            }
            if Self::statement_may_change_query_results(stmt) {
                self.invalidate_query_result_cache();
            }
        } else {
            txn.rollback().await?;
        }
        crate::monitor::record_query(&stmt.to_string(), start.elapsed());
        res
    }

    /// Execute a raw SQL string, handling both custom statements (SHOW VIEWS)
    /// and normal sqlparser statements.
    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<QueryResult>> {
        let trimmed = sql.trim().trim_end_matches(';').trim();

        // Handle custom statements not supported by sqlparser
        let upper = trimmed.to_uppercase();

        if upper == "SHOW VIEWS" {
            let start = std::time::Instant::now();
            let mut txn = self.storage.begin_transaction().await?;
            let res = self.handle_show_views(&mut *txn).await;
            if res.is_ok() {
                txn.commit().await?;
            }
            crate::monitor::record_query(trimmed, start.elapsed());
            return res.map(|r| vec![r]);
        }

        if upper == "SHOW INDEXES" || upper.starts_with("SHOW INDEXES FROM ") {
            let table_filter = if upper.starts_with("SHOW INDEXES FROM ") {
                Some(trimmed["SHOW INDEXES FROM ".len()..].trim()).filter(|table| !table.is_empty())
            } else {
                None
            };
            let start = std::time::Instant::now();
            let mut txn = self.storage.begin_transaction().await?;
            let res = self.handle_show_indexes(table_filter, &mut *txn).await;
            if res.is_ok() {
                txn.commit().await?;
            }
            crate::monitor::record_query(trimmed, start.elapsed());
            return res.map(|r| vec![r]);
        }

        if upper == "SHOW USERS" {
            let start = std::time::Instant::now();
            let mut txn = self.storage.begin_transaction().await?;
            let res = self.handle_show_users(&mut *txn).await;
            if res.is_ok() {
                txn.commit().await?;
            }
            crate::monitor::record_query(trimmed, start.elapsed());
            return res.map(|r| vec![r]);
        }

        if upper == "SHOW ALL" {
            crate::monitor::record_query(trimmed, std::time::Duration::ZERO);
            return Ok(vec![Self::show_all_settings_result()]);
        }

        if upper.starts_with("CREATE USER ") {
            return self.handle_rbac_sql(trimmed).await.map(|r| vec![r]);
        }
        if upper.starts_with("DROP USER ") {
            return self.handle_rbac_sql(trimmed).await.map(|r| vec![r]);
        }
        if upper.starts_with("GRANT ") {
            return self.handle_rbac_sql(trimmed).await.map(|r| vec![r]);
        }
        if upper.starts_with("REVOKE ") {
            return self.handle_rbac_sql(trimmed).await.map(|r| vec![r]);
        }

        let stmts = self.prepare(sql)?;
        if stmts.len() > 1
            && stmts
                .iter()
                .any(|stmt| matches!(stmt, Statement::Vacuum(_)))
        {
            return Err(FusionError::Execution(
                "VACUUM must be executed as a standalone statement".to_string(),
            ));
        }
        if stmts.len() > 1 && stmts.iter().any(Self::statement_is_data_migration_call) {
            return Err(FusionError::Execution(
                "Data V2 migration CALL procedures must be executed as standalone statements"
                    .to_string(),
            ));
        }

        if stmts.len() == 1
            && (Self::is_cacheable_group_aggregate_statement(&stmts[0])
                || Self::is_cacheable_join_group_aggregate_statement(&stmts[0]))
        {
            let start = std::time::Instant::now();
            let cache_key = Self::query_result_cache_key(trimmed);
            let current_epoch = self.current_query_result_epoch();
            crate::monitor::inc_query_result_cache_eligible();
            if let Some(cached) = self.query_result_cache.get(&cache_key) {
                if cached.epoch == current_epoch {
                    crate::monitor::inc_query_result_cache_hit();
                    crate::monitor::record_query(trimmed, start.elapsed());
                    return Ok(vec![QueryResult::Select {
                        columns: cached.columns,
                        rows: cached.rows,
                    }]);
                }
                crate::monitor::inc_query_result_cache_stale();
            }

            crate::monitor::inc_query_result_cache_miss();
            let result = self.execute(&stmts[0]).await?;
            if let QueryResult::Select { columns, rows } = &result {
                self.query_result_cache.insert(
                    cache_key,
                    CachedSelectResult {
                        epoch: current_epoch,
                        columns: columns.clone(),
                        rows: rows.clone(),
                    },
                );
                crate::monitor::inc_query_result_cache_insert();
            }
            return Ok(vec![result]);
        }

        if stmts.len() == 1 {
            return self.execute(&stmts[0]).await.map(|result| vec![result]);
        }

        let start = std::time::Instant::now();
        let mut txn = self.storage.begin_transaction().await?;
        let mut results = Vec::with_capacity(stmts.len());
        let mut may_change_query_results = false;
        let mut may_change_update_fast_path_metadata = false;
        for stmt in &stmts {
            may_change_query_results |= Self::statement_may_change_query_results(stmt);
            may_change_update_fast_path_metadata |=
                Self::statement_may_change_update_fast_path_metadata(stmt);
            match self.execute_in_transaction(stmt, &mut *txn).await {
                Ok(result) => results.push(result),
                Err(error) => {
                    txn.rollback().await?;
                    crate::monitor::record_query(trimmed, start.elapsed());
                    return Err(error);
                }
            }
        }

        txn.commit().await?;
        if may_change_update_fast_path_metadata {
            self.invalidate_update_fast_path_cache();
        }
        if may_change_query_results {
            self.invalidate_query_result_cache();
        }
        crate::monitor::record_query(trimmed, start.elapsed());
        Ok(results)
    }

    async fn handle_vacuum(&self, vacuum: &sqlparser::ast::VacuumStatement) -> Result<QueryResult> {
        if vacuum.table_name.is_some() {
            return Err(FusionError::Execution(
                "Table-specific VACUUM is not supported yet; use VACUUM for full database compaction"
                    .to_string(),
            ));
        }

        if vacuum.sort_only
            || vacuum.delete_only
            || vacuum.reindex
            || vacuum.recluster
            || vacuum.threshold.is_some()
            || vacuum.boost
        {
            return Err(FusionError::Execution(
                "VACUUM options other than FULL are not supported yet".to_string(),
            ));
        }

        let Some(fusion) = self.storage.as_any().downcast_ref::<FusionStorage>() else {
            return Err(FusionError::Execution(
                "VACUUM is only available for FusionStorage".to_string(),
            ));
        };

        let compacted = fusion.compact_now().await?;
        let detail = if compacted {
            "compaction completed"
        } else {
            "compaction skipped: not enough SSTables"
        };

        Ok(QueryResult::Success {
            message: format!("VACUUM completed: {detail}"),
        })
    }

    async fn handle_show_users(&self, txn: &mut dyn Transaction) -> Result<QueryResult> {
        let users = crate::auth::list_users(txn).await?;
        let mut rows = Vec::with_capacity(users.len());
        for (name, record) in &users {
            let mut perms = Vec::with_capacity(record.permissions.len());
            for (table, privileges) in &record.permissions {
                let mut privilege_names = Vec::with_capacity(privileges.len());
                for privilege in privileges {
                    privilege_names.push(privilege.clone());
                }
                perms.push(format!("{}: {}", table, privilege_names.join(",")));
            }
            rows.push(vec![
                Value::String(name.clone()),
                Value::Boolean(record.is_superuser),
                Value::String(if perms.is_empty() {
                    "none".to_string()
                } else {
                    perms.join("; ")
                }),
            ]);
        }
        Ok(QueryResult::Select {
            columns: vec![
                "User".to_string(),
                "Superuser".to_string(),
                "Permissions".to_string(),
            ],
            rows,
        })
    }

    async fn handle_rbac_sql(&self, sql: &str) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let mut txn = self.storage.begin_transaction().await?;
        let res = self.execute_rbac_statement(sql, &mut *txn).await;
        if res.is_ok() {
            txn.commit().await?;
        } else {
            txn.rollback().await?;
        }
        crate::monitor::record_query(sql, start.elapsed());
        res
    }

    async fn execute_rbac_statement(
        &self,
        sql: &str,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();

        // CREATE USER <name> WITH PASSWORD '<password>'
        // CREATE USER <name> WITH PASSWORD '<password>' SUPERUSER
        if upper.starts_with("CREATE USER ") {
            if parts.len() < 6 {
                return Err(FusionError::Execution(
                    "Syntax: CREATE USER <name> WITH PASSWORD '<password>' [SUPERUSER]".to_string(),
                ));
            }
            let username = parts[2].to_lowercase();
            let password = self.extract_quoted_string(sql, "PASSWORD")?;
            let is_superuser = upper.contains("SUPERUSER") && !upper.contains("WITH PASSWORD")
                || upper.ends_with("SUPERUSER");

            if crate::auth::get_user(txn, &username).await?.is_some() {
                return Err(FusionError::Execution(format!(
                    "User '{}' already exists",
                    username
                )));
            }

            let record = crate::auth::UserRecord::new(&password, is_superuser);
            crate::auth::save_user(txn, &username, &record).await?;

            return Ok(QueryResult::Success {
                message: format!("User '{}' created", username),
            });
        }

        // DROP USER <name>
        // DROP USER IF EXISTS <name>
        if upper.starts_with("DROP USER ") {
            let if_exists = upper.contains("IF EXISTS");
            let name_idx = if if_exists { 4 } else { 2 };
            if parts.len() <= name_idx {
                return Err(FusionError::Execution(
                    "Syntax: DROP USER [IF EXISTS] <name>".to_string(),
                ));
            }
            let username = parts[name_idx].to_lowercase();

            if crate::auth::get_user(txn, &username).await?.is_none() {
                if if_exists {
                    return Ok(QueryResult::Success {
                        message: format!("User '{}' does not exist (IF EXISTS)", username),
                    });
                }
                return Err(FusionError::Execution(format!(
                    "User '{}' does not exist",
                    username
                )));
            }

            crate::auth::delete_user(txn, &username).await?;
            return Ok(QueryResult::Success {
                message: format!("User '{}' dropped", username),
            });
        }

        // GRANT <privilege> ON <table> TO <user>
        // GRANT ALL ON * TO <user>
        if upper.starts_with("GRANT ") {
            // Parse: GRANT <priv>[, <priv>] ON <table> TO <user>
            let on_pos = upper.find(" ON ").ok_or_else(|| {
                FusionError::Execution("Syntax: GRANT <privilege> ON <table> TO <user>".to_string())
            })?;
            let to_pos = upper.find(" TO ").ok_or_else(|| {
                FusionError::Execution("Syntax: GRANT <privilege> ON <table> TO <user>".to_string())
            })?;

            let privs_str = sql[6..on_pos].trim();
            let table = sql[on_pos + 4..to_pos].trim();
            let username = sql[to_pos + 4..].trim().to_lowercase();

            let mut privileges = Vec::with_capacity(privs_str.matches(',').count() + 1);
            for privilege in privs_str.split(',') {
                privileges.push(privilege.trim().to_uppercase());
            }

            let mut record = crate::auth::get_user(txn, &username)
                .await?
                .ok_or_else(|| {
                    FusionError::Execution(format!("User '{}' does not exist", username))
                })?;

            for priv_name in &privileges {
                record.grant(table, priv_name);
            }
            crate::auth::save_user(txn, &username, &record).await?;

            return Ok(QueryResult::Success {
                message: format!("Granted {} on {} to {}", privs_str, table, username),
            });
        }

        // REVOKE <privilege> ON <table> FROM <user>
        if upper.starts_with("REVOKE ") {
            let on_pos = upper.find(" ON ").ok_or_else(|| {
                FusionError::Execution(
                    "Syntax: REVOKE <privilege> ON <table> FROM <user>".to_string(),
                )
            })?;
            let from_pos = upper.find(" FROM ").ok_or_else(|| {
                FusionError::Execution(
                    "Syntax: REVOKE <privilege> ON <table> FROM <user>".to_string(),
                )
            })?;

            let privs_str = sql[7..on_pos].trim();
            let table = sql[on_pos + 4..from_pos].trim();
            let username = sql[from_pos + 6..].trim().to_lowercase();

            let mut privileges = Vec::with_capacity(privs_str.matches(',').count() + 1);
            for privilege in privs_str.split(',') {
                privileges.push(privilege.trim().to_uppercase());
            }

            let mut record = crate::auth::get_user(txn, &username)
                .await?
                .ok_or_else(|| {
                    FusionError::Execution(format!("User '{}' does not exist", username))
                })?;

            for priv_name in &privileges {
                record.revoke(table, priv_name);
            }
            crate::auth::save_user(txn, &username, &record).await?;

            return Ok(QueryResult::Success {
                message: format!("Revoked {} on {} from {}", privs_str, table, username),
            });
        }

        Err(FusionError::Execution(format!(
            "Unsupported RBAC statement: {}",
            sql
        )))
    }

    fn extract_quoted_string(&self, sql: &str, keyword: &str) -> Result<String> {
        let upper = sql.to_uppercase();
        let kw_pos = upper
            .find(keyword)
            .ok_or_else(|| FusionError::Execution(format!("Missing {} keyword", keyword)))?;
        let after_kw = &sql[kw_pos + keyword.len()..].trim_start();
        // Find the quoted string (single quotes)
        if let Some(start) = after_kw.find('\'') {
            if let Some(end) = after_kw[start + 1..].find('\'') {
                return Ok(after_kw[start + 1..start + 1 + end].to_string());
            }
        }
        Err(FusionError::Execution(format!(
            "Expected quoted string after {}",
            keyword
        )))
    }

    pub(crate) fn index_count_summary_maintained_for_column(
        column: &crate::catalog::Column,
    ) -> bool {
        column.is_indexed
            && !column.is_primary
            && !column.is_nullable
            && column.index_type == crate::catalog::IndexType::BTree
    }

    pub(crate) fn index_count_summary_meta_key_for_column(
        table_name: &str,
        column_name: &str,
    ) -> String {
        let mut key = String::with_capacity(
            "index_count_meta:".len() + table_name.len() + 1 + column_name.len(),
        );
        key.push_str("index_count_meta:");
        key.push_str(table_name);
        key.push(':');
        key.push_str(column_name);
        key
    }

    fn index_count_summary_meta_prefix_for_table(table_name: &str) -> String {
        let mut prefix = String::with_capacity("index_count_meta:".len() + table_name.len() + 1);
        prefix.push_str("index_count_meta:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix
    }

    fn index_count_summary_prefix_for_table(table_name: &str) -> String {
        let mut prefix = String::with_capacity("index_count:".len() + table_name.len() + 1);
        prefix.push_str("index_count:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix
    }

    pub(crate) fn index_count_summary_prefix_for_column(
        table_name: &str,
        column_name: &str,
    ) -> String {
        let mut prefix = String::with_capacity(
            "index_count:".len() + table_name.len() + 1 + column_name.len() + 1,
        );
        prefix.push_str("index_count:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix.push_str(column_name);
        prefix.push(':');
        prefix
    }

    pub(crate) fn index_count_summary_key_for_value(
        table_name: &str,
        column_name: &str,
        value_key: &str,
    ) -> String {
        let mut key = Self::index_count_summary_prefix_for_column(table_name, column_name);
        key.reserve(value_key.len());
        key.push_str(value_key);
        key
    }

    pub(crate) fn encode_index_count_summary_count(count: i64) -> [u8; 8] {
        count.to_le_bytes()
    }

    fn encode_index_count_summary_meta(total_entries: i64, group_count: usize) -> Vec<u8> {
        format!("v1:{}:{}", total_entries, group_count).into_bytes()
    }

    pub(crate) fn decode_index_count_summary_meta(bytes: &[u8]) -> Option<(i64, usize)> {
        let meta = std::str::from_utf8(bytes).ok()?;
        let mut parts = meta.split(':');
        if parts.next()? != "v1" {
            return None;
        }
        let total_entries = parts.next()?.parse::<i64>().ok()?;
        let group_count = parts.next()?.parse::<usize>().ok()?;
        if parts.next().is_some() || total_entries < 0 {
            return None;
        }
        Some((total_entries, group_count))
    }

    pub(crate) fn decode_index_count_summary_count(bytes: &[u8]) -> Option<i64> {
        if bytes.len() != 8 {
            return None;
        }
        let mut encoded = [0u8; 8];
        encoded.copy_from_slice(bytes);
        let count = i64::from_le_bytes(encoded);
        (count >= 0).then_some(count)
    }

    pub(crate) async fn index_count_summary_available(
        &self,
        table_name: &str,
        column_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<bool> {
        if self.shard_router.is_some() {
            return Ok(false);
        }
        let meta_key = Self::index_count_summary_meta_key_for_column(table_name, column_name);
        let Some(meta_bytes) = txn.get(meta_key.as_bytes()).await? else {
            return Ok(false);
        };
        Ok(Self::decode_index_count_summary_meta(&meta_bytes).is_some())
    }

    pub(crate) async fn load_index_count_summary_meta(
        &self,
        table_name: &str,
        column_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Option<(i64, usize)>> {
        if self.shard_router.is_some() {
            return Ok(None);
        }
        let meta_key = Self::index_count_summary_meta_key_for_column(table_name, column_name);
        let Some(meta_bytes) = txn.get(meta_key.as_bytes()).await? else {
            return Ok(None);
        };
        Ok(Self::decode_index_count_summary_meta(&meta_bytes))
    }

    pub(crate) async fn replace_index_count_summary_for_column(
        &self,
        table_name: &str,
        column_name: &str,
        counts: &HashMap<String, i64>,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        if self.shard_router.is_some() {
            return Ok(());
        }

        self.delete_index_count_summary_for_column(table_name, column_name, txn)
            .await?;
        let mut total_entries = 0i64;
        for (value_key, count) in counts {
            if *count <= 0 {
                return Err(FusionError::Execution(format!(
                    "Invalid index count summary for {}.{}: {}",
                    table_name, column_name, count
                )));
            }
            total_entries = total_entries.checked_add(*count).ok_or_else(|| {
                FusionError::Execution(format!(
                    "Index count summary overflow for {}.{}",
                    table_name, column_name
                ))
            })?;
            let key = Self::index_count_summary_key_for_value(table_name, column_name, value_key);
            let count_bytes = Self::encode_index_count_summary_count(*count);
            txn.put(key.as_bytes(), &count_bytes).await?;
        }

        let meta_key = Self::index_count_summary_meta_key_for_column(table_name, column_name);
        let meta_value = Self::encode_index_count_summary_meta(total_entries, counts.len());
        txn.put(meta_key.as_bytes(), &meta_value).await?;
        Ok(())
    }

    pub(crate) async fn delete_index_count_summary_for_column(
        &self,
        table_name: &str,
        column_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        let prefix = Self::index_count_summary_prefix_for_column(table_name, column_name);
        let entries = txn.scan_prefix(prefix.as_bytes(), None).await?;
        for (key, _) in entries {
            txn.delete(&key).await?;
        }
        let meta_key = Self::index_count_summary_meta_key_for_column(table_name, column_name);
        txn.delete(meta_key.as_bytes()).await?;
        Ok(())
    }

    pub(crate) async fn delete_index_count_summaries_for_table(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        for prefix in [
            Self::index_count_summary_prefix_for_table(table_name),
            Self::index_count_summary_meta_prefix_for_table(table_name),
        ] {
            let entries = txn.scan_prefix(prefix.as_bytes(), None).await?;
            for (key, _) in entries {
                txn.delete(&key).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn adjust_index_count_summary(
        &self,
        table_name: &str,
        column_name: &str,
        value_key: &str,
        delta: i64,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        if delta == 0 {
            return Ok(());
        }
        let Some((total_entries, group_count)) = self
            .load_index_count_summary_meta(table_name, column_name, txn)
            .await?
        else {
            return Ok(());
        };

        let key = Self::index_count_summary_key_for_value(table_name, column_name, value_key);
        let current_count = match txn.get(key.as_bytes()).await? {
            Some(bytes) => Self::decode_index_count_summary_count(&bytes).ok_or_else(|| {
                FusionError::Execution(format!(
                    "Malformed index count summary for {}.{}",
                    table_name, column_name
                ))
            })?,
            None => 0,
        };
        let new_count = current_count.checked_add(delta).ok_or_else(|| {
            FusionError::Execution(format!(
                "Index count summary overflow for {}.{}",
                table_name, column_name
            ))
        })?;
        if new_count < 0 {
            return Err(FusionError::Execution(format!(
                "Index count summary underflow for {}.{}",
                table_name, column_name
            )));
        }
        let new_total_entries = total_entries.checked_add(delta).ok_or_else(|| {
            FusionError::Execution(format!(
                "Index count summary overflow for {}.{}",
                table_name, column_name
            ))
        })?;
        if new_total_entries < 0 {
            return Err(FusionError::Execution(format!(
                "Index count summary metadata underflow for {}.{}",
                table_name, column_name
            )));
        }
        let new_group_count = match (current_count == 0, new_count == 0) {
            (true, false) => group_count.checked_add(1).ok_or_else(|| {
                FusionError::Execution(format!(
                    "Index count summary group overflow for {}.{}",
                    table_name, column_name
                ))
            })?,
            (false, true) => group_count.checked_sub(1).ok_or_else(|| {
                FusionError::Execution(format!(
                    "Index count summary group underflow for {}.{}",
                    table_name, column_name
                ))
            })?,
            _ => group_count,
        };
        if new_count == 0 {
            txn.delete(key.as_bytes()).await?;
        } else {
            let count_bytes = Self::encode_index_count_summary_count(new_count);
            txn.put(key.as_bytes(), &count_bytes).await?;
        }
        let meta_key = Self::index_count_summary_meta_key_for_column(table_name, column_name);
        let meta_value = Self::encode_index_count_summary_meta(new_total_entries, new_group_count);
        txn.put(meta_key.as_bytes(), &meta_value).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

    fn statement_permissions(sql: &str) -> Vec<(String, &'static str)> {
        let statements = parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        Executor::statement_permissions(&statements[0])
    }

    fn sql_requires_raft_write(sql: &str) -> bool {
        let wal_path = format!("test_sql_requires_raft_write_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage);
        let result = executor.sql_requires_raft_write(sql).unwrap();
        let _ = std::fs::remove_file(wal_path);
        result
    }

    fn sharded_test_config() -> crate::config::Config {
        let mut config = crate::config::Config::default();
        config.distributed.enabled = true;
        config.distributed.node_id = 1;
        config.distributed.sharding = crate::config::ShardingConfig {
            enabled: true,
            strategy: crate::config::ShardingStrategy::Hash,
            shard_count: 4,
            range_boundaries: Vec::new(),
        };
        config
    }

    #[test]
    fn distinct_aggregate_select_fanout_plans_match_sum_and_avg() {
        let sum_plan = Executor::shard_sum_distinct_select_fanout_plan_for_statements(
            &parse_sql("SELECT SUM(DISTINCT amount) FROM orders").unwrap(),
        )
        .expect("sum distinct plan");
        assert_eq!(sum_plan.rewritten_sql, "SELECT DISTINCT amount FROM orders");
        assert_eq!(sum_plan.output_column, "SUM(DISTINCT amount)");

        let avg_plan = Executor::shard_avg_distinct_select_fanout_plan_for_statements(
            &parse_sql("SELECT AVG(DISTINCT amount) AS a FROM orders WHERE amount > 5").unwrap(),
        )
        .expect("avg distinct plan");
        assert_eq!(
            avg_plan.rewritten_sql,
            "SELECT DISTINCT amount FROM orders WHERE amount > 5"
        );
        assert_eq!(avg_plan.output_column, "a");

        // Non-distinct aggregates and grouped queries are not eligible for distinct fan-out.
        assert!(
            Executor::shard_sum_distinct_select_fanout_plan_for_statements(
                &parse_sql("SELECT SUM(amount) FROM orders").unwrap()
            )
            .is_none()
        );
        assert!(
            Executor::shard_sum_distinct_select_fanout_plan_for_statements(
                &parse_sql("SELECT SUM(DISTINCT amount) FROM orders GROUP BY region").unwrap()
            )
            .is_none()
        );
        assert!(
            Executor::shard_avg_distinct_select_fanout_plan_for_statements(
                &parse_sql("SELECT AVG(amount) FROM orders").unwrap()
            )
            .is_none()
        );
    }

    #[test]
    fn group_count_select_fanout_plan_matches_group_count_shapes() {
        // col, COUNT(*) and COUNT(*), col both eligible; indices track the projection order.
        let p1 = Executor::shard_group_count_select_fanout_plan_for_statements(
            &parse_sql("SELECT region, COUNT(*) FROM orders GROUP BY region").unwrap(),
        )
        .expect("group count plan");
        assert_eq!(p1.group_indices, vec![0]);
        assert_eq!(p1.count_index, 1);

        let p2 = Executor::shard_group_count_select_fanout_plan_for_statements(
            &parse_sql("SELECT COUNT(*), region FROM orders WHERE region <> 'x' GROUP BY region")
                .unwrap(),
        )
        .expect("group count plan reversed");
        assert_eq!(p2.count_index, 0);
        assert_eq!(p2.group_indices, vec![1]);

        // Multi-column GROUP BY: composite key, indices track projection order.
        let p3 = Executor::shard_group_count_select_fanout_plan_for_statements(
            &parse_sql("SELECT region, country, COUNT(*) FROM orders GROUP BY region, country")
                .unwrap(),
        )
        .expect("multi-column group count plan");
        assert_eq!(p3.group_indices, vec![0, 1]);
        assert_eq!(p3.count_index, 2);

        let p4 = Executor::shard_group_count_select_fanout_plan_for_statements(
            &parse_sql("SELECT country, COUNT(*), region FROM orders GROUP BY region, country")
                .unwrap(),
        )
        .expect("multi-column group count plan reordered");
        assert_eq!(p4.group_indices, vec![0, 2]);
        assert_eq!(p4.count_index, 1);

        // ORDER BY / LIMIT / OFFSET now eligible (post-merge): resolves to a GroupedPostMerge.
        let p5 = Executor::shard_group_count_select_fanout_plan_for_statements(
            &parse_sql(
                "SELECT region, COUNT(*) FROM orders GROUP BY region ORDER BY COUNT(*) DESC LIMIT 3 OFFSET 1",
            )
            .unwrap(),
        )
        .expect("group count order/limit plan");
        let post = p5.post_merge.as_ref().expect("post_merge");
        assert_eq!(
            post.order_keys
                .iter()
                .map(|k| (k.col_index, k.asc, k.nulls_first))
                .collect::<Vec<_>>(),
            vec![(1, false, true)]
        );
        assert_eq!(post.limit, Some(3));
        assert_eq!(post.offset, 1);
        assert!(!post.per_owner_sql.to_ascii_uppercase().contains("ORDER BY"));
        assert!(!post.per_owner_sql.to_ascii_uppercase().contains("LIMIT"));

        // Not eligible: no GROUP BY, HAVING, multiple group cols, non-count aggregate, FETCH (in
        // Query.fetch, separate from limit_clause → rejected so the row limit is never silently dropped).
        for sql in [
            "SELECT region, COUNT(*) FROM orders",
            "SELECT region, COUNT(*) FROM orders GROUP BY region, country",
            "SELECT region, SUM(amount) FROM orders GROUP BY region",
            "SELECT region, COUNT(*) FROM orders GROUP BY region ORDER BY COUNT(*) DESC FETCH FIRST 5 ROWS ONLY",
        ] {
            assert!(
                Executor::shard_group_count_select_fanout_plan_for_statements(
                    &parse_sql(sql).unwrap()
                )
                .is_none(),
                "should be ineligible: {sql}"
            );
        }
    }

    #[test]
    fn group_aggregate_select_fanout_plan_matches_sum_min_max() {
        use super::SqlShardGroupAggregateKind::{Max, Min, Sum};
        for (sql, kind, gidx, aidx) in [
            (
                "SELECT region, SUM(amount) FROM orders GROUP BY region",
                Sum,
                vec![0],
                1,
            ),
            (
                "SELECT MIN(amount), region FROM orders GROUP BY region",
                Min,
                vec![1],
                0,
            ),
            (
                "SELECT region, country, MAX(amount) FROM orders GROUP BY region, country",
                Max,
                vec![0, 1],
                2,
            ),
        ] {
            let plan = Executor::shard_group_aggregate_select_fanout_plan_for_statements(
                &parse_sql(sql).unwrap(),
            )
            .unwrap_or_else(|| panic!("expected group aggregate plan: {sql}"));
            assert_eq!(plan.kind, kind, "kind for {sql}");
            assert_eq!(plan.group_indices, gidx, "group_indices for {sql}");
            assert_eq!(plan.agg_index, aidx, "agg_index for {sql}");
        }

        // Ineligible: COUNT(*) (group-count path), AVG (separate ticket), no GROUP BY,
        // projection arity mismatch. ORDER BY / LIMIT (resolves_order_by_limit) and HAVING
        // (resolves_having) are now eligible (post-merge).
        for sql in [
            "SELECT region, COUNT(*) FROM orders GROUP BY region",
            "SELECT region, AVG(amount) FROM orders GROUP BY region",
            "SELECT region, SUM(amount) FROM orders",
            "SELECT region, country, SUM(amount) FROM orders GROUP BY region",
        ] {
            assert!(
                Executor::shard_group_aggregate_select_fanout_plan_for_statements(
                    &parse_sql(sql).unwrap()
                )
                .is_none(),
                "should be ineligible: {sql}"
            );
        }
    }

    #[test]
    fn group_aggregate_fanout_resolves_order_by_limit() {
        // (sql, expected order_keys as (col_index, asc, nulls_first), limit, offset, per_owner_sql)
        let cases: &[(&str, &[(usize, bool, bool)], Option<usize>, usize)] = &[
            // ORDER BY a group column, default ASC → NULLS LAST.
            (
                "SELECT region, SUM(amount) FROM orders GROUP BY region ORDER BY region",
                &[(0, true, false)],
                None,
                0,
            ),
            // ORDER BY the aggregate output DESC → NULLS FIRST by default, with LIMIT.
            (
                "SELECT region, SUM(amount) FROM orders GROUP BY region ORDER BY SUM(amount) DESC LIMIT 5",
                &[(1, false, true)],
                Some(5),
                0,
            ),
            // Positional ORDER BY + OFFSET.
            (
                "SELECT region, SUM(amount) FROM orders GROUP BY region ORDER BY 2 ASC LIMIT 3 OFFSET 2",
                &[(1, true, false)],
                Some(3),
                2,
            ),
            // Alias resolution + explicit NULLS FIRST.
            (
                "SELECT region, SUM(amount) AS total FROM orders GROUP BY region ORDER BY total ASC NULLS FIRST",
                &[(1, true, true)],
                None,
                0,
            ),
            // LIMIT only, no ORDER BY.
            (
                "SELECT region, SUM(amount) FROM orders GROUP BY region LIMIT 10",
                &[],
                Some(10),
                0,
            ),
            // Multi-key ORDER BY (group col then aggregate, mixed direction).
            (
                "SELECT region, country, MAX(amount) FROM orders GROUP BY region, country ORDER BY region ASC, MAX(amount) DESC",
                &[(0, true, false), (2, false, true)],
                None,
                0,
            ),
        ];
        for (sql, expected_keys, limit, offset) in cases {
            let plan = Executor::shard_group_aggregate_select_fanout_plan_for_statements(
                &parse_sql(sql).unwrap(),
            )
            .unwrap_or_else(|| panic!("expected eligible plan: {sql}"));
            let post = plan
                .post_merge
                .as_ref()
                .unwrap_or_else(|| panic!("expected post_merge: {sql}"));
            let got: Vec<(usize, bool, bool)> = post
                .order_keys
                .iter()
                .map(|k| (k.col_index, k.asc, k.nulls_first))
                .collect();
            assert_eq!(got.as_slice(), *expected_keys, "order_keys for {sql}");
            assert_eq!(post.limit, *limit, "limit for {sql}");
            assert_eq!(post.offset, *offset, "offset for {sql}");
            // The per-owner SQL must not carry ORDER BY / LIMIT / OFFSET (each owner returns all groups).
            let lowered = post.per_owner_sql.to_ascii_uppercase();
            assert!(
                !lowered.contains("ORDER BY"),
                "per_owner_sql kept ORDER BY: {sql}"
            );
            assert!(
                !lowered.contains("LIMIT"),
                "per_owner_sql kept LIMIT: {sql}"
            );
            assert!(
                !lowered.contains("OFFSET"),
                "per_owner_sql kept OFFSET: {sql}"
            );
            assert!(
                lowered.contains("GROUP BY"),
                "per_owner_sql lost GROUP BY: {sql}"
            );
        }

        // Unresolvable ORDER BY / unsupported clauses → None (→ 449 errors loudly, never silently wrong).
        for sql in [
            // ORDER BY a column that is not in the output projection.
            "SELECT region, SUM(amount) FROM orders GROUP BY region ORDER BY amount",
            // Positional out of range.
            "SELECT region, SUM(amount) FROM orders GROUP BY region ORDER BY 3",
            // Non-literal LIMIT.
            "SELECT region, SUM(amount) FROM orders GROUP BY region LIMIT region",
            // FETCH FIRST n ROWS lives in Query.fetch (not limit_clause): rejected so the row limit is
            // never silently dropped post-merge (regression guard from the 451 adversarial review).
            "SELECT region, SUM(amount) FROM orders GROUP BY region ORDER BY SUM(amount) DESC FETCH FIRST 5 ROWS ONLY",
            "SELECT region, SUM(amount) FROM orders GROUP BY region FETCH FIRST 5 ROWS ONLY",
        ] {
            assert!(
                Executor::shard_group_aggregate_select_fanout_plan_for_statements(
                    &parse_sql(sql).unwrap()
                )
                .is_none(),
                "should be ineligible (unresolvable post-merge): {sql}"
            );
        }
    }

    #[test]
    fn apply_grouped_order_limit_sorts_slices_and_orders_nulls() {
        use serde_json::json;
        // DESC by the aggregate (col 1), NULLS FIRST (default for DESC), then LIMIT 2.
        let mut rows = vec![
            vec![json!("a"), json!(10)],
            vec![json!("b"), json!(30)],
            vec![json!("c"), serde_json::Value::Null],
            vec![json!("d"), json!(20)],
        ];
        super::apply_grouped_order_limit(
            &mut rows,
            &GroupedPostMerge {
                per_owner_sql: String::new(),
                having: None,
                order_keys: vec![GroupedOrderKey {
                    col_index: 1,
                    asc: false,
                    nulls_first: true,
                }],
                limit: Some(2),
                offset: 0,
            },
        );
        assert_eq!(
            rows,
            vec![
                vec![json!("c"), serde_json::Value::Null],
                vec![json!("b"), json!(30)]
            ]
        );

        // OFFSET past the end yields an empty result.
        let mut rows = vec![vec![json!(1)], vec![json!(2)]];
        super::apply_grouped_order_limit(
            &mut rows,
            &GroupedPostMerge {
                per_owner_sql: String::new(),
                having: None,
                order_keys: vec![GroupedOrderKey {
                    col_index: 0,
                    asc: true,
                    nulls_first: false,
                }],
                limit: None,
                offset: 5,
            },
        );
        assert!(rows.is_empty());

        // ASC default NULLS LAST.
        let mut rows = vec![
            vec![serde_json::Value::Null],
            vec![json!(2)],
            vec![json!(1)],
        ];
        super::apply_grouped_order_limit(
            &mut rows,
            &GroupedPostMerge {
                per_owner_sql: String::new(),
                having: None,
                order_keys: vec![GroupedOrderKey {
                    col_index: 0,
                    asc: true,
                    nulls_first: false,
                }],
                limit: None,
                offset: 0,
            },
        );
        assert_eq!(
            rows,
            vec![
                vec![json!(1)],
                vec![json!(2)],
                vec![serde_json::Value::Null]
            ]
        );
    }

    #[test]
    fn apply_grouped_post_merge_filters_having_before_order_limit() {
        use serde_json::json;
        // HAVING SUM > 15 AND grp <> 'd', then ORDER BY SUM DESC LIMIT 2. Rows: [grp, sum].
        let mut rows = vec![
            vec![json!("a"), json!(10)],
            vec![json!("b"), json!(30)],
            vec![json!("c"), json!(20)],
            vec![json!("d"), json!(40)],
            vec![json!("e"), serde_json::Value::Null],
        ];
        let spec = GroupedPostMerge {
            per_owner_sql: String::new(),
            having: Some(GroupedHaving {
                conjuncts: vec![
                    GroupedHavingConjunct {
                        col_index: 1,
                        op: GroupedHavingOp::Gt,
                        literal: json!(15),
                    },
                    GroupedHavingConjunct {
                        col_index: 0,
                        op: GroupedHavingOp::NotEq,
                        literal: json!("d"),
                    },
                ],
            }),
            order_keys: vec![GroupedOrderKey {
                col_index: 1,
                asc: false,
                nulls_first: true,
            }],
            limit: Some(2),
            offset: 0,
        };
        super::apply_grouped_order_limit(&mut rows, &spec);
        // 'a'=10 drops (<=15), 'd'=40 drops (grp=d), 'e'=NULL drops (NULL>15 unknown). Remaining
        // b=30, c=20 → ORDER BY sum DESC → [b, c].
        assert_eq!(
            rows,
            vec![vec![json!("b"), json!(30)], vec![json!("c"), json!(20)]]
        );
    }

    #[test]
    fn group_aggregate_fanout_resolves_having() {
        // HAVING on the aggregate output + AND a group column, literal on either side.
        let plan = Executor::shard_group_aggregate_select_fanout_plan_for_statements(
            &parse_sql(
                "SELECT region, SUM(amount) FROM orders GROUP BY region \
                 HAVING SUM(amount) > 100 AND region <> 'x' ORDER BY SUM(amount) DESC LIMIT 3",
            )
            .unwrap(),
        )
        .expect("eligible having plan");
        let post = plan.post_merge.as_ref().expect("post_merge");
        let having = post.having.as_ref().expect("having");
        assert_eq!(having.conjuncts.len(), 2);
        assert_eq!(having.conjuncts[0].col_index, 1); // SUM output column
        assert_eq!(having.conjuncts[0].op, GroupedHavingOp::Gt);
        assert_eq!(having.conjuncts[0].literal, serde_json::json!(100));
        assert_eq!(having.conjuncts[1].col_index, 0); // region group column
        assert_eq!(having.conjuncts[1].op, GroupedHavingOp::NotEq);
        assert_eq!(post.order_keys.len(), 1);
        assert_eq!(post.limit, Some(3));
        // Per-owner SQL must not carry HAVING (owners return all groups).
        assert!(!post.per_owner_sql.to_ascii_uppercase().contains("HAVING"));

        // Literal-on-left flips the operator: 100 < SUM(amount) ⇒ col > 100. A SMALL literal that would
        // fall within the positional 1..=arity range (2 <= SUM(amount)) must ALSO flip, not be misread
        // as a positional column reference (regression guard from the 452 adversarial review).
        for (sql, expect_op, expect_lit) in [
            (
                "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING 100 < SUM(amount)",
                GroupedHavingOp::Gt,
                100,
            ),
            (
                "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING 2 <= SUM(amount)",
                GroupedHavingOp::GtEq,
                2,
            ),
        ] {
            let flipped = Executor::shard_group_aggregate_select_fanout_plan_for_statements(
                &parse_sql(sql).unwrap(),
            )
            .unwrap_or_else(|| panic!("flipped having plan: {sql}"));
            let c = &flipped
                .post_merge
                .as_ref()
                .unwrap()
                .having
                .as_ref()
                .unwrap()
                .conjuncts[0];
            assert_eq!(c.col_index, 1, "col_index for {sql}");
            assert_eq!(c.op, expect_op, "op for {sql}");
            assert_eq!(
                c.literal,
                serde_json::json!(expect_lit),
                "literal for {sql}"
            );
        }

        // Unresolvable HAVING ⇒ None ⇒ 449 loud error: OR, non-output column, non-literal RHS, and
        // constant-only predicates (a bare integer is NEVER a positional column in HAVING).
        for sql in [
            "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(amount) > 1 OR region = 'x'",
            "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING COUNT(*) > 1",
            "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(amount) > region",
            "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING 1 > 5",
            "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING 1 = 1",
        ] {
            assert!(
                Executor::shard_group_aggregate_select_fanout_plan_for_statements(
                    &parse_sql(sql).unwrap()
                )
                .is_none(),
                "should be ineligible (unresolvable HAVING): {sql}"
            );
        }
    }

    #[test]
    fn group_avg_select_fanout_plan_matches_avg_shapes() {
        // (sql, rewritten_sql, group_indices, sum_index, count_index, avg_output_index)
        for (sql, rewritten, gidx, sidx, cidx, aoidx) in [
            (
                "SELECT region, AVG(amount) FROM orders GROUP BY region",
                "SELECT region, SUM(amount), COUNT(amount) FROM orders GROUP BY region",
                vec![0usize],
                1usize,
                2usize,
                1usize,
            ),
            (
                "SELECT AVG(amount), region FROM orders GROUP BY region",
                "SELECT SUM(amount), COUNT(amount), region FROM orders GROUP BY region",
                vec![2usize],
                0usize,
                1usize,
                0usize,
            ),
            (
                "SELECT region, country, AVG(amount) FROM orders GROUP BY region, country",
                "SELECT region, country, SUM(amount), COUNT(amount) FROM orders GROUP BY region, country",
                vec![0usize, 1usize],
                2usize,
                3usize,
                2usize,
            ),
        ] {
            let plan = Executor::shard_group_avg_select_fanout_plan_for_statements(
                &parse_sql(sql).unwrap(),
            )
            .unwrap_or_else(|| panic!("expected group avg plan: {sql}"));
            assert_eq!(plan.rewritten_sql, rewritten, "rewritten_sql for {sql}");
            assert_eq!(plan.group_indices, gidx, "group_indices for {sql}");
            assert_eq!(plan.sum_index, sidx, "sum_index for {sql}");
            assert_eq!(plan.count_index, cidx, "count_index for {sql}");
            assert_eq!(plan.avg_output_index, aoidx, "avg_output_index for {sql}");
            assert_eq!(plan.output_columns.len(), gidx.len() + 1, "output arity for {sql}");
        }

        // ORDER BY / LIMIT / OFFSET now eligible (post-merge): resolves to a GroupedPostMerge whose
        // per_owner_sql is the (already clause-free) rewritten SUM/COUNT query, and order keys index
        // the rebuilt AVG output layout.
        let avg_ol = Executor::shard_group_avg_select_fanout_plan_for_statements(
            &parse_sql(
                "SELECT region, AVG(amount) FROM orders GROUP BY region ORDER BY AVG(amount) DESC LIMIT 2",
            )
            .unwrap(),
        )
        .expect("group avg order/limit plan");
        let post = avg_ol.post_merge.as_ref().expect("post_merge");
        assert_eq!(
            post.order_keys
                .iter()
                .map(|k| (k.col_index, k.asc, k.nulls_first))
                .collect::<Vec<_>>(),
            vec![(1, false, true)]
        );
        assert_eq!(post.limit, Some(2));
        // AVG owners always run the clause-free rewritten SUM/COUNT query.
        assert_eq!(post.per_owner_sql, avg_ol.rewritten_sql);
        assert!(!post.per_owner_sql.to_ascii_uppercase().contains("ORDER BY"));
        assert!(!post.per_owner_sql.to_ascii_uppercase().contains("LIMIT"));

        // Ineligible: non-AVG aggregates (handled by other paths), no GROUP BY (scalar avg path),
        // FETCH (in Query.fetch → rejected), projection arity mismatch. HAVING / ORDER BY / LIMIT are
        // now eligible (post-merge).
        for sql in [
            "SELECT region, SUM(amount) FROM orders GROUP BY region",
            "SELECT region, COUNT(*) FROM orders GROUP BY region",
            "SELECT AVG(amount) FROM orders",
            "SELECT region, country, AVG(amount) FROM orders GROUP BY region",
            "SELECT region, AVG(amount) FROM orders GROUP BY region ORDER BY region FETCH FIRST 5 ROWS ONLY",
        ] {
            assert!(
                Executor::shard_group_avg_select_fanout_plan_for_statements(
                    &parse_sql(sql).unwrap()
                )
                .is_none(),
                "should be ineligible: {sql}"
            );
        }
    }

    #[test]
    fn group_multi_aggregate_select_fanout_plan_matches_shapes() {
        use super::SqlShardGroupAggregateKind::{Max, Min, Sum};
        // (sql, group_indices, aggregates as (output_index, kind))
        let cases: &[(&str, Vec<usize>, Vec<(usize, super::SqlShardGroupAggregateKind)>)] = &[
            // COUNT(*) + SUM merge independently; both add partials (kind Sum).
            (
                "SELECT region, COUNT(*), SUM(amount) FROM orders GROUP BY region",
                vec![0],
                vec![(1, Sum), (2, Sum)],
            ),
            // Mixed mergeable aggregates.
            (
                "SELECT region, SUM(amount), MIN(amount), MAX(amount) FROM orders GROUP BY region",
                vec![0],
                vec![(1, Sum), (2, Min), (3, Max)],
            ),
            // Multi-column GROUP BY.
            (
                "SELECT region, country, COUNT(*), SUM(amount) FROM orders GROUP BY region, country",
                vec![0, 1],
                vec![(2, Sum), (3, Sum)],
            ),
            // Group column interleaved with aggregates; output indices track projection order.
            (
                "SELECT COUNT(*), region, MAX(amount) FROM orders GROUP BY region",
                vec![1],
                vec![(0, Sum), (2, Max)],
            ),
            // COUNT(col) (non-null count) merges by summing partial counts.
            (
                "SELECT region, COUNT(id), SUM(amount) FROM orders GROUP BY region",
                vec![0],
                vec![(1, Sum), (2, Sum)],
            ),
        ];
        for (sql, gidx, aggs) in cases {
            let plan = Executor::shard_group_multi_aggregate_select_fanout_plan_for_statements(
                &parse_sql(sql).unwrap(),
            )
            .unwrap_or_else(|| panic!("expected multi-aggregate plan: {sql}"));
            assert_eq!(plan.group_indices, *gidx, "group_indices for {sql}");
            let got: Vec<(usize, super::SqlShardGroupAggregateKind)> = plan
                .aggregates
                .iter()
                .map(|a| (a.output_index, a.kind))
                .collect();
            assert_eq!(got, *aggs, "aggregates for {sql}");
        }

        // Ineligible: AVG (not directly mergeable here), DISTINCT aggregates, no GROUP BY, a
        // projection column that is neither a group column nor a supported aggregate.
        for sql in [
            "SELECT region, COUNT(*), AVG(amount) FROM orders GROUP BY region",
            "SELECT region, COUNT(DISTINCT amount), SUM(amount) FROM orders GROUP BY region",
            "SELECT region, SUM(DISTINCT amount), COUNT(*) FROM orders GROUP BY region",
            "SELECT region, COUNT(*), SUM(amount) FROM orders",
            "SELECT region, other, COUNT(*) FROM orders GROUP BY region",
        ] {
            assert!(
                Executor::shard_group_multi_aggregate_select_fanout_plan_for_statements(
                    &parse_sql(sql).unwrap()
                )
                .is_none(),
                "should be ineligible: {sql}"
            );
        }
    }

    #[test]
    fn join_group_aggregate_cacheability_rejects_volatile_on_predicate() {
        let cacheable =
            |sql: &str| Executor::is_query_result_cacheable_statement(&parse_sql(sql).unwrap()[0]);

        // Deterministic ON (col = col) and a compound ON (col = col AND col > literal) cache fine.
        assert!(cacheable(
            "SELECT u.city, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.city"
        ));
        assert!(cacheable(
            "SELECT u.city, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id AND o.total > 100 GROUP BY u.city"
        ));
        // A volatile function in ON must NOT be cached (its truth changes with wall-clock time).
        assert!(!cacheable(
            "SELECT u.city, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id AND o.created > NOW() GROUP BY u.city"
        ));
        // Single-table grouped aggregate still cacheable (sanity).
        assert!(cacheable("SELECT city, COUNT(*) FROM users GROUP BY city"));
    }

    #[test]
    fn statement_permissions_preserve_preallocated_entries() {
        assert_eq!(
            statement_permissions("SELECT * FROM users JOIN orders ON users.id = orders.user_id"),
            vec![
                ("users".to_string(), "SELECT"),
                ("orders".to_string(), "SELECT")
            ]
        );

        assert_eq!(
            statement_permissions(
                "SELECT * FROM (SELECT id FROM users) u JOIN orders ON u.id = orders.user_id"
            ),
            vec![
                ("users".to_string(), "SELECT"),
                ("orders".to_string(), "SELECT")
            ]
        );

        assert_eq!(
            statement_permissions("DELETE FROM users WHERE id = 1"),
            vec![("users".to_string(), "DELETE")]
        );

        assert_eq!(
            statement_permissions("TRUNCATE TABLE old_orders, old_items"),
            vec![
                ("old_orders".to_string(), "DELETE"),
                ("old_items".to_string(), "DELETE")
            ]
        );

        assert_eq!(
            statement_permissions("CREATE VIEW active_users AS SELECT id FROM users"),
            vec![
                ("active_users".to_string(), "ALL"),
                ("users".to_string(), "SELECT")
            ]
        );

        assert_eq!(
            statement_permissions(
                "COPY (SELECT * FROM users JOIN orders ON users.id = orders.user_id) TO 'out.csv'"
            ),
            vec![
                ("users".to_string(), "SELECT"),
                ("orders".to_string(), "SELECT")
            ]
        );

        assert_eq!(
            statement_permissions("DROP TABLE old_orders, old_items"),
            vec![
                ("old_orders".to_string(), "ALL"),
                ("old_items".to_string(), "ALL")
            ]
        );
    }

    #[test]
    fn sql_requires_raft_write_classifies_statement_kinds() {
        assert!(!sql_requires_raft_write("SELECT * FROM users"));
        assert!(!sql_requires_raft_write("SHOW USERS"));
        assert!(!sql_requires_raft_write("EXPLAIN SELECT * FROM users"));
        assert!(!sql_requires_raft_write(
            "EXPLAIN ANALYZE SELECT * FROM users"
        ));

        assert!(sql_requires_raft_write("INSERT INTO users VALUES (1)"));
        assert!(sql_requires_raft_write(
            "EXPLAIN ANALYZE INSERT INTO users VALUES (1)"
        ));
        assert!(sql_requires_raft_write("CREATE TABLE users (id INTEGER)"));
        assert!(sql_requires_raft_write(
            "ANALYZE TABLE users COMPUTE STATISTICS"
        ));
        assert!(sql_requires_raft_write(
            "CREATE USER alice WITH PASSWORD 'secret'"
        ));
        assert!(sql_requires_raft_write("VACUUM"));
    }

    #[test]
    fn cacheable_aggregate_function_kind_matches_without_display_string() {
        let count = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("count"))]);
        let string_agg = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("String_Agg"))]);
        let qualified = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("pg_catalog")),
            ObjectNamePart::Identifier(Ident::new("count")),
        ]);

        assert_eq!(
            cacheable_aggregate_function_kind(&count),
            Some(CacheableAggregateFunction::Count)
        );
        assert_eq!(
            cacheable_aggregate_function_kind(&string_agg),
            Some(CacheableAggregateFunction::Value)
        );
        assert_eq!(cacheable_aggregate_function_kind(&qualified), None);
    }

    #[tokio::test]
    async fn scan_routed_data_prefixes_filters_table_name_prefix_collisions() {
        let wal_path = format!("test_data_prefix_collision_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage.clone());
        let schema = TableSchema::new(
            "tenant".to_string(),
            vec![
                crate::catalog::Column {
                    name: "id".to_string(),
                    data_type: "TEXT".to_string(),
                    is_primary: true,
                    is_indexed: false,
                    index_type: crate::catalog::IndexType::None,
                    default_value: None,
                    is_nullable: false,
                    is_unique: true,
                    check_expr: None,
                },
                crate::catalog::Column {
                    name: "payload".to_string(),
                    data_type: "TEXT".to_string(),
                    is_primary: false,
                    is_indexed: false,
                    index_type: crate::catalog::IndexType::None,
                    default_value: None,
                    is_nullable: true,
                    is_unique: false,
                    check_expr: None,
                },
            ],
        );
        let tenant_key = executor.routed_data_key_for_row_id("tenant", "base:1");
        let collision_key = executor.routed_data_key_for_row_id("tenant:archive", "archive:1");
        {
            let mut txn = storage.begin_transaction().await.expect("begin txn");
            txn.put(
                b"schema:tenant",
                &bincode::serialize(&schema).expect("serialize schema"),
            )
            .await
            .expect("put schema");
            txn.put(
                collision_key.as_bytes(),
                &crate::common::encoding::RowEncoder::encode(&[
                    Value::String("archive:1".to_string()),
                    Value::String("archive".to_string()),
                ]),
            )
            .await
            .expect("put collision row");
            txn.put(
                tenant_key.as_bytes(),
                &crate::common::encoding::RowEncoder::encode(&[
                    Value::String("base:1".to_string()),
                    Value::String("base".to_string()),
                ]),
            )
            .await
            .expect("put tenant row");
            txn.commit().await.expect("commit");
        }

        let mut txn = storage.begin_transaction().await.expect("begin txn");
        let rows = executor
            .scan_routed_data_prefixes_for_table("tenant", &mut *txn, None)
            .await
            .expect("scan exact table");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, tenant_key.as_bytes());

        let limited_rows = executor
            .scan_routed_data_prefixes_for_table("tenant", &mut *txn, Some(1))
            .await
            .expect("limited scan exact table");
        assert_eq!(limited_rows.len(), 1);
        assert_eq!(limited_rows[0].0, tenant_key.as_bytes());

        let exact_count = executor
            .count_routed_data_prefixes_for_table("tenant", &mut *txn)
            .await
            .expect("count exact table");
        assert_eq!(exact_count, 1);
        drop(txn);

        let counted = executor
            .execute_sql("SELECT COUNT(*) FROM tenant")
            .await
            .expect("SQL count exact table");
        match counted.as_slice() {
            [QueryResult::Select { columns, rows }] => {
                assert_eq!(columns, &vec!["COUNT(*)".to_string()]);
                assert_eq!(rows, &vec![vec![Value::Integer(1)]]);
            }
            other => panic!("expected count result, got {other:?}"),
        }
        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn structured_data_shadow_v2_tracks_crud_and_transaction_rollback() {
        let wal_path = format!("test_structured_data_shadow_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let mut config = crate::config::StorageConfig::default();
        config.structured_data_shadow_v2 = true;
        let executor = Executor::with_config(storage.clone(), &config);

        executor
            .execute_sql(
                "CREATE TABLE shadow_rows (id TEXT PRIMARY KEY, payload TEXT); \
                 INSERT INTO shadow_rows VALUES ('row:1', 'one')",
            )
            .await
            .expect("create and insert shadow row");

        let legacy_key = executor.routed_data_key_for_row_id("shadow_rows", "row:1");
        let shadow_key = executor
            .routed_structured_data_key_for_row_id("shadow_rows", "row:1")
            .expect("encode structured row key");
        {
            let txn = storage.begin_transaction().await.expect("begin read txn");
            let legacy = txn
                .get(legacy_key.as_bytes())
                .await
                .expect("read legacy row")
                .expect("legacy row exists");
            let shadow = txn
                .get(&shadow_key)
                .await
                .expect("read structured row")
                .expect("structured row exists");
            assert_eq!(shadow, legacy);
        }

        executor
            .execute_sql("UPDATE shadow_rows SET payload = 'two' WHERE id = 'row:1'")
            .await
            .expect("update shadow row");
        {
            let txn = storage.begin_transaction().await.expect("begin read txn");
            assert_eq!(
                txn.get(legacy_key.as_bytes())
                    .await
                    .expect("read legacy row"),
                txn.get(&shadow_key)
                    .await
                    .expect("read structured row after update")
            );
        }

        let rollback = executor
            .execute_sql(
                "INSERT INTO shadow_rows VALUES ('row:2', 'rollback'); \
                 UPDATE missing_shadow_table SET payload = 'fail' WHERE id = 'row:2'",
            )
            .await;
        assert!(rollback.is_err());
        let rollback_legacy = executor.routed_data_key_for_row_id("shadow_rows", "row:2");
        let rollback_shadow = executor
            .routed_structured_data_key_for_row_id("shadow_rows", "row:2")
            .expect("encode rollback shadow key");
        {
            let txn = storage.begin_transaction().await.expect("begin read txn");
            assert!(txn
                .get(rollback_legacy.as_bytes())
                .await
                .expect("read rolled back legacy row")
                .is_none());
            assert!(txn
                .get(&rollback_shadow)
                .await
                .expect("read rolled back shadow row")
                .is_none());
        }

        executor
            .execute_sql("DELETE FROM shadow_rows WHERE id = 'row:1'")
            .await
            .expect("delete shadow row");
        {
            let txn = storage.begin_transaction().await.expect("begin read txn");
            assert!(txn
                .get(legacy_key.as_bytes())
                .await
                .expect("read deleted legacy row")
                .is_none());
            assert!(txn
                .get(&shadow_key)
                .await
                .expect("read deleted structured row")
                .is_none());
        }

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn structured_data_shadow_is_opt_in_and_table_cleanup_removes_orphans() {
        let wal_path = format!("test_structured_data_cleanup_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage.clone());

        executor
            .execute_sql(
                "CREATE TABLE shadow_cleanup (id TEXT PRIMARY KEY, payload TEXT); \
                 INSERT INTO shadow_cleanup VALUES ('row:1', 'one')",
            )
            .await
            .expect("create cleanup table");
        let shadow_key = executor
            .routed_structured_data_key_for_row_id("shadow_cleanup", "row:1")
            .expect("encode structured row key");
        let historical_shard_key = crate::storage::keyspace::encode_data_key(
            crate::storage::keyspace::DataRoute::Shard(99),
            b"shadow_cleanup",
            b"row:historical",
        )
        .expect("encode historical shard key");
        let neighboring_table_key = crate::storage::keyspace::encode_data_key(
            crate::storage::keyspace::DataRoute::Shard(99),
            b"shadow_cleanup:archive",
            b"row:neighbor",
        )
        .expect("encode neighboring table key");
        {
            let txn = storage.begin_transaction().await.expect("begin read txn");
            assert!(txn
                .get(&shadow_key)
                .await
                .expect("read disabled shadow")
                .is_none());
        }

        {
            let mut txn = storage.begin_transaction().await.expect("begin orphan txn");
            txn.put(&shadow_key, b"orphan")
                .await
                .expect("put structured orphan");
            txn.put(&historical_shard_key, b"historical orphan")
                .await
                .expect("put historical shard orphan");
            txn.put(&neighboring_table_key, b"neighbor")
                .await
                .expect("put neighboring table shadow");
            txn.commit().await.expect("commit orphan");
        }
        executor
            .execute_sql("TRUNCATE TABLE shadow_cleanup")
            .await
            .expect("truncate cleanup table");
        {
            let txn = storage.begin_transaction().await.expect("begin read txn");
            assert!(txn
                .get(&shadow_key)
                .await
                .expect("read truncated shadow")
                .is_none());
            assert!(txn
                .get(&historical_shard_key)
                .await
                .expect("read historical shard shadow")
                .is_none());
            assert_eq!(
                txn.get(&neighboring_table_key)
                    .await
                    .expect("read neighboring table shadow"),
                Some(b"neighbor".to_vec())
            );
        }

        {
            let mut txn = storage.begin_transaction().await.expect("begin orphan txn");
            txn.put(&shadow_key, b"orphan")
                .await
                .expect("put structured orphan before drop");
            txn.put(&historical_shard_key, b"historical orphan")
                .await
                .expect("put historical shard orphan before drop");
            txn.commit().await.expect("commit orphan before drop");
        }
        executor
            .execute_sql("DROP TABLE shadow_cleanup")
            .await
            .expect("drop cleanup table");
        {
            let txn = storage.begin_transaction().await.expect("begin read txn");
            assert!(txn
                .get(&shadow_key)
                .await
                .expect("read dropped shadow")
                .is_none());
            assert!(txn
                .get(&historical_shard_key)
                .await
                .expect("read dropped historical shard shadow")
                .is_none());
            assert_eq!(
                txn.get(&neighboring_table_key)
                    .await
                    .expect("read neighboring table shadow after drop"),
                Some(b"neighbor".to_vec())
            );
        }

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn structured_data_shadow_cleanup_survives_fusion_flush_and_reopen() {
        let data_dir = std::env::temp_dir().join(format!(
            "fusiondb_structured_shadow_reopen_{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        let mut config = crate::config::StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        config.structured_data_shadow_v2 = true;
        let wal_path = config.wal_path();

        let shadow_key = {
            let fusion =
                crate::storage::FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                    .await
                    .expect("open FusionStorage");
            let storage: Arc<dyn Storage> = Arc::new(fusion.clone());
            let executor = Executor::with_config(storage.clone(), &config);
            executor
                .execute_sql(
                    "CREATE TABLE shadow_reopen (id TEXT PRIMARY KEY, payload TEXT); \
                     INSERT INTO shadow_reopen VALUES ('row:1', 'one')",
                )
                .await
                .expect("create persisted shadow");
            let shadow_key = executor
                .routed_structured_data_key_for_row_id("shadow_reopen", "row:1")
                .expect("encode persisted shadow key");
            let txn = storage.begin_transaction().await.expect("begin read txn");
            assert!(txn
                .get(&shadow_key)
                .await
                .expect("read persisted shadow")
                .is_some());
            drop(txn);
            fusion
                .create_snapshot_now()
                .await
                .expect("flush persisted shadow");
            shadow_key
        };

        {
            let fusion =
                crate::storage::FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                    .await
                    .expect("reopen FusionStorage");
            let storage: Arc<dyn Storage> = Arc::new(fusion.clone());
            let executor = Executor::with_config(storage.clone(), &config);
            let txn = storage.begin_transaction().await.expect("begin read txn");
            assert!(txn
                .get(&shadow_key)
                .await
                .expect("read reopened shadow")
                .is_some());
            drop(txn);
            executor
                .execute_sql("TRUNCATE TABLE shadow_reopen")
                .await
                .expect("truncate reopened shadow table");
            fusion
                .create_snapshot_now()
                .await
                .expect("flush shadow deletion");
        }

        {
            let fusion =
                crate::storage::FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                    .await
                    .expect("reopen after shadow cleanup");
            let storage: Arc<dyn Storage> = Arc::new(fusion);
            let txn = storage.begin_transaction().await.expect("begin read txn");
            assert!(txn
                .get(&shadow_key)
                .await
                .expect("read cleaned shadow")
                .is_none());
        }

        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn sharded_executor_uses_physical_shard_data_keys_for_crud() {
        let wal_path = format!("test_sharded_executor_crud_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let config = sharded_test_config();
        let shard_router =
            crate::distributed::sharding::ShardRouter::from_config(&config).expect("router");
        let mut storage_config = crate::config::StorageConfig::default();
        storage_config.structured_data_shadow_v2 = true;
        let executor = Executor::with_config_and_shard_router(
            storage.clone(),
            &storage_config,
            Some(shard_router),
        );

        executor
            .execute_sql(
                "CREATE TABLE sharded_users (id INTEGER PRIMARY KEY, name TEXT); \
                 INSERT INTO sharded_users VALUES (1, 'alice'); \
                 INSERT INTO sharded_users VALUES (2, 'bob')",
            )
            .await
            .expect("create and insert");

        let first_row_id = crate::common::encoding::encode_i64_comparable(1);
        let first_sharded_key = executor.routed_data_key_for_row_id("sharded_users", &first_row_id);
        let first_shadow_key = executor
            .routed_structured_data_key_for_row_id("sharded_users", &first_row_id)
            .expect("encode sharded shadow key");
        {
            let txn = storage.begin_transaction().await.expect("begin txn");
            let legacy = txn
                .get(first_sharded_key.as_bytes())
                .await
                .expect("get sharded key");
            assert!(legacy.is_some());
            assert_eq!(
                txn.get(&first_shadow_key)
                    .await
                    .expect("get sharded shadow key"),
                legacy
            );
            assert!(txn
                .get(b"data:sharded_users:10000000000000001")
                .await
                .expect("get legacy key")
                .is_none());
        }

        let selected = executor
            .execute_sql("SELECT name FROM sharded_users WHERE id = 1")
            .await
            .expect("select");
        match selected.as_slice() {
            [QueryResult::Select { columns, rows }] => {
                assert_eq!(columns, &vec!["name".to_string()]);
                assert_eq!(rows, &vec![vec![Value::String("alice".to_string())]]);
            }
            other => panic!("expected select result, got {other:?}"),
        }

        executor
            .execute_sql("UPDATE sharded_users SET name = 'carol' WHERE id = 1")
            .await
            .expect("update");
        let counted = executor
            .execute_sql("SELECT COUNT(*) FROM sharded_users")
            .await
            .expect("count");
        match counted.as_slice() {
            [QueryResult::Select { columns, rows }] => {
                assert_eq!(columns, &vec!["COUNT(*)".to_string()]);
                assert_eq!(rows, &vec![vec![Value::Integer(2)]]);
            }
            other => panic!("expected count result, got {other:?}"),
        }

        let analyzed = executor
            .execute_sql("ANALYZE TABLE sharded_users COMPUTE STATISTICS")
            .await
            .expect("analyze");
        match analyzed.as_slice() {
            [QueryResult::Success { message }] => {
                assert!(message.contains("2 rows"));
            }
            other => panic!("expected analyze success, got {other:?}"),
        }

        executor
            .execute_sql("CREATE INDEX idx_sharded_users_name ON sharded_users (name)")
            .await
            .expect("create index");
        let second_row_id = crate::common::encoding::encode_i64_comparable(2);
        let bob_index_key =
            executor.routed_index_key_for_value("sharded_users", "name", "bob", &second_row_id);
        let legacy_bob_index_key = format!("index:sharded_users:name:bob:{second_row_id}");
        {
            let txn = storage.begin_transaction().await.expect("begin txn");
            assert!(txn
                .get(bob_index_key.as_bytes())
                .await
                .expect("get routed index key")
                .is_some());
            assert!(txn
                .get(legacy_bob_index_key.as_bytes())
                .await
                .expect("get legacy index key")
                .is_none());
        }

        let index_lookup = executor
            .execute_sql("SELECT id FROM sharded_users WHERE name = 'bob'")
            .await
            .expect("secondary index lookup");
        match index_lookup.as_slice() {
            [QueryResult::Select { columns, rows }] => {
                assert_eq!(columns, &vec!["id".to_string()]);
                assert_eq!(rows, &vec![vec![Value::Integer(2)]]);
            }
            other => panic!("expected indexed select result, got {other:?}"),
        }

        executor
            .execute_sql("UPDATE sharded_users SET name = 'dave' WHERE id = 2")
            .await
            .expect("indexed update");
        let dave_index_key =
            executor.routed_index_key_for_value("sharded_users", "name", "dave", &second_row_id);
        {
            let txn = storage.begin_transaction().await.expect("begin txn");
            assert!(txn
                .get(bob_index_key.as_bytes())
                .await
                .expect("get old routed index key")
                .is_none());
            assert!(txn
                .get(dave_index_key.as_bytes())
                .await
                .expect("get new routed index key")
                .is_some());
        }
        let updated_index_lookup = executor
            .execute_sql("SELECT id FROM sharded_users WHERE name = 'dave'")
            .await
            .expect("updated secondary index lookup");
        match updated_index_lookup.as_slice() {
            [QueryResult::Select { columns, rows }] => {
                assert_eq!(columns, &vec!["id".to_string()]);
                assert_eq!(rows, &vec![vec![Value::Integer(2)]]);
            }
            other => panic!("expected updated indexed select result, got {other:?}"),
        }

        executor
            .execute_sql(
                "CREATE TABLE sharded_docs (id INTEGER PRIMARY KEY, body TEXT); \
                 INSERT INTO sharded_docs VALUES (1, 'quick brown fox'), (2, 'quick blue hare'); \
                 CREATE INDEX idx_sharded_docs_body ON sharded_docs (body) USING FTS",
            )
            .await
            .expect("create fts index");
        let doc_row_id = crate::common::encoding::encode_i64_comparable(1);
        let fts_index_key =
            executor.routed_fts_index_key_for_row("sharded_docs", "body", "quick", &doc_row_id);
        let legacy_fts_index_key = format!("fts:sharded_docs:body:quick:{doc_row_id}");
        {
            let txn = storage.begin_transaction().await.expect("begin txn");
            assert!(txn
                .get(fts_index_key.as_bytes())
                .await
                .expect("get routed fts key")
                .is_some());
            assert!(txn
                .get(legacy_fts_index_key.as_bytes())
                .await
                .expect("get legacy fts key")
                .is_none());
        }
        let fts_lookup = executor
            .execute_sql("SELECT id FROM sharded_docs WHERE MATCH(body) AGAINST('quick fox')")
            .await
            .expect("fts lookup");
        match fts_lookup.as_slice() {
            [QueryResult::Select { columns, rows }] => {
                assert_eq!(columns, &vec!["id".to_string()]);
                assert_eq!(rows, &vec![vec![Value::Integer(1)]]);
            }
            other => panic!("expected fts select result, got {other:?}"),
        }

        executor
            .execute_sql(
                "CREATE TABLE sharded_orders (id INTEGER PRIMARY KEY, region TEXT, status TEXT); \
                 INSERT INTO sharded_orders VALUES (1, 'west', 'open'); \
                 INSERT INTO sharded_orders VALUES (2, 'west', 'closed'); \
                 CREATE INDEX idx_sharded_orders_region_status ON sharded_orders (region, status)",
            )
            .await
            .expect("create composite index");
        let composite_lookup = executor
            .execute_sql("SELECT id FROM sharded_orders WHERE region = 'west' AND status = 'open'")
            .await
            .expect("composite index lookup");
        match composite_lookup.as_slice() {
            [QueryResult::Select { columns, rows }] => {
                assert_eq!(columns, &vec!["id".to_string()]);
                assert_eq!(rows, &vec![vec![Value::Integer(1)]]);
            }
            other => panic!("expected composite index select result, got {other:?}"),
        }

        executor
            .execute_sql("DELETE FROM sharded_users WHERE id = 1")
            .await
            .expect("delete");
        let remaining = executor
            .execute_sql("SELECT name FROM sharded_users WHERE id = 1")
            .await
            .expect("select deleted");
        match remaining.as_slice() {
            [QueryResult::Select { columns, rows }] => {
                assert_eq!(columns, &vec!["name".to_string()]);
                assert!(rows.is_empty());
            }
            other => panic!("expected empty select result, got {other:?}"),
        }
        {
            let txn = storage.begin_transaction().await.expect("begin txn");
            assert!(txn
                .get(&first_shadow_key)
                .await
                .expect("get deleted sharded shadow key")
                .is_none());
        }

        let _ = std::fs::remove_file(wal_path);
    }

    /// A cache entry whose encoded bytes differ from the bytes resolved by
    /// the current read must be ignored (BENCHPROD-463: the pre-fix cache
    /// validated nothing, so a stale entry poisoned every later reader).
    #[tokio::test]
    async fn row_cache_hit_requires_byte_identity() {
        let wal_path = format!("test_row_cache_bytes_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage);
        executor
            .execute_sql("CREATE TABLE rc_bytes (id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .expect("create table");
        executor
            .execute_sql("INSERT INTO rc_bytes VALUES (1, 'truth')")
            .await
            .expect("insert");

        let row_id = Executor::value_to_primary_row_id(&Value::Integer(1)).expect("row id");
        let key = executor.routed_data_key_for_row_id("rc_bytes", &row_id);
        executor.row_cache.insert(
            key,
            CachedRow {
                encoded: Arc::from(&b"stale-version-bytes"[..]),
                row: vec![Value::Integer(1), Value::String("poison".to_string())],
            },
        );

        let results = executor
            .execute_sql("SELECT v FROM rc_bytes WHERE id = 1")
            .await
            .expect("select");
        match results.as_slice() {
            [QueryResult::Select { rows, .. }] => {
                assert_eq!(rows, &vec![vec![Value::String("truth".to_string())]]);
            }
            other => panic!("expected select result, got {other:?}"),
        }

        let _ = std::fs::remove_file(wal_path);
    }

    /// A transaction pinned to an older MVCC snapshot must not observe a
    /// newer row version through the row cache (BENCHPROD-463: the pre-fix
    /// cache returned whatever version was cached last, breaking snapshot
    /// isolation for explicit transactions).
    #[tokio::test]
    async fn row_cache_does_not_leak_newer_version_into_snapshot() {
        let data_dir =
            std::path::PathBuf::from(format!("test_row_cache_snap_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = crate::config::StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let fusion = crate::storage::fusion::FusionStorage::with_config(
            &wal_path.to_string_lossy(),
            &config,
        )
        .await
        .expect("fusion storage");
        let storage: Arc<dyn Storage> = Arc::new(fusion);
        let executor = Executor::new(storage.clone());

        executor
            .execute_sql("CREATE TABLE rc_snap (id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .expect("create table");
        executor
            .execute_sql("INSERT INTO rc_snap VALUES (1, 'old')")
            .await
            .expect("insert");

        let mut old_snapshot_txn = storage.begin_transaction().await.expect("begin txn");

        executor
            .execute_sql("UPDATE rc_snap SET v = 'new' WHERE id = 1")
            .await
            .expect("update");
        executor
            .execute_sql("SELECT * FROM rc_snap WHERE id = 1")
            .await
            .expect("warm cache with new version");

        let statements = parse_sql("SELECT v FROM rc_snap WHERE id = 1").expect("parse");
        let result = executor
            .execute_in_transaction(&statements[0], &mut *old_snapshot_txn)
            .await
            .expect("snapshot select");
        match result {
            QueryResult::Select { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::String("old".to_string())]]);
            }
            other => panic!("expected select result, got {other:?}"),
        }
        old_snapshot_txn.rollback().await.expect("rollback");

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Two concurrent transactions inserting the same UNIQUE value must not
    /// both commit (BENCHPROD-464): the scan-based duplicate check cannot see
    /// the other uncommitted row, but both stage the same row-id-free unique
    /// sentinel key, so exact-key OCC validation aborts the loser.
    #[tokio::test]
    async fn concurrent_unique_inserts_collide_on_sentinel() {
        let data_dir =
            std::path::PathBuf::from(format!("test_unique_sentinel_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = crate::config::StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let fusion = crate::storage::fusion::FusionStorage::with_config(
            &wal_path.to_string_lossy(),
            &config,
        )
        .await
        .expect("fusion storage");
        let storage: Arc<dyn Storage> = Arc::new(fusion);
        let executor = Executor::new(storage.clone());

        executor
            .execute_sql("CREATE TABLE uniq_race (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
            .await
            .expect("create table");

        let insert_a = parse_sql("INSERT INTO uniq_race VALUES (1, 'dup@x')").expect("parse a");
        let insert_b = parse_sql("INSERT INTO uniq_race VALUES (2, 'dup@x')").expect("parse b");

        let mut txn_a = storage.begin_transaction().await.expect("begin a");
        let mut txn_b = storage.begin_transaction().await.expect("begin b");

        executor
            .execute_in_transaction(&insert_a[0], &mut *txn_a)
            .await
            .expect("stage a");
        // txn_b's scan-based duplicate check cannot see txn_a's uncommitted row.
        executor
            .execute_in_transaction(&insert_b[0], &mut *txn_b)
            .await
            .expect("stage b");

        txn_a.commit().await.expect("commit a");
        let err = txn_b
            .commit()
            .await
            .expect_err("second same-value insert must abort at OCC validation");
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("conflict"),
            "expected a write-conflict abort, got: {msg}"
        );

        // Exactly one row must be visible.
        let results = executor
            .execute_sql("SELECT COUNT(*) FROM uniq_race")
            .await
            .expect("count");
        match results.as_slice() {
            [QueryResult::Select { rows, .. }] => {
                assert_eq!(rows, &vec![vec![Value::Integer(1)]]);
            }
            other => panic!("expected select result, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// The sentinel must not linger as a false conflict: DELETE tombstones it
    /// and a later INSERT of the same value succeeds.
    #[tokio::test]
    async fn unique_sentinel_released_after_delete() {
        let wal_path = format!("test_unique_release_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage);

        executor
            .execute_sql("CREATE TABLE uniq_cycle (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
            .await
            .expect("create table");
        executor
            .execute_sql("INSERT INTO uniq_cycle VALUES (1, 'a@x')")
            .await
            .expect("insert");
        executor
            .execute_sql("DELETE FROM uniq_cycle WHERE id = 1")
            .await
            .expect("delete");
        executor
            .execute_sql("INSERT INTO uniq_cycle VALUES (2, 'a@x')")
            .await
            .expect("re-insert of a deleted unique value must succeed");

        let err = executor
            .execute_sql("INSERT INTO uniq_cycle VALUES (3, 'a@x')")
            .await
            .expect_err("live duplicate must still be rejected");
        assert!(err.to_string().contains("UNIQUE"));

        let _ = std::fs::remove_file(wal_path);
    }

    /// Pins the UPDATE half of the sentinel mechanism: a concurrent UPDATE
    /// migrating a row onto value v and an INSERT of v stage the same
    /// sentinel, so the second committer aborts.
    #[tokio::test]
    async fn concurrent_update_and_insert_collide_on_sentinel() {
        let data_dir =
            std::path::PathBuf::from(format!("test_unique_upd_race_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = crate::config::StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let fusion = crate::storage::fusion::FusionStorage::with_config(
            &wal_path.to_string_lossy(),
            &config,
        )
        .await
        .expect("fusion storage");
        let storage: Arc<dyn Storage> = Arc::new(fusion);
        let executor = Executor::new(storage.clone());

        executor
            .execute_sql("CREATE TABLE uniq_upd (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
            .await
            .expect("create table");
        executor
            .execute_sql("INSERT INTO uniq_upd VALUES (1, 'old@x')")
            .await
            .expect("seed row");

        let update = parse_sql("UPDATE uniq_upd SET email = 'new@x' WHERE id = 1").unwrap();
        let insert = parse_sql("INSERT INTO uniq_upd VALUES (2, 'new@x')").unwrap();

        let mut txn_a = storage.begin_transaction().await.expect("begin a");
        let mut txn_b = storage.begin_transaction().await.expect("begin b");
        executor
            .execute_in_transaction(&update[0], &mut *txn_a)
            .await
            .expect("stage update");
        executor
            .execute_in_transaction(&insert[0], &mut *txn_b)
            .await
            .expect("stage insert");

        txn_a.commit().await.expect("commit update");
        let err = txn_b
            .commit()
            .await
            .expect_err("insert of the migrated value must abort");
        assert!(err.to_string().to_lowercase().contains("conflict"));

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Pins the hash-fallback sentinel encoding: FLOAT UNIQUE columns have no
    /// index string, so without the fallback both concurrent inserts of the
    /// same float committed (live-reproduced during review).
    #[tokio::test]
    async fn concurrent_float_unique_inserts_collide_on_sentinel() {
        let data_dir =
            std::path::PathBuf::from(format!("test_unique_float_race_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = crate::config::StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let fusion = crate::storage::fusion::FusionStorage::with_config(
            &wal_path.to_string_lossy(),
            &config,
        )
        .await
        .expect("fusion storage");
        let storage: Arc<dyn Storage> = Arc::new(fusion);
        let executor = Executor::new(storage.clone());

        executor
            .execute_sql("CREATE TABLE uniq_float (id INTEGER PRIMARY KEY, score DOUBLE UNIQUE)")
            .await
            .expect("create table");

        let insert_a = parse_sql("INSERT INTO uniq_float VALUES (1, 1.5)").unwrap();
        let insert_b = parse_sql("INSERT INTO uniq_float VALUES (2, 1.5)").unwrap();

        let mut txn_a = storage.begin_transaction().await.expect("begin a");
        let mut txn_b = storage.begin_transaction().await.expect("begin b");
        executor
            .execute_in_transaction(&insert_a[0], &mut *txn_a)
            .await
            .expect("stage a");
        executor
            .execute_in_transaction(&insert_b[0], &mut *txn_b)
            .await
            .expect("stage b");

        txn_a.commit().await.expect("commit a");
        let err = txn_b
            .commit()
            .await
            .expect_err("second same-float insert must abort at OCC validation");
        assert!(err.to_string().to_lowercase().contains("conflict"));

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    // ---- Data V2 shadow cleanup: bounded route skip-scan (P10-2.2) ----

    /// Counts the keys a scan actually hands back, so a test can assert that
    /// per-table cleanup cost tracks the table's own rows and the number of
    /// routes present — not the size of the whole Data V2 namespace.
    struct CountingTransaction {
        inner: Box<dyn Transaction>,
        keys_seen: Arc<AtomicU64>,
    }

    #[async_trait::async_trait]
    impl Transaction for CountingTransaction {
        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.inner.get(key).await
        }
        async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
            self.inner.put(key, value).await
        }
        async fn delete(&mut self, key: &[u8]) -> Result<()> {
            self.inner.delete(key).await
        }
        async fn scan_prefix(
            &self,
            prefix: &[u8],
            limit: Option<usize>,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let rows = self.inner.scan_prefix(prefix, limit).await?;
            self.keys_seen
                .fetch_add(rows.len() as u64, AtomicOrdering::Relaxed);
            Ok(rows)
        }
        async fn scan_prefix_for_each(
            &self,
            prefix: &[u8],
            limit: Option<usize>,
            visitor: &mut dyn ScanVisitor,
        ) -> Result<usize> {
            let visited = self
                .inner
                .scan_prefix_for_each(prefix, limit, visitor)
                .await?;
            self.keys_seen
                .fetch_add(visited as u64, AtomicOrdering::Relaxed);
            Ok(visited)
        }
        async fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            limit: Option<usize>,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let rows = self.inner.scan_range(start, end, limit).await?;
            self.keys_seen
                .fetch_add(rows.len() as u64, AtomicOrdering::Relaxed);
            Ok(rows)
        }
        async fn scan_range_for_each(
            &self,
            start: &[u8],
            end: &[u8],
            limit: Option<usize>,
            visitor: &mut dyn ScanVisitor,
        ) -> Result<usize> {
            let visited = self
                .inner
                .scan_range_for_each(start, end, limit, visitor)
                .await?;
            self.keys_seen
                .fetch_add(visited as u64, AtomicOrdering::Relaxed);
            Ok(visited)
        }
        // Forward the parallel streaming variants instead of inheriting the
        // trait defaults: the defaults materialize, which would make this
        // double report a streaming scan as a full read and mask exactly the
        // early-stop behaviour it exists to measure.
        async fn scan_prefix_parallel_for_each_with_options(
            &self,
            prefix: &[u8],
            limit: Option<usize>,
            visitor: &mut dyn ScanVisitor,
            options: StorageScanOptions,
        ) -> Result<Option<usize>> {
            let visited = self
                .inner
                .scan_prefix_parallel_for_each_with_options(prefix, limit, visitor, options)
                .await?;
            if let Some(visited) = visited {
                self.keys_seen
                    .fetch_add(visited as u64, AtomicOrdering::Relaxed);
            }
            Ok(visited)
        }
        async fn scan_prefix_for_each_with_options(
            &self,
            prefix: &[u8],
            limit: Option<usize>,
            visitor: &mut dyn ScanVisitor,
            options: StorageScanOptions,
        ) -> Result<usize> {
            let visited = self
                .inner
                .scan_prefix_for_each_with_options(prefix, limit, visitor, options)
                .await?;
            self.keys_seen
                .fetch_add(visited as u64, AtomicOrdering::Relaxed);
            Ok(visited)
        }
        async fn scan_range_reverse_for_each(
            &self,
            start: &[u8],
            end: &[u8],
            limit: Option<usize>,
            visitor: &mut dyn ScanVisitor,
        ) -> Result<usize> {
            let visited = self
                .inner
                .scan_range_reverse_for_each(start, end, limit, visitor)
                .await?;
            self.keys_seen
                .fetch_add(visited as u64, AtomicOrdering::Relaxed);
            Ok(visited)
        }
        fn supports_bounded_scan_range_reverse(&self) -> bool {
            self.inner.supports_bounded_scan_range_reverse()
        }
        async fn count_prefix(&self, prefix: &[u8]) -> Result<usize> {
            self.inner.count_prefix(prefix).await
        }
        async fn first(&self, start: &[u8], end: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
            self.inner.first(start, end).await
        }
        async fn last(&self, start: &[u8], end: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
            self.inner.last(start, end).await
        }
        async fn commit(self: Box<Self>) -> Result<()> {
            self.inner.commit().await
        }
        async fn rollback(self: Box<Self>) -> Result<()> {
            self.inner.rollback().await
        }
        // Delegate rather than inherit the trait defaults: a test double that
        // silently drops the P10-2.1 migration fence would let this test pass
        // while exercising a path the production code never takes.
        async fn fence_data_migration_phase(&mut self, phase: u8, phase_seq: u64) -> Result<()> {
            self.inner
                .fence_data_migration_phase(phase, phase_seq)
                .await
        }
        fn data_migration_phase_pin(&self) -> Option<(u8, u64)> {
            self.inner.data_migration_phase_pin()
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self.inner.as_any()
        }
    }

    /// The cleanup must scale with the target table and the number of routes
    /// present, not with the size of the Data V2 namespace. Once backfill
    /// (P10-2.3) fills that namespace, a full scan would make every
    /// DROP/TRUNCATE O(all shadow rows).
    #[tokio::test]
    async fn structured_shadow_cleanup_cost_is_bounded_by_table_not_namespace() {
        let wal_path = format!("test_shadow_cleanup_bounded_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage.clone());

        // Two routes present, one target row each; the namespace is otherwise
        // filled with unrelated tables' shadows.
        const NOISE_TABLES: usize = 40;
        const NOISE_ROWS_PER_TABLE: usize = 25;
        let mut txn = storage.begin_transaction().await.unwrap();
        for route in [
            crate::storage::keyspace::DataRoute::Unsharded,
            crate::storage::keyspace::DataRoute::Shard(99),
        ] {
            txn.put(
                &crate::storage::keyspace::encode_data_key(route, b"target", b"row:1").unwrap(),
                b"target row",
            )
            .await
            .unwrap();
            for table in 0..NOISE_TABLES {
                let name = format!("noise_table_{table:03}");
                for row in 0..NOISE_ROWS_PER_TABLE {
                    txn.put(
                        &crate::storage::keyspace::encode_data_key(
                            route,
                            name.as_bytes(),
                            format!("row:{row:03}").as_bytes(),
                        )
                        .unwrap(),
                        b"noise",
                    )
                    .await
                    .unwrap();
                }
            }
        }
        txn.commit().await.unwrap();
        let namespace_rows = 2 * (1 + NOISE_TABLES * NOISE_ROWS_PER_TABLE);

        let keys_seen = Arc::new(AtomicU64::new(0));
        let mut counting: Box<dyn Transaction> = Box::new(CountingTransaction {
            inner: storage.begin_transaction().await.unwrap(),
            keys_seen: keys_seen.clone(),
        });
        executor
            .delete_structured_data_shadows_for_table("target", &mut *counting)
            .await
            .unwrap();
        counting.commit().await.unwrap();

        // Two route probes (1 key each) plus the two target rows. The bound
        // that matters: nowhere near the namespace size.
        let seen = keys_seen.load(AtomicOrdering::Relaxed);
        assert!(
            seen <= 8,
            "cleanup visited {seen} keys with {namespace_rows} rows in the namespace; \
             it must scale with routes + target rows, not namespace size"
        );

        // Correctness is unchanged: both target rows gone, all noise intact.
        let txn = storage.begin_transaction().await.unwrap();
        for route in [
            crate::storage::keyspace::DataRoute::Unsharded,
            crate::storage::keyspace::DataRoute::Shard(99),
        ] {
            assert!(txn
                .get(
                    &crate::storage::keyspace::encode_data_key(route, b"target", b"row:1").unwrap()
                )
                .await
                .unwrap()
                .is_none());
        }
        let survivors = txn
            .scan_prefix(&crate::storage::keyspace::data_namespace_prefix(), None)
            .await
            .unwrap();
        assert_eq!(survivors.len(), namespace_rows - 2);

        let _ = std::fs::remove_file(wal_path);
    }

    /// A limited table scan must stop the storage scan itself, not read the
    /// whole table and slice the result. Passing `None` down and truncating
    /// afterwards made `ORDER BY <pk> LIMIT n` read every block of the table.
    ///
    /// Covers the serial streaming path: MemoryStorage implements no parallel
    /// scan, and 400 rows is below `PARALLEL_SCAN_MIN_ROWS` anyway. The
    /// parallel branch's own stop-early and ordering behaviour is pinned by
    /// `scan_range_parallel_for_each_*` tests in `storage::fusion`.
    #[tokio::test]
    async fn limited_table_scan_stops_the_storage_scan_early() {
        let wal_path = format!("test_limited_scan_early_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage.clone());
        executor
            .execute_sql("CREATE TABLE lim_rows (id INTEGER PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();
        const ROWS: usize = 400;
        for id in 0..ROWS {
            executor
                .execute_sql(&format!("INSERT INTO lim_rows VALUES ({id}, 'p{id}')"))
                .await
                .unwrap();
        }

        let keys_seen = Arc::new(AtomicU64::new(0));
        let mut counting: Box<dyn Transaction> = Box::new(CountingTransaction {
            inner: storage.begin_transaction().await.unwrap(),
            keys_seen: keys_seen.clone(),
        });
        let rows = executor
            .scan_routed_data_prefixes_for_table_with_options(
                "lim_rows",
                &mut *counting,
                Some(10),
                StorageScanOptions::fill_cache(),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 10, "the limit must still bound the result");
        let seen = keys_seen.load(AtomicOrdering::Relaxed);
        assert!(
            seen < 50,
            "storage handed back {seen} keys for a LIMIT 10 scan over {ROWS} rows; \
             the limit is not reaching the scan"
        );

        // Unlimited scans must still see everything.
        let keys_seen_all = Arc::new(AtomicU64::new(0));
        let mut counting_all: Box<dyn Transaction> = Box::new(CountingTransaction {
            inner: storage.begin_transaction().await.unwrap(),
            keys_seen: keys_seen_all.clone(),
        });
        let all = executor
            .scan_routed_data_prefixes_for_table_with_options(
                "lim_rows",
                &mut *counting_all,
                None,
                StorageScanOptions::fill_cache(),
            )
            .await
            .unwrap();
        assert_eq!(all.len(), ROWS);

        let _ = std::fs::remove_file(wal_path);
    }

    /// `ORDER BY <pk> DESC LIMIT n` must walk the key order backward and
    /// stop after n accepted rows — not feed the whole table through the
    /// top-K heap.
    #[tokio::test]
    async fn pk_desc_limit_uses_a_bounded_reverse_scan() {
        let (executor, storage, data_dir) = fusion_executor("pk_desc_limit").await;
        executor
            .execute_sql("CREATE TABLE d_rows (id INTEGER PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();
        for id in 0..400 {
            executor
                .execute_sql(&format!("INSERT INTO d_rows VALUES ({id}, 'p{id}')"))
                .await
                .unwrap();
        }

        let keys_seen = Arc::new(AtomicU64::new(0));
        let mut counting: Box<dyn Transaction> = Box::new(CountingTransaction {
            inner: storage.begin_transaction().await.unwrap(),
            keys_seen: keys_seen.clone(),
        });
        let stmt = executor
            .prepare("SELECT id FROM d_rows ORDER BY id DESC LIMIT 10")
            .unwrap()
            .remove(0);
        let result = executor
            .execute_in_transaction(&stmt, &mut *counting)
            .await
            .unwrap();
        let QueryResult::Select { rows, .. } = result else {
            panic!("select returns rows");
        };
        assert_eq!(
            rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
            (390..400).rev().map(Value::Integer).collect::<Vec<_>>(),
            "descending PK order with the correct window"
        );
        let seen = keys_seen.load(AtomicOrdering::Relaxed);
        assert!(
            seen < 50,
            "storage handed back {seen} keys for a DESC LIMIT 10 over 400 rows;              the reverse scan is not early-stopping"
        );

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Route discovery must find every route physically present — including
    /// shard ids the current router would never enumerate — and must not
    /// touch a table whose name merely shares a prefix.
    #[tokio::test]
    async fn structured_shadow_cleanup_finds_all_historical_routes() {
        let wal_path = format!("test_shadow_cleanup_routes_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage.clone());

        let routes = [
            crate::storage::keyspace::DataRoute::Unsharded,
            crate::storage::keyspace::DataRoute::Shard(0),
            crate::storage::keyspace::DataRoute::Shard(7),
            crate::storage::keyspace::DataRoute::Shard(u64::MAX),
        ];
        let mut txn = storage.begin_transaction().await.unwrap();
        for (index, route) in routes.iter().enumerate() {
            txn.put(
                &crate::storage::keyspace::encode_data_key(
                    *route,
                    b"orders",
                    format!("row:{index}").as_bytes(),
                )
                .unwrap(),
                b"target",
            )
            .await
            .unwrap();
            // Prefix-sharing neighbors that must survive.
            for neighbor in [b"orders:archive".as_slice(), b"orders_2".as_slice()] {
                txn.put(
                    &crate::storage::keyspace::encode_data_key(*route, neighbor, b"row:n").unwrap(),
                    b"neighbor",
                )
                .await
                .unwrap();
            }
        }
        txn.commit().await.unwrap();

        let mut txn = storage.begin_transaction().await.unwrap();
        executor
            .delete_structured_data_shadows_for_table("orders", &mut *txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = storage.begin_transaction().await.unwrap();
        for (index, route) in routes.iter().enumerate() {
            assert!(
                txn.get(
                    &crate::storage::keyspace::encode_data_key(
                        *route,
                        b"orders",
                        format!("row:{index}").as_bytes()
                    )
                    .unwrap()
                )
                .await
                .unwrap()
                .is_none(),
                "route {route:?} target row survived cleanup"
            );
            for neighbor in [b"orders:archive".as_slice(), b"orders_2".as_slice()] {
                assert!(
                    txn.get(
                        &crate::storage::keyspace::encode_data_key(*route, neighbor, b"row:n")
                            .unwrap()
                    )
                    .await
                    .unwrap()
                    .is_some(),
                    "prefix-sharing neighbor was wrongly deleted in route {route:?}"
                );
            }
        }

        let _ = std::fs::remove_file(wal_path);
    }

    /// The route walk on the real engine: FusionStorage resolves scans
    /// through the MVCC merge, and a DROP legitimately sees routes whose rows
    /// exist only in its own uncommitted write buffer.
    #[tokio::test]
    async fn structured_shadow_cleanup_walks_routes_on_fusion_storage() {
        let (executor, storage, data_dir) = fusion_executor("shadow_cleanup_fusion_routes").await;

        let committed_routes = [
            crate::storage::keyspace::DataRoute::Unsharded,
            crate::storage::keyspace::DataRoute::Shard(3),
            crate::storage::keyspace::DataRoute::Shard(u64::MAX),
        ];
        let mut txn = storage.begin_transaction().await.unwrap();
        for route in committed_routes {
            txn.put(
                &crate::storage::keyspace::encode_data_key(route, b"orders", b"row:c").unwrap(),
                b"committed",
            )
            .await
            .unwrap();
            txn.put(
                &crate::storage::keyspace::encode_data_key(route, b"orders_2", b"row:n").unwrap(),
                b"neighbor",
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();

        // One route exists only in this transaction's write buffer, plus an
        // extra row in an already-committed route.
        let buffer_only_route = crate::storage::keyspace::DataRoute::Shard(42);
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(
            &crate::storage::keyspace::encode_data_key(buffer_only_route, b"orders", b"row:b")
                .unwrap(),
            b"write-buffer only",
        )
        .await
        .unwrap();
        txn.put(
            &crate::storage::keyspace::encode_data_key(
                crate::storage::keyspace::DataRoute::Shard(3),
                b"orders",
                b"row:b2",
            )
            .unwrap(),
            b"staged in committed route",
        )
        .await
        .unwrap();
        executor
            .delete_structured_data_shadows_for_table("orders", &mut *txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = storage.begin_transaction().await.unwrap();
        for route in committed_routes {
            assert!(
                txn.get(
                    &crate::storage::keyspace::encode_data_key(route, b"orders", b"row:c").unwrap()
                )
                .await
                .unwrap()
                .is_none(),
                "committed target row survived in route {route:?}"
            );
            assert!(
                txn.get(
                    &crate::storage::keyspace::encode_data_key(route, b"orders_2", b"row:n")
                        .unwrap()
                )
                .await
                .unwrap()
                .is_some(),
                "prefix-sharing neighbor was deleted in route {route:?}"
            );
        }
        assert!(
            txn.get(
                &crate::storage::keyspace::encode_data_key(buffer_only_route, b"orders", b"row:b")
                    .unwrap()
            )
            .await
            .unwrap()
            .is_none(),
            "a route present only in the write buffer was missed by route discovery"
        );
        assert!(
            txn.get(
                &crate::storage::keyspace::encode_data_key(
                    crate::storage::keyspace::DataRoute::Shard(3),
                    b"orders",
                    b"row:b2"
                )
                .unwrap()
            )
            .await
            .unwrap()
            .is_none(),
            "a staged row in an already-committed route was missed"
        );
        drop(txn);

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    // ---- Data V2 migration phase: CALL surface + fencing races (P10-2.1) ----

    fn phase_row(result: &QueryResult) -> (String, i64) {
        let QueryResult::Select { columns, rows } = result else {
            panic!("migration procedures return a row, got {result:?}");
        };
        assert_eq!(columns[0], "phase");
        assert_eq!(columns[1], "phase_seq");
        let Value::String(phase) = &rows[0][0] else {
            panic!("phase column must be a string, got {:?}", rows[0][0]);
        };
        let Value::Integer(seq) = &rows[0][1] else {
            panic!("phase_seq column must be an integer, got {:?}", rows[0][1]);
        };
        (phase.clone(), *seq)
    }

    async fn fusion_executor(test_name: &str) -> (Executor, Arc<dyn Storage>, std::path::PathBuf) {
        let data_dir =
            std::env::temp_dir().join(format!("fusiondb_{}_{}", test_name, uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = crate::config::StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let fusion =
            crate::storage::FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                .await
                .unwrap();
        let storage: Arc<dyn Storage> = Arc::new(fusion);
        let executor = Executor::with_config(storage.clone(), &config);
        (executor, storage, data_dir)
    }

    #[tokio::test]
    async fn data_migration_call_lifecycle_and_gates() {
        let wal_path = format!("test_migration_call_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage.clone());

        let shown = executor
            .execute_sql("SHOW DATA MIGRATION PHASE")
            .await
            .unwrap();
        let QueryResult::Select { rows, .. } = &shown[0] else {
            panic!("SHOW returns a row");
        };
        assert!(
            matches!(&rows[0][0], Value::String(s) if s.contains("no record")
            && s.contains("delete-only"))
        );

        let error = executor
            .execute_sql("CALL fusiondb_data_migration_advance('write-delete-shadow')")
            .await
            .expect_err("advance before init must fail loudly");
        assert!(error
            .to_string()
            .contains("run CALL fusiondb_data_migration_init() first"));

        let inited = executor
            .execute_sql("CALL fusiondb_data_migration_init()")
            .await
            .unwrap();
        assert_eq!(phase_row(&inited[0]), ("delete-only".to_string(), 1));

        // INIT retry is idempotent with zero writes.
        let inited = executor
            .execute_sql("CALL fusiondb_data_migration_init()")
            .await
            .unwrap();
        assert_eq!(phase_row(&inited[0]), ("delete-only".to_string(), 1));

        let advanced = executor
            .execute_sql("CALL fusiondb_data_migration_advance('write-delete-shadow')")
            .await
            .unwrap();
        assert_eq!(
            phase_row(&advanced[0]),
            ("write-delete-shadow".to_string(), 2)
        );

        // Advance retry to the current phase is an idempotent no-op.
        let advanced = executor
            .execute_sql("CALL fusiondb_data_migration_advance('write-delete-shadow')")
            .await
            .unwrap();
        assert_eq!(
            phase_row(&advanced[0]),
            ("write-delete-shadow".to_string(), 2)
        );

        // Backfill is reachable as of P10-2.3.
        let advanced = executor
            .execute_sql("CALL fusiondb_data_migration_advance('backfill')")
            .await
            .unwrap();
        assert_eq!(phase_row(&advanced[0]), ("backfill".to_string(), 3));

        // The next rung exists but is beyond this build's advance gate.
        let error = executor
            .execute_sql("CALL fusiondb_data_migration_advance('validated')")
            .await
            .expect_err("validated is beyond this build's advance gate");
        assert!(error.to_string().contains("not supported by this build"));

        // Downgrade and rung-skipping are single-step violations.
        let error = executor
            .execute_sql("CALL fusiondb_data_migration_advance('delete-only')")
            .await
            .expect_err("downgrade must fail");
        assert!(error.to_string().contains("only advance one step"));
        let error = executor
            .execute_sql("CALL fusiondb_data_migration_advance('v2-readable')")
            .await
            .expect_err("skipping rungs must fail");
        assert!(error.to_string().contains("only advance one step"));

        let error = executor
            .execute_sql("CALL fusiondb_data_migration_advance('nonsense')")
            .await
            .expect_err("unknown phase name must fail");
        assert!(error
            .to_string()
            .contains("unknown Data V2 migration phase"));

        let error = executor
            .execute_sql("CALL fusiondb_data_migration_init('x')")
            .await
            .expect_err("init takes no arguments");
        assert!(error.to_string().contains("takes no arguments"));
        let error = executor
            .execute_sql("CALL fusiondb_data_migration_advance()")
            .await
            .expect_err("advance takes one argument");
        assert!(error.to_string().contains("exactly one"));
        let error = executor
            .execute_sql("CALL some_other_procedure()")
            .await
            .expect_err("unknown CALL names stay unsupported");
        assert!(error.to_string().contains("Unsupported SQL statement"));

        // Migration procedures are standalone statements, like VACUUM.
        let error = executor
            .execute_sql("CALL fusiondb_data_migration_init(); SELECT 1")
            .await
            .expect_err("migration CALL must be standalone");
        assert!(error.to_string().contains("standalone"));

        let shown = executor
            .execute_sql("SHOW DATA MIGRATION PHASE")
            .await
            .unwrap();
        assert_eq!(phase_row(&shown[0]), ("backfill".to_string(), 3));

        let _ = std::fs::remove_file(wal_path);
    }

    /// pgwire parses the text before `execute_sql` can intercept the raw
    /// string, so the diagnostic must also work as a parsed statement.
    #[tokio::test]
    async fn show_data_migration_phase_works_as_a_parsed_statement() {
        let wal_path = format!("test_migration_show_stmt_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage.clone());
        executor
            .execute_sql("CALL fusiondb_data_migration_init()")
            .await
            .unwrap();

        let stmt = crate::parser::parse_sql("SHOW DATA MIGRATION PHASE")
            .unwrap()
            .remove(0);
        assert!(!Executor::statement_may_change_query_results(&stmt));
        let mut txn = storage.begin_transaction().await.unwrap();
        let result = executor
            .execute_in_transaction(&stmt, &mut *txn)
            .await
            .expect("the parsed form must resolve, not error as unsupported");
        assert_eq!(phase_row(&result), ("delete-only".to_string(), 1));

        let _ = std::fs::remove_file(wal_path);
    }

    /// The superuser gate is statement-shaped: a leading comment or a tab
    /// separator must not slip a migration procedure past it.
    #[tokio::test]
    async fn migration_call_superuser_gate_resists_prefix_tricks() {
        let wal_path = format!("test_migration_authz_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage.clone());
        executor
            .execute_sql("CREATE USER alice WITH PASSWORD 'pw'")
            .await
            .unwrap();

        for sql in [
            "CALL fusiondb_data_migration_init()",
            "/* sneak */ CALL fusiondb_data_migration_init()",
            "CALL\tfusiondb_data_migration_advance('write-delete-shadow')",
            "  \n CALL fusiondb_data_migration_init()",
        ] {
            let error = executor
                .authorize_sql("alice", sql)
                .await
                .expect_err(&format!("non-superuser must be refused for: {sql}"));
            assert!(
                error.to_string().to_lowercase().contains("superuser"),
                "unexpected error for {sql}: {error}"
            );
        }

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn data_migration_record_overrides_the_config_flag() {
        // Flag OFF, record advanced to write-delete-shadow: shadows appear.
        let wal_path = format!("test_migration_flag_off_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage.clone());
        executor
            .execute_sql("CREATE TABLE flagless (id TEXT PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();
        executor
            .execute_sql("CALL fusiondb_data_migration_init()")
            .await
            .unwrap();
        executor
            .execute_sql("CALL fusiondb_data_migration_advance('write-delete-shadow')")
            .await
            .unwrap();
        executor
            .execute_sql("INSERT INTO flagless VALUES ('row:1', 'one')")
            .await
            .unwrap();
        let shadow_key = executor
            .routed_structured_data_key_for_row_id("flagless", "row:1")
            .unwrap();
        {
            let txn = storage.begin_transaction().await.unwrap();
            assert!(
                txn.get(&shadow_key).await.unwrap().is_some(),
                "record=write-delete-shadow must shadow-write even with the flag off"
            );
        }
        let _ = std::fs::remove_file(wal_path);

        // Flag ON, record still delete-only: no shadows.
        let wal_path = format!("test_migration_flag_on_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let mut config = crate::config::StorageConfig::default();
        config.structured_data_shadow_v2 = true;
        let executor = Executor::with_config(storage.clone(), &config);
        executor
            .execute_sql("CREATE TABLE flagged (id TEXT PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();
        {
            // Plant a delete-only record directly (flag-on INIT would start
            // at write-delete-shadow).
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(
                migration_phase_key(),
                &DataMigrationPhaseRecord {
                    phase: DataMigrationPhase::DeleteOnly,
                    phase_seq: 1,
                    updated_at_unix_ms: 42,
                }
                .encode(),
            )
            .await
            .unwrap();
            txn.commit().await.unwrap();
        }
        executor
            .execute_sql("INSERT INTO flagged VALUES ('row:1', 'one')")
            .await
            .unwrap();
        let shadow_key = executor
            .routed_structured_data_key_for_row_id("flagged", "row:1")
            .unwrap();
        {
            let txn = storage.begin_transaction().await.unwrap();
            assert!(
                txn.get(&shadow_key).await.unwrap().is_none(),
                "record=delete-only must suppress shadows even with the flag on"
            );
        }
        let _ = std::fs::remove_file(wal_path);
    }

    /// On FusionStorage the shadow decision runs through the cached fence,
    /// a different branch from the MemoryStorage fallback above, and it must
    /// survive a reopen (the record is read back by `load_and_gate`).
    #[tokio::test]
    async fn fusion_cached_fence_drives_shadow_writes_across_reopen() {
        let (executor, storage, data_dir) = fusion_executor("migration_fusion_fence").await;
        executor
            .execute_sql("CREATE TABLE fenced_rows (id TEXT PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();
        executor
            .execute_sql("CALL fusiondb_data_migration_init()")
            .await
            .unwrap();
        executor
            .execute_sql("CALL fusiondb_data_migration_advance('write-delete-shadow')")
            .await
            .unwrap();
        executor
            .execute_sql("INSERT INTO fenced_rows VALUES ('row:1', 'one')")
            .await
            .unwrap();
        let shadow_key = executor
            .routed_structured_data_key_for_row_id("fenced_rows", "row:1")
            .unwrap();
        {
            let txn = storage.begin_transaction().await.unwrap();
            assert!(txn.get(&shadow_key).await.unwrap().is_some());
        }
        drop(executor);
        drop(storage);

        // Reopen: the record must be re-read and still drive shadow writes,
        // even though the config flag is off.
        let mut config = crate::config::StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let fusion =
            crate::storage::FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                .await
                .expect("reopen with a write-delete-shadow record");
        let storage: Arc<dyn Storage> = Arc::new(fusion);
        let executor = Executor::with_config(storage.clone(), &config);
        assert!(!executor.structured_data_shadow_v2);
        executor
            .execute_sql("INSERT INTO fenced_rows VALUES ('row:2', 'two')")
            .await
            .unwrap();
        let shadow_key = executor
            .routed_structured_data_key_for_row_id("fenced_rows", "row:2")
            .unwrap();
        let txn = storage.begin_transaction().await.unwrap();
        assert!(
            txn.get(&shadow_key).await.unwrap().is_some(),
            "the reopened fence must keep shadow writes on"
        );
        drop(txn);

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn fenced_write_aborts_when_phase_advances_mid_transaction() {
        let (executor, storage, data_dir) = fusion_executor("migration_fence_race").await;
        executor
            .execute_sql("CREATE TABLE fence_race (id TEXT PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();

        // An interactive transaction (the pg session shape: BEGIN .. write
        // .. COMMIT commits this same FusionTransaction) writes under the
        // pre-INIT fence.
        let insert = executor
            .prepare("INSERT INTO fence_race VALUES ('row:1', 'stale')")
            .unwrap()
            .remove(0);
        let mut in_flight = storage.begin_transaction().await.unwrap();
        executor
            .execute_in_transaction(&insert, &mut *in_flight)
            .await
            .unwrap();

        // A concurrent operator INIT commits first.
        executor
            .execute_sql("CALL fusiondb_data_migration_init()")
            .await
            .unwrap();

        let error = in_flight
            .commit()
            .await
            .expect_err("COMMIT after a concurrent phase change must abort");
        assert!(
            error.to_string().contains("migration phase advanced"),
            "unexpected: {error}"
        );

        // The retry succeeds under the new fence.
        let insert = executor
            .prepare("INSERT INTO fence_race VALUES ('row:1', 'fresh')")
            .unwrap()
            .remove(0);
        let mut retry = storage.begin_transaction().await.unwrap();
        executor
            .execute_in_transaction(&insert, &mut *retry)
            .await
            .unwrap();
        retry.commit().await.unwrap();

        let rows = executor
            .execute_sql("SELECT payload FROM fence_race WHERE id = 'row:1'")
            .await
            .unwrap();
        let QueryResult::Select { rows, .. } = &rows[0] else {
            panic!("select returns rows");
        };
        assert_eq!(rows[0][0], Value::String("fresh".to_string()));

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// The atomicity rule: a session transaction that mixes data writes with
    /// an advance must abort at COMMIT. Committing both would publish rows
    /// evaluated under the old phase together with the new phase contract.
    #[tokio::test]
    async fn session_transaction_mixing_data_write_and_advance_aborts() {
        let (executor, storage, data_dir) = fusion_executor("migration_mixed_txn").await;
        executor
            .execute_sql("CREATE TABLE mixed_txn (id TEXT PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();
        executor
            .execute_sql("CALL fusiondb_data_migration_init()")
            .await
            .unwrap();

        let insert = executor
            .prepare("INSERT INTO mixed_txn VALUES ('row:1', 'one')")
            .unwrap()
            .remove(0);
        let advance = executor
            .prepare("CALL fusiondb_data_migration_advance('write-delete-shadow')")
            .unwrap()
            .remove(0);

        // BEGIN; INSERT; CALL advance; COMMIT
        let mut session = storage.begin_transaction().await.unwrap();
        executor
            .execute_in_transaction(&insert, &mut *session)
            .await
            .unwrap();
        executor
            .execute_in_transaction(&advance, &mut *session)
            .await
            .unwrap();
        let error = session
            .commit()
            .await
            .expect_err("mixing data writes with an advance must abort at COMMIT");
        assert!(
            error.to_string().contains("must not share a transaction"),
            "unexpected: {error}"
        );

        // Neither effect landed: the row is absent and the phase is unchanged.
        let rows = executor
            .execute_sql("SELECT payload FROM mixed_txn WHERE id = 'row:1'")
            .await
            .unwrap();
        let QueryResult::Select { rows, .. } = &rows[0] else {
            panic!("select returns rows");
        };
        assert!(rows.is_empty());
        let shown = executor
            .execute_sql("SHOW DATA MIGRATION PHASE")
            .await
            .unwrap();
        assert_eq!(phase_row(&shown[0]), ("delete-only".to_string(), 1));

        // The same statements in their own transactions both succeed.
        executor
            .execute_sql("INSERT INTO mixed_txn VALUES ('row:1', 'one')")
            .await
            .unwrap();
        executor
            .execute_sql("CALL fusiondb_data_migration_advance('write-delete-shadow')")
            .await
            .unwrap();

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn concurrent_double_advance_exactly_one_wins() {
        let (executor, storage, data_dir) = fusion_executor("migration_double_advance").await;
        executor
            .execute_sql("CALL fusiondb_data_migration_init()")
            .await
            .unwrap();

        let advance = executor
            .prepare("CALL fusiondb_data_migration_advance('write-delete-shadow')")
            .unwrap()
            .remove(0);
        let mut first = storage.begin_transaction().await.unwrap();
        let mut second = storage.begin_transaction().await.unwrap();
        executor
            .execute_in_transaction(&advance, &mut *first)
            .await
            .unwrap();
        executor
            .execute_in_transaction(&advance, &mut *second)
            .await
            .unwrap();

        first.commit().await.expect("first advance wins");
        let error = second
            .commit()
            .await
            .expect_err("second concurrent advance must lose OCC validation");
        assert!(
            error.to_string().contains("Write conflict"),
            "unexpected: {error}"
        );

        // The loser's retry lands on the idempotent branch: same phase, same
        // sequence, zero writes.
        let retried = executor
            .execute_sql("CALL fusiondb_data_migration_advance('write-delete-shadow')")
            .await
            .unwrap();
        assert_eq!(
            phase_row(&retried[0]),
            ("write-delete-shadow".to_string(), 2)
        );

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    // ---- Data V2 backfill engine (P10-2.3) ----

    /// Walk the ladder to `backfill` so chunk steps are legal.
    async fn advance_to_backfill(executor: &Executor) {
        for sql in [
            "CALL fusiondb_data_migration_init()",
            "CALL fusiondb_data_migration_advance('write-delete-shadow')",
            "CALL fusiondb_data_migration_advance('backfill')",
        ] {
            executor.execute_sql(sql).await.expect(sql);
        }
    }

    fn backfill_status_row(result: &QueryResult) -> (String, i64) {
        let QueryResult::Select { columns, rows } = result else {
            panic!("backfill procedures return a row, got {result:?}");
        };
        assert_eq!(columns[0], "status");
        assert_eq!(columns[1], "rows_done");
        let (Value::String(status), Value::Integer(rows_done)) = (&rows[0][0], &rows[0][1]) else {
            panic!("unexpected status row shape: {:?}", rows[0]);
        };
        (status.clone(), *rows_done)
    }

    /// Drive chunks to completion and report how many steps it took.
    async fn drain_backfill(executor: &Executor) -> (usize, i64) {
        for step in 1..=200 {
            let result = executor
                .execute_sql("CALL fusiondb_data_backfill_step()")
                .await
                .expect("backfill step");
            let (status, rows_done) = backfill_status_row(&result[0]);
            if status == "complete" {
                return (step, rows_done);
            }
        }
        panic!("backfill did not converge within 200 chunks");
    }

    async fn shadow_exists(
        executor: &Executor,
        storage: &Arc<dyn Storage>,
        table: &str,
        row: &str,
    ) -> bool {
        let key = executor
            .routed_structured_data_key_for_row_id(table, row)
            .unwrap();
        let txn = storage.begin_transaction().await.unwrap();
        txn.get(&key).await.unwrap().is_some()
    }

    #[tokio::test]
    async fn backfill_copies_every_legacy_row_and_resumes_across_chunks() {
        let (executor, storage, data_dir) = fusion_executor("backfill_basic").await;
        executor
            .execute_sql("CREATE TABLE b_rows (id INT PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();
        // More rows than one chunk holds, so resumption is exercised.
        let rows = Executor::BACKFILL_CHUNK_ROWS + 37;
        for id in 0..rows {
            executor
                .execute_sql(&format!("INSERT INTO b_rows VALUES ({id}, 'payload-{id}')"))
                .await
                .unwrap();
        }

        // Written before the phase reached write-delete-shadow, so no shadows exist yet.
        assert!(!shadow_exists(&executor, &storage, "b_rows", "0").await);

        advance_to_backfill(&executor).await;
        let (steps, rows_done) = drain_backfill(&executor).await;
        assert!(steps >= 2, "expected multiple chunks, took {steps}");
        assert_eq!(rows_done as usize, rows);

        for id in 0..rows {
            let row_id = crate::common::encoding::encode_i64_comparable(id as i64);
            assert!(
                shadow_exists(&executor, &storage, "b_rows", &row_id).await,
                "row {id} was not backfilled"
            );
        }

        // Idempotent: another step on a complete backfill is a no-op.
        let result = executor
            .execute_sql("CALL fusiondb_data_backfill_step()")
            .await
            .unwrap();
        assert_eq!(backfill_status_row(&result[0]).0, "complete");

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Progress and copied rows are written by the same transaction, so a
    /// crash between chunks can lose both or neither — never a cursor that
    /// claims rows it did not copy.
    #[tokio::test]
    async fn backfill_cursor_survives_reopen_and_resumes() {
        let data_dir =
            std::env::temp_dir().join(format!("fusiondb_backfill_resume_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = crate::config::StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let rows = Executor::BACKFILL_CHUNK_ROWS + 11;

        let rows_after_first_chunk = {
            let fusion =
                crate::storage::FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                    .await
                    .unwrap();
            let storage: Arc<dyn Storage> = Arc::new(fusion);
            let executor = Executor::with_config(storage.clone(), &config);
            executor
                .execute_sql("CREATE TABLE b_resume (id INT PRIMARY KEY, payload TEXT)")
                .await
                .unwrap();
            for id in 0..rows {
                executor
                    .execute_sql(&format!("INSERT INTO b_resume VALUES ({id}, 'p{id}')"))
                    .await
                    .unwrap();
            }
            advance_to_backfill(&executor).await;
            let result = executor
                .execute_sql("CALL fusiondb_data_backfill_step()")
                .await
                .unwrap();
            let (status, rows_done) = backfill_status_row(&result[0]);
            assert_eq!(status, "in-progress");
            rows_done
        };
        assert!(rows_after_first_chunk > 0);

        // Reopen: the cursor must be durable and the backfill must resume,
        // not restart.
        let fusion =
            crate::storage::FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                .await
                .expect("reopen mid-backfill");
        let storage: Arc<dyn Storage> = Arc::new(fusion);
        let executor = Executor::with_config(storage.clone(), &config);
        let status = executor
            .execute_sql("CALL fusiondb_data_backfill_status()")
            .await
            .unwrap();
        assert_eq!(
            backfill_status_row(&status[0]),
            ("in-progress".to_string(), rows_after_first_chunk)
        );

        let (_steps, total) = drain_backfill(&executor).await;
        assert_eq!(total as usize, rows);
        for id in 0..rows {
            let row_id = crate::common::encoding::encode_i64_comparable(id as i64);
            assert!(shadow_exists(&executor, &storage, "b_resume", &row_id).await);
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// A chunk and a concurrent row write target the same v2 key, so
    /// write-set-only OCC forces one of them to abort — the backfill can
    /// never overwrite a newer row with a stale copy.
    #[tokio::test]
    async fn backfill_chunk_conflicts_with_concurrent_row_write() {
        let (executor, storage, data_dir) = fusion_executor("backfill_occ").await;
        executor
            .execute_sql("CREATE TABLE b_race (id INT PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();
        executor
            .execute_sql("INSERT INTO b_race VALUES (1, 'original')")
            .await
            .unwrap();
        advance_to_backfill(&executor).await;

        // Stage a chunk without committing it.
        let step = executor
            .prepare("CALL fusiondb_data_backfill_step()")
            .unwrap()
            .remove(0);
        let mut chunk_txn = storage.begin_transaction().await.unwrap();
        executor
            .execute_in_transaction(&step, &mut *chunk_txn)
            .await
            .unwrap();

        // A concurrent UPDATE commits first, writing the same shadow key.
        executor
            .execute_sql("UPDATE b_race SET payload = 'newer' WHERE id = 1")
            .await
            .unwrap();

        let error = chunk_txn
            .commit()
            .await
            .expect_err("a chunk racing a row write must abort, not overwrite");
        assert!(
            error.to_string().contains("Write conflict"),
            "unexpected: {error}"
        );

        // Retrying converges on the newer value.
        drain_backfill(&executor).await;
        let shadow_key = executor
            .routed_structured_data_key_for_row_id(
                "b_race",
                &crate::common::encoding::encode_i64_comparable(1),
            )
            .unwrap();
        let legacy_key = executor.routed_data_key_for_row_id(
            "b_race",
            &crate::common::encoding::encode_i64_comparable(1),
        );
        let txn = storage.begin_transaction().await.unwrap();
        assert_eq!(
            txn.get(&shadow_key).await.unwrap(),
            txn.get(legacy_key.as_bytes()).await.unwrap(),
            "shadow must equal the newest legacy value"
        );

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// The named blocker: a DROP's v2 cleanup only tombstones keys its own
    /// snapshot saw, so without a shared write key a concurrent chunk would
    /// commit fresh shadow rows for a table that no longer exists.
    #[tokio::test]
    async fn drop_table_conflicts_with_an_in_flight_backfill_chunk() {
        let (executor, storage, data_dir) = fusion_executor("backfill_drop_race").await;
        executor
            .execute_sql("CREATE TABLE b_drop (id INT PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();
        for id in 0..5 {
            executor
                .execute_sql(&format!("INSERT INTO b_drop VALUES ({id}, 'p{id}')"))
                .await
                .unwrap();
        }
        advance_to_backfill(&executor).await;

        let step = executor
            .prepare("CALL fusiondb_data_backfill_step()")
            .unwrap()
            .remove(0);
        let mut chunk_txn = storage.begin_transaction().await.unwrap();
        executor
            .execute_in_transaction(&step, &mut *chunk_txn)
            .await
            .unwrap();

        // DROP commits first and touches the shared backfill-state record.
        executor.execute_sql("DROP TABLE b_drop").await.unwrap();

        let error = chunk_txn
            .commit()
            .await
            .expect_err("a chunk racing DROP must abort instead of stranding orphan shadows");
        assert!(
            error.to_string().contains("Write conflict"),
            "unexpected: {error}"
        );

        // No orphan shadow rows survive for the dropped table.
        for id in 0..5 {
            let row_id = crate::common::encoding::encode_i64_comparable(id);
            assert!(
                !shadow_exists(&executor, &storage, "b_drop", &row_id).await,
                "row {id} of the dropped table was stranded in the v2 namespace"
            );
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn backfill_gates_phase_arguments_and_topology() {
        let (executor, _storage, data_dir) = fusion_executor("backfill_gates").await;
        executor
            .execute_sql("CREATE TABLE b_gate (id INT PRIMARY KEY)")
            .await
            .unwrap();

        // Steps before the phase reaches backfill are refused.
        let error = executor
            .execute_sql("CALL fusiondb_data_backfill_step()")
            .await
            .expect_err("a step below phase backfill must be refused");
        assert!(error.to_string().contains("requires migration phase"));

        // Status is readable at any phase and reports not-started.
        let status = executor
            .execute_sql("CALL fusiondb_data_backfill_status()")
            .await
            .unwrap();
        assert_eq!(backfill_status_row(&status[0]).0, "not-started");

        advance_to_backfill(&executor).await;
        let error = executor
            .execute_sql("CALL fusiondb_data_backfill_step('x')")
            .await
            .expect_err("step takes no arguments");
        assert!(error.to_string().contains("takes no arguments"));

        // Migration procedures stay standalone and superuser-only.
        let error = executor
            .execute_sql("CALL fusiondb_data_backfill_step(); SELECT 1")
            .await
            .expect_err("backfill step must be standalone");
        assert!(error.to_string().contains("standalone"));
        executor
            .execute_sql("CREATE USER bob WITH PASSWORD 'pw'")
            .await
            .unwrap();
        for sql in [
            "CALL fusiondb_data_backfill_step()",
            "/* sneak */ CALL fusiondb_data_backfill_status()",
        ] {
            let error = executor
                .authorize_sql("bob", sql)
                .await
                .expect_err("non-superuser must be refused");
            assert!(error.to_string().to_lowercase().contains("superuser"));
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// The cursor is only meaningful under the topology it was recorded on.
    /// A shard-count change invalidates it, so resuming across one must be
    /// refused instead of silently copying rows at the wrong routes.
    #[tokio::test]
    async fn backfill_refuses_to_resume_across_a_topology_change() {
        let (executor, storage, data_dir) = fusion_executor("backfill_topology").await;
        executor
            .execute_sql("CREATE TABLE b_topo (id INT PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();
        executor
            .execute_sql("INSERT INTO b_topo VALUES (1, 'one')")
            .await
            .unwrap();
        advance_to_backfill(&executor).await;

        // Record a state that claims a different starting topology than the
        // executor's current one (which is None — no router configured).
        let mut txn = storage.begin_transaction().await.unwrap();
        let planted = crate::storage::data_migration::DataBackfillState {
            shard_count_at_start: Some(16),
            chunks_done: 1,
            rows_done: 0,
            updated_at_unix_ms: 1,
            complete: false,
            cursor: None,
        };
        txn.put(
            crate::storage::data_migration::backfill_state_key(),
            &planted.encode().unwrap(),
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let error = executor
            .execute_sql("CALL fusiondb_data_backfill_step()")
            .await
            .expect_err("resuming across a topology change must be refused");
        assert!(
            error.to_string().contains("shard topology change"),
            "unexpected: {error}"
        );

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Copying must preserve the row bytes, not merely create a key.
    #[tokio::test]
    async fn backfill_shadow_values_equal_their_legacy_rows() {
        let (executor, storage, data_dir) = fusion_executor("backfill_values").await;
        executor
            .execute_sql("CREATE TABLE b_val (id INT PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();
        for id in 0..5 {
            executor
                .execute_sql(&format!(
                    "INSERT INTO b_val VALUES ({id}, 'payload number {id}')"
                ))
                .await
                .unwrap();
        }
        advance_to_backfill(&executor).await;
        drain_backfill(&executor).await;

        let txn = storage.begin_transaction().await.unwrap();
        for id in 0..5 {
            let row_id = crate::common::encoding::encode_i64_comparable(id);
            let legacy = txn
                .get(
                    executor
                        .routed_data_key_for_row_id("b_val", &row_id)
                        .as_bytes(),
                )
                .await
                .unwrap()
                .expect("legacy row");
            let shadow = txn
                .get(
                    &executor
                        .routed_structured_data_key_for_row_id("b_val", &row_id)
                        .unwrap(),
                )
                .await
                .unwrap()
                .expect("shadow row");
            assert_eq!(shadow, legacy, "row {id} shadow bytes differ from legacy");
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Table identity comes from the schema catalog, not from a positional
    /// guess: primary keys are not always the first column, and both table
    /// names and row ids may contain ':'. Orphan rows of no known table are
    /// skipped rather than copied into some table's shadow set.
    #[tokio::test]
    async fn backfill_resolves_table_identity_for_colon_names_and_late_primary_keys() {
        let (executor, storage, data_dir) = fusion_executor("backfill_identity").await;
        // Primary key is the SECOND column, and row ids contain ':'.
        executor
            .execute_sql("CREATE TABLE b_ident (payload TEXT, id TEXT PRIMARY KEY)")
            .await
            .unwrap();
        executor
            .execute_sql("INSERT INTO b_ident VALUES ('one', 'row:1')")
            .await
            .unwrap();
        // A quoted identifier keeps its quotes in the stored name, so it does
        // not actually collide; exercise it anyway as the end-to-end shape.
        executor
            .execute_sql("CREATE TABLE \"b_ident quoted\" (payload TEXT, id TEXT PRIMARY KEY)")
            .await
            .unwrap();
        executor
            .execute_sql("INSERT INTO \"b_ident quoted\" VALUES ('q', 'row:8')")
            .await
            .unwrap();

        // A genuinely colon-named table (only reachable for synthetic or
        // historical data) whose rows collide with `b_ident`'s split points.
        // Registering it in the catalog forces the multi-candidate tie-break.
        let archive_schema = crate::catalog::TableSchema::new(
            "b_ident:archive".to_string(),
            vec![
                crate::catalog::Column {
                    name: "payload".to_string(),
                    data_type: "TEXT".to_string(),
                    is_primary: false,
                    is_indexed: false,
                    index_type: crate::catalog::IndexType::None,
                    default_value: None,
                    is_nullable: true,
                    is_unique: false,
                    check_expr: None,
                },
                crate::catalog::Column {
                    name: "id".to_string(),
                    data_type: "TEXT".to_string(),
                    is_primary: true,
                    is_indexed: false,
                    index_type: crate::catalog::IndexType::None,
                    default_value: None,
                    is_nullable: false,
                    is_unique: true,
                    check_expr: None,
                },
            ],
        );
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(
            b"schema:b_ident:archive",
            &bincode::serialize(&archive_schema).unwrap(),
        )
        .await
        .unwrap();
        txn.put(
            b"data:b_ident:archive:row:9",
            &crate::common::encoding::RowEncoder::encode(&[
                Value::String("arch".to_string()),
                Value::String("row:9".to_string()),
            ]),
        )
        .await
        .unwrap();
        // An orphan row of a table that does not exist.
        txn.put(
            b"data:b_ghost:row:1",
            &crate::common::encoding::RowEncoder::encode(&[Value::String("ghost".to_string())]),
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        advance_to_backfill(&executor).await;
        let (_steps, rows_done) = drain_backfill(&executor).await;
        assert_eq!(rows_done, 3, "only the three real rows may be copied");

        assert!(shadow_exists(&executor, &storage, "b_ident", "row:1").await);
        assert!(shadow_exists(&executor, &storage, "\"b_ident quoted\"", "row:8").await);
        assert!(
            shadow_exists(&executor, &storage, "b_ident:archive", "row:9").await,
            "the colon-named table's row was attributed to the wrong table"
        );
        assert!(
            !shadow_exists(&executor, &storage, "b_ghost", "row:1").await,
            "an orphan row was copied into the v2 namespace"
        );

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Legacy rows live under several physical routes; the backfill walks the
    /// keyspace rather than asking the router, so historical shard routes are
    /// copied too, and non-base-row keys in the shard region are skipped.
    #[tokio::test]
    async fn backfill_covers_historical_routes_and_skips_other_key_families() {
        let (executor, storage, data_dir) = fusion_executor("backfill_routes").await;
        executor
            .execute_sql("CREATE TABLE b_hist (id INT PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();
        executor
            .execute_sql("INSERT INTO b_hist VALUES (1, 'unsharded')")
            .await
            .unwrap();

        // A row left behind by a historical shard topology, plus a sharded
        // index key that must NOT be treated as a base row.
        let historical_value = crate::common::encoding::RowEncoder::encode(&[
            Value::Integer(2),
            Value::String("historical".to_string()),
        ]);
        let mut txn = storage.begin_transaction().await.unwrap();
        let historical_row_id = crate::common::encoding::encode_i64_comparable(2);
        txn.put(
            format!("shard:7:data:b_hist:{historical_row_id}").as_bytes(),
            &historical_value,
        )
        .await
        .unwrap();
        txn.put(b"shard:7:index:b_hist:payload:x:1", b"index entry")
            .await
            .unwrap();
        txn.commit().await.unwrap();

        advance_to_backfill(&executor).await;
        let (_steps, rows_done) = drain_backfill(&executor).await;
        assert_eq!(rows_done, 2, "both routes' rows must be copied");

        assert!(
            shadow_exists(
                &executor,
                &storage,
                "b_hist",
                &crate::common::encoding::encode_i64_comparable(1)
            )
            .await
        );
        assert!(
            shadow_exists(&executor, &storage, "b_hist", &historical_row_id).await,
            "a row on a historical shard route was skipped"
        );

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
