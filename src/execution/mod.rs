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
use crate::storage::{vector_index::VectorIndex, FusionStorage, ScanVisitor, Storage, Transaction};
use moka::sync::Cache;
use parking_lot::RwLock;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName,
    ObjectNamePart, ObjectType, OrderByKind, SelectItem, SetExpr, Statement, TableFactor,
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

pub struct Executor {
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) shard_router: Option<ShardRouter>,
    statement_cache: Cache<String, Vec<Statement>>,
    query_result_cache: Cache<String, CachedSelectResult>,
    simple_pk_update_fast_path_cache: Cache<String, bool>,
    query_result_epoch: AtomicU64,
    prepared_statements: RwLock<PreparedStatementStore>,
    pub(crate) row_cache: Cache<String, Vec<Value>>,
    pub(crate) vector_index: Arc<VectorIndex>,
    pub(crate) embedding_registry: Arc<EmbeddingRegistry>,
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

        Self {
            storage,
            shard_router,
            statement_cache: Cache::new(config.statement_cache_capacity),
            query_result_cache: Cache::new(config.statement_cache_capacity.max(1)),
            simple_pk_update_fast_path_cache: Cache::new(config.statement_cache_capacity.max(1)),
            query_result_epoch: AtomicU64::new(0),
            prepared_statements: RwLock::new(PreparedStatementStore::default()),
            row_cache: Cache::new(config.row_cache_capacity),
            vector_index: shared_vector_index,
            embedding_registry: Arc::new(EmbeddingRegistry::new()),
        }
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
        let mut pairs = Vec::new();
        for prefix in prefixes {
            let remaining = limit.map(|limit| limit.saturating_sub(pairs.len()));
            if remaining == Some(0) {
                break;
            }
            let mut shard_pairs = txn.scan_prefix(prefix.as_bytes(), remaining).await?;
            pairs.append(&mut shard_pairs);
            if limit.is_some_and(|limit| pairs.len() >= limit) {
                break;
            }
        }
        Ok(pairs)
    }

    pub(crate) async fn scan_routed_data_prefixes_for_table(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_routed_prefixes(self.routed_data_prefixes_for_table(table_name), txn, limit)
            .await
    }

    pub(crate) async fn scan_routed_data_prefixes_for_each(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> Result<usize> {
        let mut visited = 0usize;
        for prefix in self.routed_data_prefixes_for_table(table_name) {
            let remaining = limit.map(|limit| limit.saturating_sub(visited));
            if remaining == Some(0) {
                break;
            }
            let mut stop_aware = StopAwareScanVisitor {
                inner: visitor,
                stopped: false,
            };
            visited += txn
                .scan_prefix_for_each(prefix.as_bytes(), remaining, &mut stop_aware)
                .await?;
            if stop_aware.stopped {
                break;
            }
            if limit.is_some_and(|limit| visited >= limit) {
                break;
            }
        }
        Ok(visited)
    }

    pub(crate) async fn count_routed_data_prefixes_for_table(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<usize> {
        let mut count = 0usize;
        for prefix in self.routed_data_prefixes_for_table(table_name) {
            count = count.saturating_add(txn.count_prefix(prefix.as_bytes()).await?);
        }
        Ok(count)
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
    }

    pub(crate) fn invalidate_storage_caches(&self) {
        self.invalidate_query_result_cache();
        self.invalidate_update_fast_path_cache();
        self.row_cache.invalidate_all();
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
        if !matches!(
            join.join_operator,
            sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(_))
                | sqlparser::ast::JoinOperator::Join(sqlparser::ast::JoinConstraint::On(_))
        ) {
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
            || upper.starts_with("EXPLAIN ")
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
                    create_index.unique,
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
            _ => Ok(QueryResult::Success {
                message: "Statement not supported yet".to_string(),
            }),
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

        if stmts.len() == 1
            && (Self::is_cacheable_group_aggregate_statement(&stmts[0])
                || Self::is_cacheable_join_group_aggregate_statement(&stmts[0]))
        {
            let start = std::time::Instant::now();
            let cache_key = Self::query_result_cache_key(trimmed);
            let current_epoch = self.current_query_result_epoch();
            if let Some(cached) = self.query_result_cache.get(&cache_key) {
                if cached.epoch == current_epoch {
                    crate::monitor::record_query(trimmed, start.elapsed());
                    return Ok(vec![QueryResult::Select {
                        columns: cached.columns,
                        rows: cached.rows,
                    }]);
                }
            }

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
        for stmt in &stmts {
            may_change_query_results |= Self::statement_may_change_query_results(stmt);
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

        assert!(sql_requires_raft_write("INSERT INTO users VALUES (1)"));
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
    async fn sharded_executor_uses_physical_shard_data_keys_for_crud() {
        let wal_path = format!("test_sharded_executor_crud_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let config = sharded_test_config();
        let shard_router =
            crate::distributed::sharding::ShardRouter::from_config(&config).expect("router");
        let executor = Executor::with_config_and_shard_router(
            storage.clone(),
            &crate::config::StorageConfig::default(),
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
        {
            let txn = storage.begin_transaction().await.expect("begin txn");
            assert!(txn
                .get(first_sharded_key.as_bytes())
                .await
                .expect("get sharded key")
                .is_some());
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

        let _ = std::fs::remove_file(wal_path);
    }
}
