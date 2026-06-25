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
use crate::common::{FusionError, Result, Value};
use crate::config::StorageConfig;
use crate::monitor;
use crate::parser::parse_sql;
use crate::storage::{vector_index::VectorIndex, FusionStorage, Storage, Transaction};
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

pub struct Executor {
    pub(crate) storage: Arc<dyn Storage>,
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
        let shared_vector_index = storage
            .as_any()
            .downcast_ref::<FusionStorage>()
            .map(|fusion| fusion.vector_index.clone())
            .unwrap_or_else(|| Arc::new(VectorIndex::new()));

        Self {
            storage,
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

    pub(crate) fn invalidate_query_result_cache(&self) {
        self.query_result_epoch.fetch_add(1, AtomicOrdering::AcqRel);
        self.query_result_cache.invalidate_all();
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

    async fn require_superuser(&self, username: &str) -> Result<()> {
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
}
