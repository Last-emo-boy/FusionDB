mod aggregation;
mod ddl;
mod dml;
mod expr;
mod query;
mod scan;

pub(crate) use aggregation::AggregateAccumulator;

use crate::ai::embedding::EmbeddingRegistry;
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::parser::parse_sql;
use crate::storage::{vector_index::VectorIndex, Storage, Transaction};
use moka::sync::Cache;
use parking_lot::RwLock;
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub enum QueryResult {
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Success {
        message: String,
    },
}

pub struct Executor {
    pub(crate) storage: Arc<dyn Storage>,
    statement_cache: Cache<String, Vec<Statement>>,
    prepared_statements: RwLock<HashMap<String, Vec<Statement>>>,
    pub(crate) row_cache: Cache<String, Vec<Value>>,
    pub(crate) vector_index: Arc<VectorIndex>,
    pub(crate) embedding_registry: Arc<EmbeddingRegistry>,
}

impl Executor {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            statement_cache: Cache::new(1000),
            prepared_statements: RwLock::new(HashMap::new()),
            row_cache: Cache::new(10000),
            vector_index: Arc::new(VectorIndex::new()),
            embedding_registry: Arc::new(EmbeddingRegistry::new()),
        }
    }

    pub fn prepare(&self, sql: &str) -> Result<Vec<Statement>> {
        // Custom statements not supported by sqlparser return empty vec;
        // they are handled by execute_sql() instead.
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_uppercase();
        if upper == "SHOW VIEWS" || upper == "SHOW USERS"
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

    pub fn register_prepared_statement(&self, sql: &str) -> Result<String> {
        monitor::inc_parse();
        let stmts = parse_sql(sql)?;
        let id = uuid::Uuid::new_v4().to_string();

        let mut cache = self.prepared_statements.write();
        cache.insert(id.clone(), stmts);

        Ok(id)
    }

    pub fn get_prepared_statement(&self, id: &str) -> Result<Vec<Statement>> {
        let cache = self.prepared_statements.read();
        if let Some(stmts) = cache.get(id) {
            monitor::inc_plan();
            Ok(stmts.clone())
        } else {
            Err(FusionError::Execution(format!(
                "Prepared statement {} not found",
                id
            )))
        }
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
        match stmt {
            Statement::CreateTable(create_table) => {
                self.handle_create_table(
                    &create_table.name,
                    &create_table.columns,
                    create_table.if_not_exists,
                    txn,
                ).await
            }
            Statement::Insert(insert) => {
                self.handle_insert(insert.table.to_string(), &insert.columns, &insert.source, &insert.returning, &insert.on, txn, params)
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
            Statement::Drop {
                names,
                object_type: sqlparser::ast::ObjectType::View,
                if_exists,
                ..
            } => {
                self.handle_drop_view(names, *if_exists, txn).await
            }
            Statement::Drop {
                names,
                object_type: sqlparser::ast::ObjectType::Index,
                if_exists,
                ..
            } => {
                self.handle_drop_index(names, *if_exists, txn).await
            }
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
                    Err(crate::common::FusionError::Execution(
                        format!("SHOW CREATE {} not supported", obj_type),
                    ))
                }
            }
            Statement::ExplainTable { table_name, .. } => {
                self.handle_describe_table(table_name, txn).await
            }
            Statement::AlterTable(alter_table) => {
                self.handle_alter_table(&alter_table.name, &alter_table.operations, txn).await
            }
            Statement::Truncate(truncate) => {
                self.handle_truncate(&truncate.table_names, txn).await
            }
            Statement::CreateView(cv) => {
                self.handle_create_view(&cv.name, &cv.query, cv.or_replace, txn).await
            }
            _ => Ok(QueryResult::Success {
                message: "Statement not supported yet".to_string(),
            }),
        }
    }

    /// Check if a user has permission to access a table with a given operation.
    /// Public API for RBAC enforcement.
    pub async fn check_table_permission(&self, username: &str, table: &str, operation: &str) -> Result<()> {
        let mut txn = self.storage.begin_transaction().await?;
        crate::auth::check_permission(&mut *txn, username, table, operation).await
    }

    pub async fn execute(&self, stmt: &Statement) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let mut txn = self.storage.begin_transaction().await?;
        let res = self.execute_in_transaction(stmt, &mut *txn).await;
        if res.is_ok() {
            txn.commit().await?;
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
            if res.is_ok() { txn.commit().await?; }
            crate::monitor::record_query(trimmed, start.elapsed());
            return res.map(|r| vec![r]);
        }

        if upper == "SHOW USERS" {
            let start = std::time::Instant::now();
            let mut txn = self.storage.begin_transaction().await?;
            let res = self.handle_show_users(&mut *txn).await;
            if res.is_ok() { txn.commit().await?; }
            crate::monitor::record_query(trimmed, start.elapsed());
            return res.map(|r| vec![r]);
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
        let mut results = Vec::new();
        for stmt in &stmts {
            results.push(self.execute(stmt).await?);
        }
        Ok(results)
    }

    async fn handle_show_users(&self, txn: &mut dyn Transaction) -> Result<QueryResult> {
        let users = crate::auth::list_users(txn).await?;
        let rows: Vec<Vec<Value>> = users.iter().map(|(name, record)| {
            let perms: Vec<String> = record.permissions.iter()
                .map(|(t, p)| format!("{}: {}", t, p.iter().cloned().collect::<Vec<_>>().join(",")))
                .collect();
            vec![
                Value::String(name.clone()),
                Value::Boolean(record.is_superuser),
                Value::String(if perms.is_empty() { "none".to_string() } else { perms.join("; ") }),
            ]
        }).collect();
        Ok(QueryResult::Select {
            columns: vec!["User".to_string(), "Superuser".to_string(), "Permissions".to_string()],
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

    async fn execute_rbac_statement(&self, sql: &str, txn: &mut dyn Transaction) -> Result<QueryResult> {
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
            let is_superuser = upper.contains("SUPERUSER") && !upper.contains("WITH PASSWORD") || upper.ends_with("SUPERUSER");

            if crate::auth::get_user(txn, &username).await?.is_some() {
                return Err(FusionError::Execution(format!("User '{}' already exists", username)));
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
                return Err(FusionError::Execution("Syntax: DROP USER [IF EXISTS] <name>".to_string()));
            }
            let username = parts[name_idx].to_lowercase();

            if crate::auth::get_user(txn, &username).await?.is_none() {
                if if_exists {
                    return Ok(QueryResult::Success { message: format!("User '{}' does not exist (IF EXISTS)", username) });
                }
                return Err(FusionError::Execution(format!("User '{}' does not exist", username)));
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
            let on_pos = upper.find(" ON ").ok_or_else(|| FusionError::Execution("Syntax: GRANT <privilege> ON <table> TO <user>".to_string()))?;
            let to_pos = upper.find(" TO ").ok_or_else(|| FusionError::Execution("Syntax: GRANT <privilege> ON <table> TO <user>".to_string()))?;

            let privs_str = sql[6..on_pos].trim();
            let table = sql[on_pos + 4..to_pos].trim();
            let username = sql[to_pos + 4..].trim().to_lowercase();

            let privileges: Vec<String> = privs_str.split(',').map(|s| s.trim().to_uppercase()).collect();

            let mut record = crate::auth::get_user(txn, &username).await?
                .ok_or_else(|| FusionError::Execution(format!("User '{}' does not exist", username)))?;

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
            let on_pos = upper.find(" ON ").ok_or_else(|| FusionError::Execution("Syntax: REVOKE <privilege> ON <table> FROM <user>".to_string()))?;
            let from_pos = upper.find(" FROM ").ok_or_else(|| FusionError::Execution("Syntax: REVOKE <privilege> ON <table> FROM <user>".to_string()))?;

            let privs_str = sql[7..on_pos].trim();
            let table = sql[on_pos + 4..from_pos].trim();
            let username = sql[from_pos + 6..].trim().to_lowercase();

            let privileges: Vec<String> = privs_str.split(',').map(|s| s.trim().to_uppercase()).collect();

            let mut record = crate::auth::get_user(txn, &username).await?
                .ok_or_else(|| FusionError::Execution(format!("User '{}' does not exist", username)))?;

            for priv_name in &privileges {
                record.revoke(table, priv_name);
            }
            crate::auth::save_user(txn, &username, &record).await?;

            return Ok(QueryResult::Success {
                message: format!("Revoked {} on {} from {}", privs_str, table, username),
            });
        }

        Err(FusionError::Execution(format!("Unsupported RBAC statement: {}", sql)))
    }

    fn extract_quoted_string(&self, sql: &str, keyword: &str) -> Result<String> {
        let upper = sql.to_uppercase();
        let kw_pos = upper.find(keyword)
            .ok_or_else(|| FusionError::Execution(format!("Missing {} keyword", keyword)))?;
        let after_kw = &sql[kw_pos + keyword.len()..].trim_start();
        // Find the quoted string (single quotes)
        if let Some(start) = after_kw.find('\'') {
            if let Some(end) = after_kw[start + 1..].find('\'') {
                return Ok(after_kw[start + 1..start + 1 + end].to_string());
            }
        }
        Err(FusionError::Execution(format!("Expected quoted string after {}", keyword)))
    }
}
