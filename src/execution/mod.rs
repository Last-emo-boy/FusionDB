use std::sync::Arc;
use parking_lot::RwLock;
use moka::sync::Cache;
use std::collections::{HashSet, HashMap};
use crate::storage::{Storage, Transaction, vector_index::VectorIndex};
use crate::common::{Result, FusionError, Value};
use crate::catalog::{TableSchema, Column, IndexType};
use crate::parser::parse_sql;
use crate::monitor;
use crate::common::encoding::encode_key;
use sqlparser::ast::{Statement, SetExpr, TableFactor, Expr, Value as SqlValue, BinaryOperator, Function, FunctionArg, FunctionArgExpr, FunctionArguments, OrderByKind, LimitClause, SelectItem};
use std::cmp::Ordering;

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
    storage: Arc<dyn Storage>,
    statement_cache: Cache<String, Vec<Statement>>,
    prepared_statements: RwLock<HashMap<String, Vec<Statement>>>,
    row_cache: Cache<String, Vec<Value>>,
    vector_index: Arc<VectorIndex>,
}

#[derive(Debug, Clone)]
enum AggregateAccumulator {
    Count(i64),
    Sum(f64, bool), // val, is_int
    Avg(f64, i64),  // sum, count
    Min(Option<Value>),
    Max(Option<Value>),
}

impl AggregateAccumulator {
    fn new(func_name: &str) -> Self {
        match func_name.to_uppercase().as_str() {
            "COUNT" => AggregateAccumulator::Count(0),
            "SUM" => AggregateAccumulator::Sum(0.0, true),
            "AVG" => AggregateAccumulator::Avg(0.0, 0),
            "MIN" => AggregateAccumulator::Min(None),
            "MAX" => AggregateAccumulator::Max(None),
            _ => AggregateAccumulator::Count(0),
        }
    }

    fn update(&mut self, val: &Value) {
        match self {
            AggregateAccumulator::Count(c) => {
                if *val != Value::Null {
                    *c += 1;
                }
            },
            AggregateAccumulator::Sum(sum, is_int) => {
                 match val {
                     Value::Integer(i) => *sum += *i as f64,
                     Value::Float(f) => { *is_int = false; *sum += *f; }
                     _ => {}
                 }
            },
            AggregateAccumulator::Avg(sum, count) => {
                 match val {
                     Value::Integer(i) => { *sum += *i as f64; *count += 1; },
                     Value::Float(f) => { *sum += *f; *count += 1; },
                     _ => {}
                 }
            },
             AggregateAccumulator::Min(min) => {
                if *val == Value::Null { return; }
                if min.is_none() {
                    *min = Some(val.clone());
                } else {
                    if let Some(current) = min {
                        if val.compare(current) == Ordering::Less {
                             *min = Some(val.clone());
                        }
                    }
                }
            },
            AggregateAccumulator::Max(max) => {
                if *val == Value::Null { return; }
                if max.is_none() {
                    *max = Some(val.clone());
                } else {
                    if let Some(current) = max {
                        if val.compare(current) == Ordering::Greater {
                             *max = Some(val.clone());
                        }
                    }
                }
            }
        }
    }
    
    fn finalize(&self) -> Value {
        match self {
            AggregateAccumulator::Count(c) => Value::Integer(*c),
            AggregateAccumulator::Sum(sum, is_int) => {
                if *is_int {
                    Value::Integer(*sum as i64)
                } else {
                    Value::Float(*sum)
                }
            },
            AggregateAccumulator::Avg(sum, count) => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Float(*sum / *count as f64)
                }
            },
            AggregateAccumulator::Min(min) => min.clone().unwrap_or(Value::Null),
            AggregateAccumulator::Max(max) => max.clone().unwrap_or(Value::Null),
        }
    }
}

impl Executor {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { 
            storage,
            statement_cache: Cache::new(1000),
            prepared_statements: RwLock::new(HashMap::new()),
            row_cache: Cache::new(10000),
            vector_index: Arc::new(VectorIndex::new()),
        }
    }
    
    pub fn prepare(&self, sql: &str) -> Result<Vec<Statement>> {
        if let Some(stmts) = self.statement_cache.get(sql) {
            return Ok(stmts.clone());
        }
        
        monitor::inc_parse(); // Count cache miss/parse
        let stmts = parse_sql(sql)?;
        self.statement_cache.insert(sql.to_string(), stmts.clone());
        
        Ok(stmts)
    }

    pub fn register_prepared_statement(&self, sql: &str) -> Result<String> {
        monitor::inc_parse(); // Always parses
        let stmts = parse_sql(sql)?;
        let id = uuid::Uuid::new_v4().to_string();
        
        let mut cache = self.prepared_statements.write();
        cache.insert(id.clone(), stmts);
        
        Ok(id)
    }

    pub fn get_prepared_statement(&self, id: &str) -> Result<Vec<Statement>> {
        let cache = self.prepared_statements.read();
        if let Some(stmts) = cache.get(id) {
            monitor::inc_plan(); // Count usages
            Ok(stmts.clone())
        } else {
            Err(FusionError::Execution(format!("Prepared statement {} not found", id)))
        }
    }

    pub async fn execute_in_transaction(&self, stmt: &Statement, txn: &mut dyn Transaction) -> Result<QueryResult> {
        self.execute_in_transaction_with_params(stmt, txn, &[]).await
    }

    pub async fn execute_in_transaction_with_params(&self, stmt: &Statement, txn: &mut dyn Transaction, params: &[Value]) -> Result<QueryResult> {
        match stmt {
            Statement::CreateTable(create_table) => {
                self.handle_create_table(&create_table.name, &create_table.columns, txn).await
            },
            Statement::Insert(insert) => {
                self.handle_insert(insert.table.to_string(), &insert.source, txn, params).await
            },
            Statement::Query(query) => {
                self.handle_query(query, txn, params).await
            },
            Statement::CreateIndex(create_index) => {
                self.handle_create_index(&create_index.name, &create_index.table_name, &create_index.columns, create_index.unique, &create_index.index_options, txn).await
            },
            Statement::Delete(delete) => {
                self.handle_delete(delete, txn, params).await
            },
            Statement::Update(update) => {
                self.handle_update(update, txn, params).await
            },
            Statement::Explain { statement, analyze, .. } => {
                self.handle_explain(statement, *analyze, txn, params).await
            },
            Statement::Drop { names, if_exists, object_type, .. } => {
                self.handle_drop_table(names, *if_exists, *object_type, txn).await
            },
            Statement::ShowTables { .. } => {
                self.handle_show_tables(txn).await
            },
            Statement::ExplainTable { table_name, .. } => {
                self.handle_describe_table(table_name, txn).await
            },
            _ => Ok(QueryResult::Success { 
                message: "Statement not supported yet".to_string() 
            }),
        }
    }

    async fn handle_describe_table(&self, table_name: &sqlparser::ast::ObjectName, txn: &mut dyn Transaction) -> Result<QueryResult> {
        let table_name_str = table_name.to_string();
        let schema_key = format!("schema:{}", table_name_str);
        if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
             let schema: TableSchema = bincode::deserialize(&schema_bytes).map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;
             
             let mut rows = Vec::new();
             for col in schema.columns {
                 rows.push(vec![
                     Value::String(col.name),
                     Value::String(col.data_type),
                     Value::String(if col.is_primary { "PRI".to_string() } else { "".to_string() }),
                     Value::String(if col.is_indexed { format!("{:?}", col.index_type) } else { "".to_string() }),
                 ]);
             }
             
             Ok(QueryResult::Select {
                 columns: vec!["Field".to_string(), "Type".to_string(), "Key".to_string(), "Index".to_string()],
                 rows,
             })
        } else {
            Err(FusionError::Execution(format!("Table {} not found", table_name_str)))
        }
    }

    async fn handle_show_tables(&self, txn: &mut dyn Transaction) -> Result<QueryResult> {
        let prefix = "schema:";
        let kv_pairs = txn.scan_prefix(prefix.as_bytes()).await?;
        
        let mut tables = Vec::new();
        for (k, _) in kv_pairs {
            if let Ok(key_str) = std::str::from_utf8(&k) {
                if let Some(table_name) = key_str.strip_prefix(prefix) {
                    tables.push(vec![Value::String(table_name.to_string())]);
                }
            }
        }
        
        Ok(QueryResult::Select {
            columns: vec!["Table".to_string()],
            rows: tables,
        })
    }

    async fn handle_explain(&self, stmt: &Statement, analyze: bool, txn: &mut dyn Transaction, params: &[Value]) -> Result<QueryResult> {
        if analyze {
            let start = std::time::Instant::now();
            let _ = Box::pin(self.execute_in_transaction_with_params(stmt, txn, params)).await?;
            let duration = start.elapsed();
            
            let plan = self.explain_statement_plan(stmt, txn).await?;
            let output = format!("Execution Time: {:?}\nPlan:\n{}", duration, plan);
            
            Ok(QueryResult::Select {
                columns: vec!["EXPLAIN ANALYZE".to_string()],
                rows: vec![vec![Value::String(output)]],
            })
        } else {
            let plan = self.explain_statement_plan(stmt, txn).await?;
            Ok(QueryResult::Select {
                columns: vec!["EXPLAIN".to_string()],
                rows: vec![vec![Value::String(plan)]],
            })
        }
    }

    async fn explain_statement_plan(&self, stmt: &Statement, txn: &mut dyn Transaction) -> Result<String> {
         match stmt {
             Statement::Query(query) => self.explain_query(query, txn).await,
             _ => Ok(format!("Statement type not supported for detailed explanation: {}", stmt)),
         }
    }

    async fn explain_query(&self, query: &sqlparser::ast::Query, txn: &mut dyn Transaction) -> Result<String> {
        if let SetExpr::Select(select) = &query.body.as_ref() {
             let mut plan = String::new();
             plan.push_str("SELECT\n");
             
             if let Some(table) = select.from.first() {
                 plan.push_str(&format!("  FROM: {}\n", table.relation));
                 let access_path = self.explain_table_access(&table.relation, &select.selection, txn).await?;
                 plan.push_str(&format!("  Access Path: {}\n", access_path));
                 
                 for join in &table.joins {
                      plan.push_str(&format!("  JOIN: {}\n", join.relation));
                      let join_access = self.explain_table_access(&join.relation, &None, txn).await?;
                    plan.push_str(&format!("    Access Path: {}\n", join_access));
                    plan.push_str(&format!("    Operator: {:?}\n", join.join_operator));
                }
            }
             
             if let Some(selection) = &select.selection {
                 plan.push_str(&format!("  Filter: {}\n", selection));
             }

             if matches!(select.group_by, sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if !exprs.is_empty()) {
                  plan.push_str(&format!("  Group By: {}\n", select.group_by));
             }
             

            if let Some(order_by) = &query.order_by {
                 plan.push_str(&format!("  Order By: {}\n", order_by));
             }
             
             if let Some(limit) = &query.limit_clause {
                 plan.push_str(&format!("  Limit: {}\n", limit));
             }
             
             Ok(plan)
        } else {
            Ok("Complex query (Set Operations?)".to_string())
        }
    }

    async fn explain_table_access(&self, table: &TableFactor, selection: &Option<Expr>, txn: &mut dyn Transaction) -> Result<String> {
         if let TableFactor::Table { name, .. } = table {
             let table_name = name.to_string();
             let schema_key = format!("schema:{}", table_name);
             if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
                  let schema: TableSchema = bincode::deserialize(&schema_bytes).map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;
                  
                  if let Some(sel) = selection {
                      if let Expr::BinaryOp { left, op: BinaryOperator::Eq, .. } = sel {
                          if let Expr::Identifier(ident) = left.as_ref() {
                               if schema.get_column_index(&ident.value) == Some(0) {
                                   return Ok("Primary Key Lookup (Clustered Index)".to_string());
                               }
                          }
                      }
                      
                      let mut used_index = None;
                      self.check_index_usage(sel, &schema, &mut used_index);
                      
                      if let Some(idx_info) = used_index {
                          return Ok(format!("Index Scan using {}", idx_info));
                      }
                  }
                  
                  Ok("Full Table Scan".to_string())
             } else {
                 Ok("Table not found".to_string())
             }
         } else {
             Ok("Unknown Table Factor".to_string())
         }
    }

    fn check_index_usage(&self, expr: &Expr, schema: &TableSchema, result: &mut Option<String>) {
        match expr {
            Expr::BinaryOp { left, op: BinaryOperator::Eq, .. } => {
                 if let Expr::Identifier(ident) = left.as_ref() {
                     if let Some(idx) = schema.get_column_index(&ident.value) {
                         if schema.columns[idx].is_indexed {
                             *result = Some(format!("{} ({:?})", ident.value, schema.columns[idx].index_type));
                         }
                     }
                 }
            },
            Expr::MatchAgainst { columns, .. } => {
                 if !columns.is_empty() {
                      let col = &columns[0];
                      let col_name = col.to_string();
                       if let Some(idx) = schema.get_column_index(&col_name) {
                         if schema.columns[idx].is_indexed {
                             *result = Some(format!("{} (FTS)", col_name));
                         }
                     }
                 }
            },
            Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                self.check_index_usage(left, schema, result);
                if result.is_none() {
                    self.check_index_usage(right, schema, result);
                }
            },
            _ => {}
        }
    }

    async fn handle_create_index(&self, index_name: &Option<sqlparser::ast::ObjectName>, table_name: &sqlparser::ast::ObjectName, columns: &[sqlparser::ast::IndexColumn], unique: bool, index_options: &[sqlparser::ast::IndexOption], txn: &mut dyn Transaction) -> Result<QueryResult> {
        let table_name_str = table_name.to_string();
        let index_name_str = index_name.as_ref().map(|n| n.to_string()).unwrap_or_else(|| format!("idx_{}_{}", table_name_str, uuid::Uuid::new_v4()));
        
        let schema_key = format!("schema:{}", table_name_str);
        let schema_bytes = txn.get(schema_key.as_bytes()).await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let mut schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        let mut target_col_indices = Vec::new();
        for index_col in columns {
            let col_expr = &index_col.column; 
            if let Expr::Identifier(ident) = &col_expr.expr {
                if let Some(idx) = schema.get_column_index(&ident.value) {
                    target_col_indices.push(idx);
                } else {
                    return Err(FusionError::Execution(format!("Column {} not found", ident.value)));
                }
            } else {
                 return Err(FusionError::Execution("Index only supports simple column references".to_string()));
            }
        }
        
        if target_col_indices.len() != 1 {
             return Err(FusionError::Execution("Currently only single-column index is supported".to_string()));
        }
        let col_idx = target_col_indices[0];
        let col_name = schema.columns[col_idx].name.clone();

        let mut index_type = IndexType::BTree;
        for opt in index_options {
            if let sqlparser::ast::IndexOption::Using(algo) = opt {
                if let sqlparser::ast::IndexType::Custom(ident) = algo {
                    if ident.value.eq_ignore_ascii_case("FTS") {
                        index_type = IndexType::FTS;
                    } else if ident.value.eq_ignore_ascii_case("HNSW") {
                        index_type = IndexType::HNSW;
                    }
                }
            }
        }

        schema.columns[col_idx].is_indexed = true;
        schema.columns[col_idx].index_type = index_type.clone();
        
        // If HNSW, initialize the vector index
        if index_type == IndexType::HNSW {
            let idx_name = format!("hnsw_{}_{}", table_name_str, col_name);
            self.vector_index.create_index(&idx_name);
        }
        let new_schema_value = bincode::serialize(&schema)
            .map_err(|e| FusionError::Execution(format!("Schema serialization error: {}", e)))?;
        txn.put(schema_key.as_bytes(), &new_schema_value).await?;

        let prefix = format!("data:{}:", table_name_str);
        let kv_pairs = txn.scan_prefix(prefix.as_bytes()).await?;

        let mut count = 0;
        for (k, v) in kv_pairs {
            let parts: Vec<&str> = std::str::from_utf8(&k).unwrap().split(':').collect();
            let row_id = parts.last().unwrap();
            
            let row: Vec<Value> = bincode::deserialize(&v)
                .map_err(|e| FusionError::Execution(format!("Data deserialization error: {}", e)))?;
            let val = &row[col_idx];
            
            if index_type == IndexType::FTS {
                if let Value::String(text) = val {
                    let tokens = Self::tokenize(text);
                    let unique_tokens: HashSet<String> = tokens.into_iter().collect();
                    for token in unique_tokens {
                        let index_key = format!("fts:{}:{}:{}:{}", table_name_str, col_name, token, row_id);
                        txn.put(index_key.as_bytes(), &[]).await?;
                    }
                }
            } else if index_type == IndexType::HNSW {
                if let Value::Vector(vec) = val {
                    let idx_name = format!("hnsw_{}_{}", table_name_str, col_name);
                    self.vector_index.insert(&idx_name, row_id.to_string(), vec.clone())?;
                }
            } else {
                let val_str = match val {
                    Value::Integer(i) => i.to_string(),
                    Value::String(s) => s.clone(),
                    Value::Boolean(b) => b.to_string(),
                    _ => continue,
                };
                let index_key = format!("index:{}:{}:{}:{}", table_name_str, col_name, val_str, row_id);
                txn.put(index_key.as_bytes(), &[]).await?;
            }
            count += 1;
        }

        Ok(QueryResult::Success { 
            message: format!("Index {} ({:?}) created on {}({}), indexed {} rows", index_name_str, index_type, table_name_str, col_name, count) 
        })
    }

    pub async fn execute(&self, stmt: &Statement) -> Result<QueryResult> {
        let mut txn = self.storage.begin_transaction().await?;
        let res = self.execute_in_transaction(stmt, &mut *txn).await;
        if res.is_ok() {
            txn.commit().await?;
        } else {
            txn.rollback().await?;
        }
        res
    }

    async fn handle_create_table(&self, name: &sqlparser::ast::ObjectName, columns: &[sqlparser::ast::ColumnDef], txn: &mut dyn Transaction) -> Result<QueryResult> {
        let table_name = name.to_string();
        let cols: Vec<Column> = columns.iter().map(|c| Column {
            name: c.name.to_string(),
            data_type: format!("{}", c.data_type),
            is_primary: false,
            is_indexed: false,
            index_type: IndexType::None,
        }).collect();

        let schema = TableSchema::new(table_name.clone(), cols);
        let key = format!("schema:{}", table_name);
        let value = bincode::serialize(&schema)
            .map_err(|e| FusionError::Execution(format!("Schema serialization error: {}", e)))?;

        txn.put(key.as_bytes(), &value).await?;

        Ok(QueryResult::Success { 
            message: format!("Table {} created", table_name) 
        })
    }

    async fn handle_drop_table(&self, names: &[sqlparser::ast::ObjectName], if_exists: bool, object_type: sqlparser::ast::ObjectType, txn: &mut dyn Transaction) -> Result<QueryResult> {
        if object_type != sqlparser::ast::ObjectType::Table {
            return Err(FusionError::Execution("Only DROP TABLE is supported".to_string()));
        }

        let mut dropped_count = 0;
        for name in names {
            let table_name = name.to_string();
            
            let schema_key = format!("schema:{}", table_name);
            if txn.get(schema_key.as_bytes()).await?.is_none() {
                if if_exists {
                    continue;
                } else {
                    return Err(FusionError::Execution(format!("Table {} does not exist", table_name)));
                }
            }

            txn.delete(schema_key.as_bytes()).await?;

            let prefix = format!("data:{}:", table_name);
            let kv_pairs = txn.scan_prefix(prefix.as_bytes()).await?;
            for (k, _) in kv_pairs {
                txn.delete(&k).await?;
            }

            let index_prefix = format!("index:{}:", table_name);
            let index_entries = txn.scan_prefix(index_prefix.as_bytes()).await?;
            for (k, _) in index_entries {
                txn.delete(&k).await?;
            }

            dropped_count += 1;
        }

        Ok(QueryResult::Success { 
            message: format!("Dropped {} tables", dropped_count) 
        })
    }

    async fn handle_insert(&self, table_name: String, source: &Option<Box<sqlparser::ast::Query>>, txn: &mut dyn Transaction, params: &[Value]) -> Result<QueryResult> {
        let table_name_str = table_name;
        
        let schema_key = format!("schema:{}", table_name_str);
        let schema_bytes = txn.get(schema_key.as_bytes()).await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        if let Some(query) = source {
            if let SetExpr::Values(values) = &query.body.as_ref() {
                let mut count = 0;
                for row in &values.rows {
                    let mut row_values = Vec::new();
                    for (i, expr) in row.iter().enumerate() {
                        let val = self.evaluate_value(expr, &[], &schema, params).unwrap_or(Value::Null);
                        row_values.push(val);
                    }
                    
                    if row_values.len() != schema.columns.len() {
                         return Err(FusionError::Execution("Column count mismatch".to_string()));
                    }

                    let row_id = if let Some(first) = row_values.first() {
                         encode_key(first)
                    } else {
                        uuid::Uuid::new_v4().to_string()
                    };

                    let key = format!("data:{}:{}", table_name_str, row_id);
                    let value = bincode::serialize(&row_values)
                        .map_err(|e| FusionError::Execution(format!("Data serialization error: {}", e)))?;
                    txn.put(key.as_bytes(), &value).await?;
                    monitor::inc_row_write();

                    // Update Cache
                    self.row_cache.insert(key.clone(), row_values.clone());

                    for (idx, col) in schema.columns.iter().enumerate() {
                        if col.is_indexed {
                            let val = &row_values[idx];
                            
                            if col.index_type == IndexType::FTS {
                             if let Value::String(text) = val {
                                 let tokens = Self::tokenize(text);
                                 let unique_tokens: HashSet<String> = tokens.into_iter().collect();
                                 for token in unique_tokens {
                                     let index_key = format!("fts:{}:{}:{}:{}", table_name_str, col.name, token, row_id);
                                     txn.put(index_key.as_bytes(), &[]).await?;
                                 }
                             }
                        } else if col.index_type == IndexType::HNSW {
                            if let Value::Vector(vec) = val {
                                let idx_name = format!("hnsw_{}_{}", table_name_str, col.name);
                                // row_id is String, passing directly
                                self.vector_index.insert(&idx_name, row_id.clone(), vec.clone())?;
                            }
                        } else {
                                if let Some(val_str) = self.value_to_index_string(val) {
                                    let index_key = format!("index:{}:{}:{}:{}", table_name_str, col.name, val_str, row_id);
                                    txn.put(index_key.as_bytes(), &[]).await?;
                                }
                            }
                        }
                    }
                    
                    count += 1;
                }
                return Ok(QueryResult::Success { message: format!("Inserted {} rows", count) });
            }
        }
        
        Err(FusionError::Execution("Unsupported INSERT format".to_string()))
    }

    async fn handle_delete(&self, delete: &sqlparser::ast::Delete, txn: &mut dyn Transaction, params: &[Value]) -> Result<QueryResult> {
        let table_name_str = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables) => {
                if let Some(table) = tables.first() {
                    if let TableFactor::Table { name, .. } = &table.relation {
                        name.to_string()
                    } else {
                        return Err(FusionError::Execution("Unsupported DELETE format".to_string()));
                    }
                } else {
                    return Err(FusionError::Execution("Missing table in DELETE".to_string()));
                }
            },
             sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                 if let Some(table) = tables.first() {
                    if let TableFactor::Table { name, .. } = &table.relation {
                        name.to_string()
                    } else {
                        return Err(FusionError::Execution("Unsupported DELETE format".to_string()));
                    }
                } else {
                    return Err(FusionError::Execution("Missing table in DELETE".to_string()));
                }
             }
        };

        let schema_key = format!("schema:{}", table_name_str);
        let schema_bytes = txn.get(schema_key.as_bytes()).await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        let prefix = format!("data:{}:", table_name_str);
        let kv_pairs = txn.scan_prefix(prefix.as_bytes()).await?;

        let mut deleted_count = 0;
        for (k, v) in kv_pairs {
            let row: Vec<Value> = bincode::deserialize(&v)
                .map_err(|e| FusionError::Execution(format!("Data deserialization error: {}", e)))?;
            
            let mut delete_flag = true;
            if let Some(selection) = &delete.selection {
                delete_flag = self.evaluate_expr(selection, &row, &schema, params)?;
            }

            if delete_flag {
                txn.delete(&k).await?;
                
                // Invalidate Cache
                if let Ok(key_str) = std::str::from_utf8(&k) {
                    self.row_cache.invalidate(key_str);
                }

                let parts: Vec<&str> = std::str::from_utf8(&k).unwrap().split(':').collect();
                let row_id = parts.last().unwrap();

                for (idx, col) in schema.columns.iter().enumerate() {
                    if col.is_indexed {
                        let val = &row[idx];
                        
                        if col.index_type == IndexType::FTS {
                            if let Value::String(text) = val {
                                let tokens = Self::tokenize(text);
                                let unique_tokens: HashSet<String> = tokens.into_iter().collect();
                                for token in unique_tokens {
                                    let index_key = format!("fts:{}:{}:{}:{}", table_name_str, col.name, token, row_id);
                                    txn.delete(index_key.as_bytes()).await?;
                                }
                            }
                        } else if col.index_type == IndexType::HNSW {
                            // Deletion from HNSW is complex and often not fully supported or expensive.
                            // For this MVP, we might skip explicit deletion or implement a soft delete in VectorIndex.
                            // self.vector_index.delete(...) // TODO
                        } else {
                            if let Some(val_str) = self.value_to_index_string(val) {
                                let index_key = format!("index:{}:{}:{}:{}", table_name_str, col.name, val_str, row_id);
                                txn.delete(index_key.as_bytes()).await?;
                            }
                        }
                    }
                }
                
                deleted_count += 1;
            }
        }

        Ok(QueryResult::Success { 
            message: format!("Deleted {} rows", deleted_count) 
        })
    }

    async fn handle_update(
        &self,
        update: &sqlparser::ast::Update,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<QueryResult> {
        let table_name_str = match &update.table {
            sqlparser::ast::TableWithJoins { relation, .. } => {
                if let TableFactor::Table { name, .. } = relation {
                    name.to_string()
                } else {
                     return Err(FusionError::Execution("Unsupported UPDATE format".to_string()));
                }
            }
        };

        let schema_key = format!("schema:{}", table_name_str);
        let schema_bytes = txn.get(schema_key.as_bytes()).await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        let prefix = format!("data:{}:", table_name_str);
        let kv_pairs = txn.scan_prefix(prefix.as_bytes()).await?;

        let mut updated_count = 0;
        for (k, v) in kv_pairs {
            let mut row: Vec<Value> = bincode::deserialize(&v)
                .map_err(|e| FusionError::Execution(format!("Data deserialization error: {}", e)))?;
            let old_row = row.clone(); 
            
            let mut update_flag = true;
            if let Some(selection_expr) = &update.selection {
                update_flag = self.evaluate_expr(selection_expr, &row, &schema, params)?;
            }

            if update_flag {
                for assignment in &update.assignments {
                    let col_name = match &assignment.target {
                        sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                            name.to_string()
                        },
                         _ => return Err(FusionError::Execution("Unsupported assignment target".to_string())),
                    };

                    if let Some(col_idx) = schema.get_column_index(&col_name) {
                        let new_val = self.evaluate_value(&assignment.value, &old_row, &schema, params)?;
                        row[col_idx] = new_val;
                    } else {
                         return Err(FusionError::Execution(format!("Column {} not found in assignment", col_name)));
                    }
                }

                let new_value_bytes = bincode::serialize(&row)
                    .map_err(|e| FusionError::Execution(format!("Data serialization error: {}", e)))?;
                txn.put(&k, &new_value_bytes).await?;

                // Update Cache
                if let Ok(key_str) = std::str::from_utf8(&k) {
                    self.row_cache.insert(key_str.to_string(), row.clone());
                }

                let parts: Vec<&str> = std::str::from_utf8(&k).unwrap().split(':').collect();
                let row_id = parts.last().unwrap();

                for (idx, col) in schema.columns.iter().enumerate() {
                    if col.is_indexed {
                        let old_val = &old_row[idx];
                        let new_val = &row[idx];

                        if old_val != new_val {
                            if col.index_type == IndexType::FTS {
                                if let Value::String(text) = old_val {
                                    let tokens = Self::tokenize(text);
                                    for token in tokens {
                                        let index_key = format!("fts:{}:{}:{}:{}", table_name_str, col.name, token, row_id);
                                        txn.delete(index_key.as_bytes()).await?;
                                    }
                                }
                                if let Value::String(text) = new_val {
                                    let tokens = Self::tokenize(text);
                                    let unique_tokens: HashSet<String> = tokens.into_iter().collect();
                                    for token in unique_tokens {
                                        let index_key = format!("fts:{}:{}:{}:{}", table_name_str, col.name, token, row_id);
                                        txn.put(index_key.as_bytes(), &[]).await?;
                                    }
                                }
                            } else {
                                if let Some(old_val_str) = self.value_to_index_string(old_val) {
                                    let old_index_key = format!("index:{}:{}:{}:{}", table_name_str, col.name, old_val_str, row_id);
                                    txn.delete(old_index_key.as_bytes()).await?;
                                }

                                if let Some(new_val_str) = self.value_to_index_string(new_val) {
                                    let new_index_key = format!("index:{}:{}:{}:{}", table_name_str, col.name, new_val_str, row_id);
                                    txn.put(new_index_key.as_bytes(), &[]).await?;
                                }
                            }
                        }
                    }
                }
                
                updated_count += 1;
            }
        }

        Ok(QueryResult::Success { 
            message: format!("Updated {} rows", updated_count) 
        })
    }

    fn value_to_index_string(&self, val: &Value) -> Option<String> {
        match val {
            Value::Integer(i) => Some(crate::common::encoding::encode_i64_comparable(*i)),
            Value::String(s) => Some(s.clone()),
            Value::Boolean(b) => Some(b.to_string()),
            _ => None,
        }
    }

    async fn scan_table_base(&self, relation: &TableFactor, txn: &mut dyn Transaction) -> Result<(TableSchema, Vec<Vec<Value>>)> {
         if let TableFactor::Table { name, .. } = relation {
             let table_name = name.to_string();
             let schema_key = format!("schema:{}", table_name);
             let schema_bytes = txn.get(schema_key.as_bytes()).await?
                 .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name)))?;
             let schema: TableSchema = bincode::deserialize(&schema_bytes)
                 .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;
             
             let prefix = format!("data:{}:", table_name);
             let kv_pairs = txn.scan_prefix(prefix.as_bytes()).await?;
             let mut rows = Vec::new();
             for (_, v) in kv_pairs {
                 let row: Vec<Value> = bincode::deserialize(&v)
                     .map_err(|e| FusionError::Execution(format!("Data deserialization error: {}", e)))?;
                 rows.push(row);
             }
             Ok((schema, rows))
         } else {
             Err(FusionError::Execution("Unsupported table factor".to_string()))
         }
    }

    fn prefix_schema_columns(&self, schema: &mut TableSchema, relation: &TableFactor) -> Result<()> {
         if let TableFactor::Table { name, alias, .. } = relation {
             let prefix = if let Some(a) = alias {
                 a.name.value.clone()
             } else {
                 name.to_string()
             };
             
             for col in &mut schema.columns {
                 col.name = format!("{}.{}", prefix, col.name);
             }
             Ok(())
         } else {
             Ok(())
         }
    }

    async fn execute_join(&self, from: &[sqlparser::ast::TableWithJoins], selection: &Option<Expr>, txn: &mut dyn Transaction, params: &[Value]) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        let first = &from[0];
        let (mut schema, mut rows) = self.scan_table_base(&first.relation, txn).await?;
        
        self.prefix_schema_columns(&mut schema, &first.relation)?;

        for join in &first.joins {
             let (right_schema_base, right_rows) = self.scan_table_base(&join.relation, txn).await?;
             let mut right_schema = right_schema_base;
             self.prefix_schema_columns(&mut right_schema, &join.relation)?;

             let mut new_columns = schema.columns.clone();
             new_columns.extend(right_schema.columns.clone());
             let new_schema = TableSchema::new("join_result".to_string(), new_columns);

             let mut new_rows = Vec::new();
             let is_left_outer = matches!(join.join_operator, sqlparser::ast::JoinOperator::LeftOuter(_) | sqlparser::ast::JoinOperator::Left(_));

             // Try Hash Join Optimization
             let mut hash_join_executed = false;
             
             // Check if we can use Hash Join (Simple Equi-Join)
             if let sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(expr)) | 
                    sqlparser::ast::JoinOperator::LeftOuter(sqlparser::ast::JoinConstraint::On(expr)) |
                    sqlparser::ast::JoinOperator::Left(sqlparser::ast::JoinConstraint::On(expr)) |
                    sqlparser::ast::JoinOperator::Join(sqlparser::ast::JoinConstraint::On(expr)) = &join.join_operator {
                 
                 // println!("Checking Hash Join eligibility for join...");
                 if let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = expr {
                     let l_name = match left.as_ref() {
                         Expr::Identifier(i) => Some(i.value.clone()),
                         Expr::CompoundIdentifier(ids) => Some(ids.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")),
                         _ => None
                     };
                     
                     let r_name = match right.as_ref() {
                         Expr::Identifier(i) => Some(i.value.clone()),
                         Expr::CompoundIdentifier(ids) => Some(ids.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")),
                         _ => None
                     };

                     // println!("Left: {:?}, Right: {:?}", l_name, r_name);

                     if let (Some(ln), Some(rn)) = (l_name, r_name) {
                         // Resolve columns to see if they split between left and right tables
                         let l_idx_opt = self.resolve_column_index(&ln, &schema).ok();
                         let r_idx_opt = self.resolve_column_index(&rn, &right_schema).ok();
                         
                         // println!("L_idx: {:?}, R_idx: {:?}", l_idx_opt, r_idx_opt);
                         
                         // Determine which side is which
                         // Case 1: left expr is from left table, right expr is from right table
                         let (left_key_idx, right_key_idx) = if let (Some(l_idx), Some(r_idx)) = (l_idx_opt, r_idx_opt) {
                             (l_idx, r_idx)
                         } else {
                             // Case 2: left expr is from right table, right expr is from left table
                             let l_idx_rev = self.resolve_column_index(&ln, &right_schema).ok();
                             let r_idx_rev = self.resolve_column_index(&rn, &schema).ok();
                             if let (Some(l_idx), Some(r_idx)) = (l_idx_rev, r_idx_rev) {
                                 (r_idx, l_idx) // (left_schema_idx, right_schema_idx)
                             } else {
                                 (usize::MAX, usize::MAX)
                             }
                         };

                         if left_key_idx != usize::MAX {
                             // Perform Hash Join
                             hash_join_executed = true;
                             monitor::inc_plan(); // Reuse plan counter or add new metric for hash join?
                             
                             // Build Phase (Right Table)
                             // Map: Value -> Vec<Row>
                             let mut hash_map: HashMap<Value, Vec<&Vec<Value>>> = HashMap::new();
                             for r_row in &right_rows {
                                 let key = r_row[right_key_idx].clone();
                                 hash_map.entry(key).or_default().push(r_row);
                             }

                             // Probe Phase (Left Table)
                             for l_row in &rows {
                                 let key = &l_row[left_key_idx];
                                 if let Some(matches) = hash_map.get(key) {
                                     for r_row in matches {
                                         let mut joined_row = l_row.clone();
                                         joined_row.extend((*r_row).clone());
                                         new_rows.push(joined_row);
                                     }
                                 } else if is_left_outer {
                                     let mut joined_row = l_row.clone();
                                     for _ in 0..right_schema.columns.len() {
                                         joined_row.push(Value::Null);
                                     }
                                     new_rows.push(joined_row);
                                 }
                             }
                         }
                     }
                 }
              }

              if !hash_join_executed {
                 // Fallback to Nested Loop Join
                 for left_row in &rows {
                     let mut matched = false;
                     for right_row in &right_rows {
                         let mut joined_row = left_row.clone();
                         joined_row.extend(right_row.clone());
                         
                         let mut match_join = true;
                         match &join.join_operator {
                             sqlparser::ast::JoinOperator::Inner(constraint) | 
                             sqlparser::ast::JoinOperator::LeftOuter(constraint) | 
                             sqlparser::ast::JoinOperator::Left(constraint) |
                             sqlparser::ast::JoinOperator::RightOuter(constraint) | 
                             sqlparser::ast::JoinOperator::Right(constraint) |
                             sqlparser::ast::JoinOperator::FullOuter(constraint) | 
                             sqlparser::ast::JoinOperator::Join(constraint) => {
                                 match constraint {
                                     sqlparser::ast::JoinConstraint::On(expr) => {
                                          let res = self.evaluate_expr(expr, &joined_row, &new_schema, params)?;
                                          if !res {
                                              match_join = false;
                                          }
                                     },
                                     _ => {} 
                                 }
                             },
                             sqlparser::ast::JoinOperator::CrossJoin(_) => {}, 
                             _ => {}, 
                         }
                         
                         if match_join {
                             new_rows.push(joined_row);
                             matched = true;
                         }
                     }
                     
                     if !matched && is_left_outer {
                         let mut joined_row = left_row.clone();
                         for _ in 0..right_schema.columns.len() {
                              joined_row.push(Value::Null);
                         }
                         new_rows.push(joined_row);
                     }
                 }
             }
             
             schema = new_schema;
             rows = new_rows;
        }

        for table in from.iter().skip(1) {
             let (right_schema_base, right_rows) = self.scan_table_base(&table.relation, txn).await?;
             let mut right_schema = right_schema_base;
             self.prefix_schema_columns(&mut right_schema, &table.relation)?;

             let mut new_columns = schema.columns.clone();
             new_columns.extend(right_schema.columns.clone());
             let new_schema = TableSchema::new("join_result".to_string(), new_columns);

             let mut new_rows = Vec::new();
             for left_row in &rows {
                 for right_row in &right_rows {
                     let mut joined_row = left_row.clone();
                     joined_row.extend(right_row.clone());
                     new_rows.push(joined_row);
                 }
             }
             schema = new_schema;
             rows = new_rows;
        }

        if let Some(expr) = selection {
            let mut filtered_rows = Vec::new();
            for row in rows {
                if self.evaluate_expr(expr, &row, &schema, params)? {
                    filtered_rows.push(row);
                }
            }
            rows = filtered_rows;
        }

        Ok((schema, rows))
    }
    
    fn try_index_scan<'a>(&'a self, expr: &'a Expr, table_name: &'a str, schema: &'a TableSchema, txn: &'a mut dyn Transaction, params: &'a [Value]) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<HashSet<String>>>> + Send + 'a>> {
        Box::pin(async move {
            match expr {
                Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
                    let col_name = if let Expr::Identifier(ident) = left.as_ref() {
                        Some(ident.value.clone())
                    } else { None };

                    if let Some(col_name) = col_name {
                        if let Some(col_idx) = schema.get_column_index(&col_name) {
                            if schema.columns[col_idx].is_indexed && schema.columns[col_idx].index_type == IndexType::BTree {
                                let val = self.evaluate_value(right, &[], &schema, params).unwrap_or(Value::Null);
                                if let Some(val_str) = self.value_to_index_string(&val) {
                                    let index_prefix = format!("index:{}:{}:{}:", table_name, col_name, val_str);
                                    let index_entries = txn.scan_prefix(index_prefix.as_bytes()).await?;
                                    
                                    let mut row_ids = HashSet::new();
                                    for (k, _) in index_entries {
                                        let parts: Vec<&str> = std::str::from_utf8(&k).unwrap().split(':').collect();
                                        let row_id = parts.last().unwrap();
                                        row_ids.insert(row_id.to_string());
                                    }
                                    return Ok(Some(row_ids));
                                }
                            }
                        }
                    }
                },
                Expr::MatchAgainst { columns, match_value, .. } => {
                    if columns.len() == 1 {
                            let col_ident = &columns[0];
                            let col_name = col_ident.to_string();
                            
                            if let Some(col_idx) = schema.get_column_index(&col_name) {
                                if schema.columns[col_idx].is_indexed && schema.columns[col_idx].index_type == IndexType::FTS {
                                    monitor::inc_fts_search();
                                    let match_val = if let SqlValue::SingleQuotedString(s) = match_value {
                                        Value::String(s.clone())
                                    } else if let SqlValue::Placeholder(p) = match_value {
                                        let idx = p.replace("$", "").parse::<usize>().unwrap_or(0);
                                        if idx > 0 && idx <= params.len() {
                                            params[idx - 1].clone()
                                        } else {
                                            Value::Null
                                        }
                                    } else {
                                        Value::Null
                                    };

                                    if let Value::String(query_text) = match_val {
                                        let tokens = Self::tokenize(&query_text);
                                        if !tokens.is_empty() {
                                            let mut candidate_row_ids: Option<HashSet<String>> = None;
                                            
                                            for token in tokens {
                                                let index_prefix = format!("fts:{}:{}:{}:", table_name, col_name, token);
                                                let index_entries = txn.scan_prefix(index_prefix.as_bytes()).await?;
                                                
                                                let mut current_token_row_ids = HashSet::new();
                                                for (k, _) in index_entries {
                                                    let parts: Vec<&str> = std::str::from_utf8(&k).unwrap().split(':').collect();
                                                    let row_id = parts.last().unwrap();
                                                    current_token_row_ids.insert(row_id.to_string());
                                                }
                                                
                                                if let Some(candidates) = candidate_row_ids {
                                                    candidate_row_ids = Some(candidates.intersection(&current_token_row_ids).cloned().collect());
                                                } else {
                                                    candidate_row_ids = Some(current_token_row_ids);
                                                }
                                                
                                                if candidate_row_ids.as_ref().unwrap().is_empty() {
                                                    return Ok(Some(HashSet::new()));
                                                }
                                            }
                                            if let Some(res) = &candidate_row_ids {
                                                monitor::add_fts_hits(res.len() as u64);
                                            }
                                            return Ok(candidate_row_ids);
                                        }
                                    }
                                }
                            }
                    }
                },
                Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                    let left_res = self.try_index_scan(left, table_name, schema, txn, params).await?;
                    if left_res.is_some() {
                        return Ok(left_res);
                    }
                    
                    let right_res = self.try_index_scan(right, table_name, schema, txn, params).await?;
                    if right_res.is_some() {
                        return Ok(right_res);
                    }
                },
                Expr::Nested(inner) => {
                    return self.try_index_scan(inner, table_name, schema, txn, params).await;
                }
                _ => {}
            }
            Ok(None)
        })
    }

    async fn scan_single_table(&self, table: &TableFactor, selection: &Option<Expr>, txn: &mut dyn Transaction, params: &[Value], limit: Option<usize>, order_by: Option<&sqlparser::ast::OrderBy>) -> Result<(TableSchema, Vec<Vec<Value>>)> {
         if let TableFactor::Table { name, .. } = table {
             let table_name = name.to_string();
             let schema_key = format!("schema:{}", table_name);
             let schema_bytes = txn.get(schema_key.as_bytes()).await?
                 .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name)))?;
             let schema: TableSchema = bincode::deserialize(&schema_bytes)
                 .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;
             
             let mut rows = Vec::new();
             let mut index_used = false;

             // Optimization: Vector Search (HNSW)
             if let Some(order_by) = order_by {
                 if let Some(l) = limit {
                     // Using order_by.exprs. If unavailable, trying order_by.kind (assuming error message guide)
                     // Actually, if 'kind' is available, let's try to match it.
                     // But first, let's just try to access it as 'exprs' again in case I fixed the import?
                     // No, error was specific.
                     // Let's try to assume 'exprs' is inside 'order_by' directly.
                     // Wait, if I use order_by.exprs and it fails, I'll try order_by.kind.
                     // I will try order_by.exprs first.
                     // Wait, I already tried that and it failed.
                     // So I will try order_by.kind.
                     // I'll assume 'kind' is a Vec<OrderByExpr> or similar.
                     if l > 0 {
                         // Temporary workaround to inspect type:
                         // let _ = &order_by.kind; 
                         // But I want to fix it.
                         // Let's assume order_by.exprs is correct for older versions but here we have 0.60.
                         // I will try to iterate over order_by.exprs.
                         // Maybe I need to clone it? No.
                         
                         // I will try to use `order_by.exprs` but cast it? No.
                         // I will try `order_by.kind`.
                          if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
                              if exprs.len() == 1 {
                             let sort_expr = &exprs[0].expr;
                             let mut vector_search_args = None; // (col_name, query_val_expr)

                             // Case 1: <-> operator
                             if let Expr::BinaryOp { left, op: BinaryOperator::Custom(op_str), right } = sort_expr {
                                 if op_str == "<->" {
                                     if let Expr::Identifier(ident) = left.as_ref() {
                                          vector_search_args = Some((ident.value.clone(), right.as_ref().clone()));
                                     }
                                 }
                             } 
                             // Case 2: VECTOR_DISTANCE function
                             else if let Expr::Function(func) = sort_expr {
                                 if func.name.to_string().to_uppercase() == "VECTOR_DISTANCE" {
                                      if let FunctionArguments::List(args) = &func.args {
                                          if args.args.len() == 2 {
                                              if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) = &args.args[0] {
                                                   if let FunctionArg::Unnamed(FunctionArgExpr::Expr(val_expr)) = &args.args[1] {
                                                        vector_search_args = Some((ident.value.clone(), val_expr.clone()));
                                                   }
                                              }
                                          }
                                      }
                                 }
                             }

                             if let Some((col_name, query_expr)) = vector_search_args {
                                 if let Some(idx) = schema.get_column_index(&col_name) {
                                     if schema.columns[idx].index_type == IndexType::HNSW {
                                          let query_val = self.evaluate_value(&query_expr, &[], &schema, params)?;
                                          if let Value::Vector(query_vec) = query_val {
                                              let idx_name = format!("hnsw_{}_{}", table_name, col_name);
                                              // HNSW Search
                                              let search_results = self.vector_index.search(&idx_name, &query_vec, l)?;
                                              
                                              for (id, _dist) in search_results {
                                                   let key = format!("data:{}:{}", table_name, id);
                                                   if let Some(row) = self.row_cache.get(&key) {
                                                       rows.push(row);
                                                   } else {
                                                        if let Some(data) = txn.get(key.as_bytes()).await? {
                                                             if let Ok(row) = bincode::deserialize::<Vec<Value>>(&data) {
                                                                 self.row_cache.insert(key, row.clone());
                                                                 rows.push(row);
                                                             }
                                                        }
                                                   }
                                              }
                                              index_used = true;
                                          }
                                     }
                                 }
                             }
                         }
                     }
                 }
             }
             }

             // Optimization: Check for Primary Key (Clustered Index) Lookup
             // We assume the first column (index 0) is the clustered key
             if let Some(sel) = selection {
                 if let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = sel {
                     // Check if left is col 0
                     let is_pk = if let Expr::Identifier(ident) = left.as_ref() {
                          self.resolve_column_index(&ident.value, &schema).ok() == Some(0)
                     } else { false };

                     if is_pk {
                          // Evaluate right side
                          let val = self.evaluate_value(right, &[], &schema, params).unwrap_or(Value::Null);
                          let row_id = match val {
                              Value::Integer(i) => Some(crate::common::encoding::encode_i64_comparable(i)),
                              Value::String(s) => Some(s),
                              _ => None,
                          };
                          
                          if let Some(id) = row_id {
                              let key = format!("data:{}:{}", table_name, id);
                              
                              // Check Cache
                              if let Some(row) = self.row_cache.get(&key) {
                                  // Re-check condition to be safe
                                  if self.evaluate_expr(sel, &row, &schema, params)? {
                                      monitor::inc_row_cache_hit();
                                      return Ok((schema, vec![row.clone()]));
                                  }
                                  monitor::inc_row_cache_hit(); // Hit but filtered
                                  return Ok((schema, vec![]));
                              }

                              if let Some(v) = txn.get(key.as_bytes()).await? {
                                   monitor::inc_row_read();
                                   let row: Vec<Value> = bincode::deserialize(&v)
                                       .map_err(|e| FusionError::Execution(format!("Data deserialization error: {}", e)))?;
                                   
                                   // Update Cache
                                   self.row_cache.insert(key, row.clone());

                                   // Re-check condition to be safe
                                   if self.evaluate_expr(sel, &row, &schema, params)? {
                                       return Ok((schema, vec![row]));
                                   }
                              }
                              return Ok((schema, vec![]));
                          }
                     }
                 }
             }

             // Try Index Scan
             if let Some(sel) = selection {
                if let Some(row_ids) = self.try_index_scan(sel, &table_name, &schema, txn, params).await? {
                    index_used = true;
                    for row_id in row_ids {
                        let data_key = format!("data:{}:{}", table_name, row_id);
                        
                        // Check Cache
                        if let Some(row) = self.row_cache.get(&data_key) {
                            monitor::inc_row_cache_hit();
                            rows.push(row);
                        } else {
                            if let Some(data_bytes) = txn.get(data_key.as_bytes()).await? {
                                monitor::inc_row_read();
                                let row: Vec<Value> = bincode::deserialize(&data_bytes)
                                    .map_err(|e| FusionError::Execution(format!("Data deserialization error: {}", e)))?;
                                
                                // Update Cache
                        self.row_cache.insert(data_key, row.clone());
                        
                        rows.push(row);
                    }
                }
                if let Some(l) = limit {
                    if rows.len() >= l {
                        break;
                    }
                }
            }
        }
     }

     if !index_used {
                 let prefix = format!("data:{}:", table_name);
                 let kv_pairs = txn.scan_prefix(prefix.as_bytes()).await?;
                 for (_, v) in kv_pairs {
                     let row: Vec<Value> = bincode::deserialize(&v)
                         .map_err(|e| FusionError::Execution(format!("Data deserialization error: {}", e)))?;
                     rows.push(row);
                     // Note: We can't apply LIMIT here easily because we haven't applied WHERE (selection) yet.
                     // Unless selection is None.
                     if selection.is_none() {
                        if let Some(l) = limit {
                            if rows.len() >= l {
                                break;
                            }
                        }
                     }
                 }
             }
             
             // Apply WHERE (Always apply to ensure non-index conditions are met)
             if let Some(sel) = selection {
                 let mut filtered_rows = Vec::new();
                 for row in rows {
                     if self.evaluate_expr(sel, &row, &schema, params)? {
                         filtered_rows.push(row);
                         if let Some(l) = limit {
                            if filtered_rows.len() >= l {
                                break;
                            }
                         }
                     }
                 }
                 rows = filtered_rows;
             }
             
             Ok((schema, rows))
         } else {
             Err(FusionError::Execution("Unsupported table factor".to_string()))
         }
    }

    async fn handle_query(&self, query: &sqlparser::ast::Query, txn: &mut dyn Transaction, params: &[Value]) -> Result<QueryResult> {
        if let SetExpr::Select(select) = &query.body.as_ref() {
            let is_join = select.from.len() > 1 || (!select.from.is_empty() && !select.from[0].joins.is_empty());
            
            // Extract Limit
            let (limit, offset) = match &query.limit_clause {
                Some(LimitClause::LimitOffset { limit, offset, .. }) => {
                    let limit = if let Some(limit_expr) = limit {
                        match limit_expr {
                            Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Number(n, _), .. }) => Some(n.parse::<usize>().unwrap_or(usize::MAX)),
                            _ => None,
                        }
                    } else { None };

                    let offset = if let Some(offset_struct) = offset {
                        match &offset_struct.value {
                            Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Number(n, _), .. }) => n.parse::<usize>().unwrap_or(0),
                            _ => 0,
                        }
                    } else { 0 };
                    
                    (limit, offset)
                },
                _ => (None, 0),
            };

            // Push down limit only if no ORDER BY and no GROUP BY (simplified)
            let is_group_by_none = matches!(select.group_by, sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty());
            let push_down_limit = if query.order_by.is_none() && is_group_by_none {
                if let Some(l) = limit {
                    Some(l + offset) // Must fetch limit + offset
                } else {
                    None
                }
            } else {
                None
            };

            let (mut schema, mut rows) = if is_join {
                self.execute_join(&select.from, &select.selection, txn, params).await?
            } else if let Some(table) = select.from.first() {
                self.scan_single_table(&table.relation, &select.selection, txn, params, push_down_limit, query.order_by.as_ref()).await?
            } else {
                 return Ok(QueryResult::Select { columns: vec![], rows: vec![] });
            };

            let mut columns = Vec::new();
            let mut is_count_star = false;

            let is_wildcard = select.projection.iter().any(|item| matches!(item, SelectItem::Wildcard(_)));
            
            if is_wildcard {
                 columns = schema.columns.iter().map(|c| c.name.clone()).collect();
            } else {
                if select.projection.len() == 1 {
                    let expr = match &select.projection[0] {
                        SelectItem::UnnamedExpr(expr) => Some(expr),
                        SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                        _ => None,
                    };

                    if let Some(Expr::Function(func)) = expr {
                        if func.name.to_string().to_uppercase() == "COUNT" {
                            match &func.args {
                                FunctionArguments::List(args) => {
                                    if args.args.len() == 1 {
                                        if let FunctionArg::Unnamed(FunctionArgExpr::Wildcard) = &args.args[0] {
                                            is_count_star = true;
                                            columns.push("COUNT(*)".to_string());
                                        }
                                    }
                                },
                                _ => {}
                            }
                        }
                    }
                }

                if !is_count_star {
                    for item in &select.projection {
                        match item {
                            SelectItem::UnnamedExpr(expr) => {
                                if let Expr::Identifier(ident) = expr {
                                    columns.push(ident.value.clone());
                                } else {
                                    columns.push(format!("{}", expr));
                                }
                            },
                            SelectItem::ExprWithAlias { alias, .. } => {
                                columns.push(alias.value.clone());
                            },
                            SelectItem::QualifiedWildcard(_, _) => {
                                 columns = schema.columns.iter().map(|c| c.name.clone()).collect();
                                 break; 
                            }
                            _ => {}
                        }
                    }
                }
            }

            if let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by {
                if !group_exprs.is_empty() {
                    // 1. Identify Aggregates
                    let mut aggregates: Vec<(Expr, String)> = Vec::new(); // (Expr, FuncName)
                    
                    for item in &select.projection {
                        match item {
                            SelectItem::UnnamedExpr(expr) => self.extract_aggregates_from_expr(expr, &mut aggregates),
                            SelectItem::ExprWithAlias { expr, .. } => self.extract_aggregates_from_expr(expr, &mut aggregates),
                            _ => {}
                        }
                    }
                    if let Some(having) = &select.having {
                        self.extract_aggregates_from_expr(having, &mut aggregates);
                    }
                    
                    // 2. Scan and Accumulate
                    // Map: GroupKey -> Vec<Accumulator>
                    let mut groups: std::collections::HashMap<Vec<Value>, Vec<AggregateAccumulator>> = std::collections::HashMap::new();
                    
                    for row in rows {
                         let mut group_key = Vec::new();
                         for expr in group_exprs {
                             let val = self.evaluate_value(expr, &row, &schema, params).unwrap_or(Value::Null);
                             group_key.push(val);
                         }
                         
                         let accs = groups.entry(group_key).or_insert_with(|| {
                             aggregates.iter().map(|(_, name)| AggregateAccumulator::new(name)).collect()
                         });
                         
                         for (i, (expr, _)) in aggregates.iter().enumerate() {
                             // Evaluate argument
                             if let Expr::Function(func) = expr {
                                 let arg_val = if let FunctionArguments::List(args) = &func.args {
                                     if args.args.is_empty() {
                                          Value::Integer(1) 
                                     } else {
                                         if let FunctionArg::Unnamed(FunctionArgExpr::Wildcard) = &args.args[0] {
                                              Value::Integer(1)
                                         } else if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = &args.args[0] {
                                              self.evaluate_value(e, &row, &schema, params).unwrap_or(Value::Null)
                                         } else {
                                              Value::Null
                                         }
                                     }
                                 } else {
                                     Value::Null
                                 };
                                 
                                 accs[i].update(&arg_val);
                             }
                         }
                    }

                    // 3. Finalize and Generate Result Rows
                    let mut grouped_rows = Vec::new();

                    for (group_key, accs) in groups {
                         let mut agg_map = std::collections::HashMap::new();
                         for (i, (expr, _)) in aggregates.iter().enumerate() {
                             agg_map.insert(expr.clone(), accs[i].finalize());
                         }

                         if let Some(having) = &select.having {
                             let val = self.evaluate_final_group_expr(having, &group_key, group_exprs, &agg_map, &schema, params)?;
                             let keep = match val {
                                 Value::Boolean(b) => b,
                                 Value::Null => false, 
                                 _ => return Err(FusionError::Execution("HAVING clause must return boolean".to_string())),
                             };
                             if !keep { continue; }
                         }

                         let mut new_row = Vec::new();
                         
                         for item in &select.projection {
                             let val = match item {
                                 SelectItem::UnnamedExpr(expr) => self.evaluate_final_group_expr(expr, &group_key, group_exprs, &agg_map, &schema, params)?,
                                 SelectItem::ExprWithAlias { expr, .. } => self.evaluate_final_group_expr(expr, &group_key, group_exprs, &agg_map, &schema, params)?,
                                 _ => Value::Null, 
                             };
                             new_row.push(val);
                         }
                         grouped_rows.push(new_row);
                    }
                    rows = grouped_rows;
                    
                    let new_cols: Vec<Column> = columns.iter().map(|name| Column {
                        name: name.clone(),
                        data_type: "UNKNOWN".to_string(),
                        is_primary: false,
                        is_indexed: false,
                        index_type: IndexType::None,
                    }).collect();
                    schema = TableSchema::new("temp_group_by_result".to_string(), new_cols);
                }
            }
            
            if let Some(order_by) = &query.order_by {
                if let OrderByKind::Expressions(exprs) = &order_by.kind {
                    rows.sort_by(|a, b| {
                        for order_expr in exprs {
                            let val_a = self.evaluate_value(&order_expr.expr, a, &schema, params).unwrap_or(Value::Null);
                            let val_b = self.evaluate_value(&order_expr.expr, b, &schema, params).unwrap_or(Value::Null);
                            
                            let ordering = self.compare_for_sort(&val_a, &val_b);
                            if ordering != Ordering::Equal {
                                return if order_expr.options.asc.unwrap_or(true) {
                                    ordering
                                } else {
                                    ordering.reverse()
                                };
                            }
                        }
                        Ordering::Equal
                    });
                }
            }

            // Removed duplicate limit parsing logic here as we did it above
            // But we still need to apply offset and limit to final rows
            // Logic above was just for pushdown optimization
            
            let rows = rows.into_iter().skip(offset);
            let rows: Vec<Vec<Value>> = if let Some(limit) = limit {
                rows.take(limit).collect()
            } else {
                rows.collect()
            };

            if is_count_star {
                let count = rows.len();
                return Ok(QueryResult::Select { 
                    columns, 
                    rows: vec![vec![Value::Integer(count as i64)]] 
                });
            }

            let final_rows = if is_wildcard {
                rows
            } else if matches!(select.group_by, sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if !exprs.is_empty()) {
                 rows
            } else {
                let mut projected_rows = Vec::new();
                for row in rows {
                    let mut new_row = Vec::new();
                    for item in &select.projection {
                        let val = match item {
                            SelectItem::UnnamedExpr(expr) => self.evaluate_value(expr, &row, &schema, params).unwrap_or(Value::Null),
                            SelectItem::ExprWithAlias { expr, .. } => self.evaluate_value(expr, &row, &schema, params).unwrap_or(Value::Null),
                            _ => Value::Null,
                        };
                        new_row.push(val);
                    }
                    projected_rows.push(new_row);
                }
                projected_rows
            };

            return Ok(QueryResult::Select { columns, rows: final_rows });
        }
        Err(FusionError::Execution("Unsupported SELECT format".to_string()))
    }
    
    fn extract_aggregates_from_expr(&self, expr: &Expr, aggregates: &mut Vec<(Expr, String)>) {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                if matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                    if !aggregates.iter().any(|(e, _)| e == expr) {
                        aggregates.push((expr.clone(), name));
                    }
                } else {
                     if let FunctionArguments::List(args) = &func.args {
                         for arg in &args.args {
                             if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                                 self.extract_aggregates_from_expr(e, aggregates);
                             }
                         }
                     }
                }
            },
            Expr::BinaryOp { left, right, .. } => {
                self.extract_aggregates_from_expr(left, aggregates);
                self.extract_aggregates_from_expr(right, aggregates);
            },
            Expr::Nested(expr) => self.extract_aggregates_from_expr(expr, aggregates),
            Expr::UnaryOp { expr, .. } => self.extract_aggregates_from_expr(expr, aggregates),
            Expr::Cast { expr, .. } => self.extract_aggregates_from_expr(expr, aggregates),
            _ => {}
        }
    }

    fn evaluate_final_group_expr(&self, expr: &Expr, group_key: &[Value], group_exprs: &[Expr], agg_map: &std::collections::HashMap<Expr, Value>, schema: &TableSchema, params: &[Value]) -> Result<Value> {
        // 1. Check if it is a pre-calculated aggregate
        if let Some(val) = agg_map.get(expr) {
            return Ok(val.clone());
        }
        
        // 2. Check if it matches a group expression
        if let Some(idx) = group_exprs.iter().position(|e| e == expr) {
            return Ok(group_key[idx].clone());
        }
        
        // 3. Recurse / Evaluate
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let l = self.evaluate_final_group_expr(left, group_key, group_exprs, agg_map, schema, params)?;
                let r = self.evaluate_final_group_expr(right, group_key, group_exprs, agg_map, schema, params)?;
                self.evaluate_binary_op(l, op, r)
            },
            Expr::Nested(e) => self.evaluate_final_group_expr(e, group_key, group_exprs, agg_map, schema, params),
            Expr::Value(v) => Ok(self.sql_value_to_fusion_value(&v.value)),
            Expr::Identifier(ident) => {
                Err(FusionError::Execution(format!("Column '{}' must appear in the GROUP BY clause or be used in an aggregate function", ident.value)))
            },
            Expr::UnaryOp { op, expr } => {
                 let val = self.evaluate_final_group_expr(expr, group_key, group_exprs, agg_map, schema, params)?;
                 match op {
                     sqlparser::ast::UnaryOperator::Minus => {
                         match val {
                             Value::Integer(i) => Ok(Value::Integer(-i)),
                             Value::Float(f) => Ok(Value::Float(-f)),
                             _ => Err(FusionError::Execution("Unary minus on non-number".to_string())),
                         }
                     },
                     _ => Err(FusionError::Execution("Unsupported unary operator in GROUP BY".to_string())),
                 }
            },
            _ => Err(FusionError::Execution("Unsupported expression in GROUP BY projection".to_string())),
        }
    }

    fn evaluate_binary_op(&self, left_val: Value, op: &BinaryOperator, right_val: Value) -> Result<Value> {
         match op {
            BinaryOperator::Plus => {
                match (left_val, right_val) {
                    (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l + r)),
                    (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l + r)),
                    (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 + r)),
                    (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l + r as f64)),
                    _ => Err(FusionError::Execution("Type mismatch in addition".to_string())),
                }
            },
            BinaryOperator::Minus => {
                match (left_val, right_val) {
                    (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l - r)),
                    (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l - r)),
                    (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 - r)),
                    (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l - r as f64)),
                    _ => Err(FusionError::Execution("Type mismatch in subtraction".to_string())),
                }
            },
            BinaryOperator::Multiply => {
                match (left_val, right_val) {
                    (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l * r)),
                    (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l * r)),
                    (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 * r)),
                    (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l * r as f64)),
                    _ => Err(FusionError::Execution("Type mismatch in multiplication".to_string())),
                }
            },
            BinaryOperator::Divide => {
                match (left_val, right_val) {
                    (Value::Integer(l), Value::Integer(r)) => {
                        if r == 0 { return Err(FusionError::Execution("Division by zero".to_string())); }
                        Ok(Value::Integer(l / r))
                    },
                    (Value::Float(l), Value::Float(r)) => {
                        if r == 0.0 { return Err(FusionError::Execution("Division by zero".to_string())); }
                        Ok(Value::Float(l / r))
                    },
                    (Value::Integer(l), Value::Float(r)) => {
                         if r == 0.0 { return Err(FusionError::Execution("Division by zero".to_string())); }
                         Ok(Value::Float(l as f64 / r))
                    },
                    (Value::Float(l), Value::Integer(r)) => {
                         if r == 0 { return Err(FusionError::Execution("Division by zero".to_string())); }
                         Ok(Value::Float(l / r as f64))
                    },
                    _ => Err(FusionError::Execution("Type mismatch in division".to_string())),
                }
            },
            BinaryOperator::Eq => Ok(Value::Boolean(left_val == right_val)),
            BinaryOperator::NotEq => Ok(Value::Boolean(left_val != right_val)),
            BinaryOperator::Gt => Ok(Value::Boolean(left_val.compare(&right_val) == Ordering::Greater)),
            BinaryOperator::Lt => Ok(Value::Boolean(left_val.compare(&right_val) == Ordering::Less)),
            BinaryOperator::GtEq => Ok(Value::Boolean(left_val.compare(&right_val) != Ordering::Less)),
            BinaryOperator::LtEq => Ok(Value::Boolean(left_val.compare(&right_val) != Ordering::Greater)),
            _ => Err(FusionError::Execution(format!("Unsupported operator: {}", op))),
        }
    }

    fn evaluate_expr(&self, expr: &Expr, row: &[Value], schema: &TableSchema, params: &[Value]) -> Result<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_value(left, row, schema, params)?;
                let right_val = self.evaluate_value(right, row, schema, params)?;
                
                match op {
                    BinaryOperator::Eq => Ok(left_val == right_val),
                    BinaryOperator::NotEq => Ok(left_val != right_val),
                    BinaryOperator::Gt => self.compare_values(&left_val, &right_val, |l, r| l > r),
                    BinaryOperator::Lt => self.compare_values(&left_val, &right_val, |l, r| l < r),
                    BinaryOperator::GtEq => self.compare_values(&left_val, &right_val, |l, r| l >= r),
                    BinaryOperator::LtEq => self.compare_values(&left_val, &right_val, |l, r| l <= r),
                    _ => Err(FusionError::Execution(format!("Unsupported operator: {}", op))),
                }
            },
            Expr::MatchAgainst { columns, match_value, .. } => {
                let search_terms = if let SqlValue::SingleQuotedString(s) = match_value {
                    Self::tokenize(s)
                } else if let SqlValue::Placeholder(p) = match_value {
                    let idx = p.replace("$", "").parse::<usize>().unwrap_or(0);
                    if idx > 0 && idx <= params.len() {
                        if let Value::String(s) = &params[idx - 1] {
                            Self::tokenize(s)
                        } else {
                            return Err(FusionError::Execution("MATCH AGAINST parameter must be a string".to_string()));
                        }
                    } else {
                        return Err(FusionError::Execution("Invalid parameter index".to_string()));
                    }
                } else {
                    return Err(FusionError::Execution("MATCH AGAINST requires a string literal or placeholder".to_string()));
                };

                if search_terms.is_empty() {
                    return Ok(false);
                }

                if columns.len() != 1 {
                     return Err(FusionError::Execution("MATCH currently supports only single column".to_string()));
                }
                let col_ident = &columns[0];
                let col_name = col_ident.to_string(); 

                let col_idx = self.resolve_column_index(&col_name, schema)?;
                let val = &row[col_idx];

                if let Value::String(text) = val {
                    let text_tokens: HashSet<String> = Self::tokenize(text).into_iter().collect();
                    for term in search_terms {
                        if !text_tokens.contains(&term) {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                } else {
                    Ok(false)
                }
            },
            _ => Err(FusionError::Execution("Unsupported expression type".to_string())),
        }
    }

    fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    fn resolve_column_index(&self, col_name: &str, schema: &TableSchema) -> Result<usize> {
        if let Some(idx) = schema.columns.iter().position(|c| c.name == col_name) {
            return Ok(idx);
        }
        
        if !col_name.contains('.') {
            let suffix = format!(".{}", col_name);
            let matches: Vec<usize> = schema.columns.iter().enumerate()
                .filter(|(_, c)| c.name.ends_with(&suffix) || c.name == col_name)
                .map(|(i, _)| i)
                .collect();

            if matches.len() == 1 {
                return Ok(matches[0]);
            } else if matches.len() > 1 {
                return Err(FusionError::Execution(format!("Ambiguous column name: {}", col_name)));
            }
        }
        
        Err(FusionError::Execution(format!("Column {} not found", col_name)))
    }

    fn evaluate_value(&self, expr: &Expr, row: &[Value], schema: &TableSchema, params: &[Value]) -> Result<Value> {
        match expr {
            Expr::Identifier(ident) => {
                let col_name = ident.value.clone();
                let idx = self.resolve_column_index(&col_name, schema)?;
                Ok(row[idx].clone())
            },
            Expr::CompoundIdentifier(idents) => {
                let col_name = idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
                let idx = self.resolve_column_index(&col_name, schema)?;
                Ok(row[idx].clone())
            },
            Expr::Function(func) => self.evaluate_function(func, row, schema, params),
            Expr::Nested(inner) => self.evaluate_value(inner, row, schema, params),
            Expr::Array(arr) => {
                let mut values = Vec::new();
                for elem in &arr.elem {
                    values.push(self.evaluate_value(elem, row, schema, params)?);
                }
                Ok(Value::Array(values))
            },
            Expr::Value(v) => {
                if let SqlValue::Placeholder(p) = &v.value {
                    let idx = p.replace("$", "").parse::<usize>().unwrap_or(0);
                    if idx > 0 && idx <= params.len() {
                        Ok(params[idx - 1].clone())
                    } else {
                        Err(FusionError::Execution(format!("Invalid parameter placeholder: {}", p)))
                    }
                } else {
                    Ok(self.sql_value_to_fusion_value(&v.value))
                }
            },
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_value(left, row, schema, params)?;
                let right_val = self.evaluate_value(right, row, schema, params)?;
                match op {
                    BinaryOperator::Plus => self.compute_math_op(&left_val, &right_val, |a, b| a + b, |a, b| a + b),
                    BinaryOperator::Minus => self.compute_math_op(&left_val, &right_val, |a, b| a - b, |a, b| a - b),
                    BinaryOperator::Multiply => self.compute_math_op(&left_val, &right_val, |a, b| a * b, |a, b| a * b),
                    BinaryOperator::Divide => {
                         match &right_val {
                             Value::Integer(0) | Value::Float(0.0) => return Err(FusionError::Execution("Division by zero".to_string())),
                             _ => {}
                         }
                         self.compute_math_op(&left_val, &right_val, |a, b| a / b, |a, b| a / b)
                    },
                    BinaryOperator::Modulo => self.compute_math_op(&left_val, &right_val, |a, b| a % b, |a, b| a % b),
                    BinaryOperator::Arrow => {
                         if let Value::Object(map) = left_val {
                             if let Value::String(key) = right_val {
                                 Ok(map.get(&key).cloned().unwrap_or(Value::Null))
                             } else {
                                 Err(FusionError::Execution("JSON key must be string".to_string()))
                             }
                         } else {
                             Ok(Value::Null)
                         }
                    },
                    BinaryOperator::LongArrow => {
                         if let Value::Object(map) = left_val {
                             if let Value::String(key) = right_val {
                                 let v = map.get(&key).cloned().unwrap_or(Value::Null);
                                 if let Value::String(s) = v {
                                     Ok(Value::String(s))
                                 } else {
                                     Ok(Value::String(v.to_string()))
                                 }
                             } else {
                                 Err(FusionError::Execution("JSON key must be string".to_string()))
                             }
                         } else {
                             Ok(Value::Null)
                         }
                    },
                    _ => Err(FusionError::Execution(format!("Unsupported operator in value expression: {}", op))),
                }
            },
            _ => Err(FusionError::Execution(format!("Unsupported value expression: {:?}", expr))),
        }
    }

    fn compute_math_op<I, F>(&self, left: &Value, right: &Value, op_int: I, op_float: F) -> Result<Value>
    where I: Fn(i64, i64) -> i64, F: Fn(f64, f64) -> f64 {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(op_int(*l, *r))),
            (Value::Float(l), Value::Float(r)) => Ok(Value::Float(op_float(*l, *r))),
            (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(op_float(*l as f64, *r))),
            (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(op_float(*l, *r as f64))),
             (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ => Err(FusionError::Execution("Type mismatch in arithmetic operation".to_string())),
        }
    }

    fn compare_values<F>(&self, left: &Value, right: &Value, op: F) -> Result<bool> 
    where F: Fn(&f64, &f64) -> bool {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Ok(op(&(*l as f64), &(*r as f64))),
            (Value::Float(l), Value::Float(r)) => Ok(op(l, r)),
            (Value::Integer(l), Value::Float(r)) => Ok(op(&(*l as f64), r)),
            (Value::Float(l), Value::Integer(r)) => Ok(op(l, &(*r as f64))),
            _ => Err(FusionError::Execution("Type mismatch in comparison".to_string())),
        }
    }

    fn sql_value_to_fusion_value(&self, v: &SqlValue) -> Value {
        match v {
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Value::Integer(i)
                } else if let Ok(f) = n.parse::<f64>() {
                    Value::Float(f)
                } else {
                    Value::Null
                }
            },
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                if s.trim().starts_with('{') && s.trim().ends_with('}') {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
                         return self.json_value_to_fusion_value(&v);
                    }
                }
                Value::String(s.clone())
            },
            SqlValue::Boolean(b) => Value::Boolean(*b),
            SqlValue::Null => Value::Null,
            _ => Value::Null,
        }
    }

    fn json_value_to_fusion_value(&self, v: &serde_json::Value) -> Value {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Boolean(*b),
            serde_json::Value::Number(n) => {
                if n.is_i64() {
                    Value::Integer(n.as_i64().unwrap())
                } else {
                    Value::Float(n.as_f64().unwrap_or(0.0))
                }
            },
            serde_json::Value::String(s) => Value::String(s.clone()),
            serde_json::Value::Array(arr) => {
                Value::Array(arr.iter().map(|x| self.json_value_to_fusion_value(x)).collect())
            },
            serde_json::Value::Object(obj) => {
                let mut map = std::collections::HashMap::new();
                for (k, v) in obj {
                    map.insert(k.clone(), self.json_value_to_fusion_value(v));
                }
                Value::Object(map)
            },
        }
    }

    fn evaluate_function(&self, func: &Function, row: &[Value], schema: &TableSchema, params: &[Value]) -> Result<Value> {
        let name = func.name.to_string().to_uppercase();
        
        let args = match &func.args {
            FunctionArguments::List(list) => &list.args,
            _ => return Err(FusionError::Execution("Unsupported function argument format".to_string())),
        };

        match name.as_str() {
            "VECTOR_DISTANCE" => {
                if args.len() != 2 {
                     return Err(FusionError::Execution("VECTOR_DISTANCE requires 2 arguments".to_string()));
                }
                
                let v1 = self.evaluate_arg(&args[0], row, schema, params)?;
                let v2 = self.evaluate_arg(&args[1], row, schema, params)?;

                self.compute_vector_distance(&v1, &v2)
            },
            _ => Err(FusionError::Execution(format!("Unsupported function: {}", name))),
        }
    }

    fn evaluate_arg(&self, arg: &FunctionArg, row: &[Value], schema: &TableSchema, params: &[Value]) -> Result<Value> {
        match arg {
            FunctionArg::Named { arg, .. } => self.evaluate_arg_expr(arg, row, schema, params),
            FunctionArg::Unnamed(arg) => self.evaluate_arg_expr(arg, row, schema, params),
            _ => Err(FusionError::Execution("Unsupported function argument type".to_string())),
        }
    }

    fn evaluate_arg_expr(&self, arg_expr: &FunctionArgExpr, row: &[Value], schema: &TableSchema, params: &[Value]) -> Result<Value> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => self.evaluate_value(expr, row, schema, params),
            _ => Err(FusionError::Execution("Unsupported function argument type".to_string())),
        }
    }

    fn compute_vector_distance(&self, v1: &Value, v2: &Value) -> Result<Value> {
        let vec1 = self.extract_vector(v1)?;
        let vec2 = self.extract_vector(v2)?;

        if vec1.len() != vec2.len() {
             return Err(FusionError::Execution("Vector dimensions mismatch".to_string()));
        }

        let mut sum_sq = 0.0;
        for (a, b) in vec1.iter().zip(vec2.iter()) {
            sum_sq += (a - b).powi(2);
        }

        Ok(Value::Float(sum_sq.sqrt()))
    }

    fn extract_vector(&self, v: &Value) -> Result<Vec<f64>> {
        match v {
            Value::Vector(vec) => Ok(vec.iter().map(|&x| x as f64).collect()),
            Value::Array(arr) => {
                let mut res = Vec::new();
                for item in arr {
                    match item {
                        Value::Integer(i) => res.push(*i as f64),
                        Value::Float(f) => res.push(*f),
                        _ => return Err(FusionError::Execution("Vector elements must be numbers".to_string())),
                    }
                }
                Ok(res)
            },
            _ => Err(FusionError::Execution(format!("Value is not a vector: {:?}", v))),
        }
    }

    fn compare_for_sort(&self, v1: &Value, v2: &Value) -> Ordering {
        v1.compare(v2)
    }

    fn get_type_order(&self, v: &Value) -> u8 {
        v.get_type_order()
    }
}
