use crate::catalog::{Column, IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use sqlparser::ast::{BinaryOperator, ColumnOption, Expr, SetExpr, Statement, TableFactor};
use std::collections::HashSet;

use super::{Executor, QueryResult};

impl Executor {
    pub(crate) async fn handle_describe_table(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name_str = table_name.to_string();
        let schema_key = format!("schema:{}", table_name_str);
        if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
            let schema: TableSchema = bincode::deserialize(&schema_bytes)
                .map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;

            let mut rows = Vec::new();
            for col in schema.columns {
                rows.push(vec![
                    Value::String(col.name),
                    Value::String(col.data_type),
                    Value::String(if col.is_primary {
                        "PRI".to_string()
                    } else {
                        "".to_string()
                    }),
                    Value::String(if col.is_indexed {
                        format!("{:?}", col.index_type)
                    } else {
                        "".to_string()
                    }),
                ]);
            }

            Ok(QueryResult::Select {
                columns: vec![
                    "Field".to_string(),
                    "Type".to_string(),
                    "Key".to_string(),
                    "Index".to_string(),
                ],
                rows,
            })
        } else {
            Err(FusionError::Execution(format!(
                "Table {} not found",
                table_name_str
            )))
        }
    }

    pub(crate) async fn handle_show_create_table(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name_str = table_name.to_string();
        let schema_key = format!("schema:{}", table_name_str);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;

        let mut ddl = format!("CREATE TABLE {} (\n", table_name_str);
        for (i, col) in schema.columns.iter().enumerate() {
            ddl.push_str(&format!("  {} {}", col.name, col.data_type));
            if col.is_primary {
                ddl.push_str(" PRIMARY KEY");
            }
            if i < schema.columns.len() - 1 {
                ddl.push(',');
            }
            ddl.push('\n');
        }
        ddl.push_str(");");

        Ok(QueryResult::Select {
            columns: vec!["Table".to_string(), "Create Table".to_string()],
            rows: vec![vec![Value::String(table_name_str), Value::String(ddl)]],
        })
    }

    pub(crate) async fn handle_show_tables(
        &self,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let prefix = "schema:";
        let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;

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

    pub(crate) async fn handle_show_views(&self, txn: &mut dyn Transaction) -> Result<QueryResult> {
        let prefix = "view:";
        let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;

        let mut views = Vec::new();
        for (k, v) in kv_pairs {
            if let Ok(key_str) = std::str::from_utf8(&k) {
                if let Some(view_name) = key_str.strip_prefix(prefix) {
                    let definition = String::from_utf8(v).unwrap_or_default();
                    views.push(vec![
                        Value::String(view_name.to_string()),
                        Value::String(definition),
                    ]);
                }
            }
        }

        Ok(QueryResult::Select {
            columns: vec!["View".to_string(), "Definition".to_string()],
            rows: views,
        })
    }

    pub(crate) async fn handle_explain(
        &self,
        stmt: &Statement,
        analyze: bool,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<QueryResult> {
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

    async fn explain_statement_plan(
        &self,
        stmt: &Statement,
        txn: &mut dyn Transaction,
    ) -> Result<String> {
        match stmt {
            Statement::Query(query) => self.explain_query(query, txn).await,
            _ => Ok(format!(
                "Statement type not supported for detailed explanation: {}",
                stmt
            )),
        }
    }

    async fn explain_query(
        &self,
        query: &sqlparser::ast::Query,
        txn: &mut dyn Transaction,
    ) -> Result<String> {
        if let SetExpr::Select(select) = &query.body.as_ref() {
            let mut plan = String::new();
            plan.push_str("SELECT\n");

            if let Some(table) = select.from.first() {
                plan.push_str(&format!("  FROM: {}\n", table.relation));
                let access_path = self
                    .explain_table_access(&table.relation, &select.selection, txn)
                    .await?;
                plan.push_str(&format!("  Access Path: {}\n", access_path));

                for join in &table.joins {
                    plan.push_str(&format!("  JOIN: {}\n", join.relation));
                    let join_access = self
                        .explain_table_access(&join.relation, &None, txn)
                        .await?;
                    plan.push_str(&format!("    Access Path: {}\n", join_access));
                    plan.push_str(&format!("    Operator: {:?}\n", join.join_operator));
                }
            }

            if let Some(selection) = &select.selection {
                plan.push_str(&format!("  Filter: {}\n", selection));
            }

            if matches!(select.group_by, sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if !exprs.is_empty())
            {
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

    async fn explain_table_access(
        &self,
        table: &TableFactor,
        selection: &Option<Expr>,
        txn: &mut dyn Transaction,
    ) -> Result<String> {
        if let TableFactor::Table { name, .. } = table {
            let table_name = name.to_string();
            let schema_key = format!("schema:{}", table_name);
            if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
                let schema: TableSchema = bincode::deserialize(&schema_bytes)
                    .map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;

                if let Some(sel) = selection {
                    if let Expr::BinaryOp {
                        left,
                        op: BinaryOperator::Eq,
                        ..
                    } = sel
                    {
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
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                ..
            } => {
                if let Expr::Identifier(ident) = left.as_ref() {
                    if let Some(idx) = schema.get_column_index(&ident.value) {
                        if schema.columns[idx].is_indexed {
                            *result = Some(format!(
                                "{} ({:?})",
                                ident.value, schema.columns[idx].index_type
                            ));
                        }
                    }
                }
            }
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
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                self.check_index_usage(left, schema, result);
                if result.is_none() {
                    self.check_index_usage(right, schema, result);
                }
            }
            _ => {}
        }
    }

    pub(crate) async fn handle_create_index(
        &self,
        index_name: &Option<sqlparser::ast::ObjectName>,
        table_name: &sqlparser::ast::ObjectName,
        columns: &[sqlparser::ast::IndexColumn],
        _unique: bool,
        index_options: &[sqlparser::ast::IndexOption],
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name_str = table_name.to_string();
        let index_name_str = index_name
            .as_ref()
            .map(|n| n.to_string())
            .unwrap_or_else(|| format!("idx_{}_{}", table_name_str, uuid::Uuid::new_v4()));

        let schema_key = format!("schema:{}", table_name_str);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
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
                    return Err(FusionError::Execution(format!(
                        "Column {} not found",
                        ident.value
                    )));
                }
            } else {
                return Err(FusionError::Execution(
                    "Index only supports simple column references".to_string(),
                ));
            }
        }

        if target_col_indices.len() != 1 {
            return Err(FusionError::Execution(
                "Currently only single-column index is supported".to_string(),
            ));
        }
        let col_idx = target_col_indices[0];
        let col_name = schema.columns[col_idx].name.clone();

        let mut index_type = IndexType::BTree;
        for opt in index_options {
            if let sqlparser::ast::IndexOption::Using(sqlparser::ast::IndexType::Custom(ident)) =
                opt
            {
                if ident.value.eq_ignore_ascii_case("FTS") {
                    index_type = IndexType::FTS;
                } else if ident.value.eq_ignore_ascii_case("HNSW") {
                    index_type = IndexType::HNSW;
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
        let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;

        let mut count = 0;
        for (k, v) in kv_pairs {
            let parts: Vec<&str> = std::str::from_utf8(&k).unwrap().split(':').collect();
            let row_id = parts.last().unwrap();

            let row: Vec<Value> = crate::common::encoding::RowDecoder::decode(&v).map_err(|e| {
                FusionError::Execution(format!("Data deserialization error: {}", e))
            })?;
            let val = &row[col_idx];

            if index_type == IndexType::FTS {
                if let Value::String(text) = val {
                    let tokens = Self::tokenize(text);
                    let unique_tokens: HashSet<String> = tokens.into_iter().collect();
                    for token in unique_tokens {
                        let index_key =
                            format!("fts:{}:{}:{}:{}", table_name_str, col_name, token, row_id);
                        txn.put(index_key.as_bytes(), &[]).await?;
                    }
                }
            } else if index_type == IndexType::HNSW {
                if let Value::Vector(vec) = val {
                    let idx_name = format!("hnsw_{}_{}", table_name_str, col_name);
                    self.vector_index
                        .insert(&idx_name, row_id.to_string(), vec.clone())?;
                }
            } else {
                let val_str = match val {
                    Value::Integer(i) => i.to_string(),
                    Value::String(s) => s.clone(),
                    Value::Boolean(b) => b.to_string(),
                    _ => continue,
                };
                let index_key = format!(
                    "index:{}:{}:{}:{}",
                    table_name_str, col_name, val_str, row_id
                );
                txn.put(index_key.as_bytes(), &[]).await?;
            }
            count += 1;
        }

        // Store index metadata for DROP INDEX support
        let meta_key = format!("index_meta:{}", index_name_str);
        let meta_val = format!("{}:{}", table_name_str, col_name);
        txn.put(meta_key.as_bytes(), meta_val.as_bytes()).await?;

        Ok(QueryResult::Success {
            message: format!(
                "Index {} ({:?}) created on {}({}), indexed {} rows",
                index_name_str, index_type, table_name_str, col_name, count
            ),
        })
    }

    pub(crate) async fn handle_create_table(
        &self,
        name: &sqlparser::ast::ObjectName,
        columns: &[sqlparser::ast::ColumnDef],
        if_not_exists: bool,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name = name.to_string();

        // IF NOT EXISTS check
        let schema_key_check = format!("schema:{}", table_name);
        if txn.get(schema_key_check.as_bytes()).await?.is_some() {
            if if_not_exists {
                return Ok(QueryResult::Success {
                    message: format!("Table {} already exists (skipped)", table_name),
                });
            } else {
                return Err(FusionError::Execution(format!(
                    "Table {} already exists",
                    table_name
                )));
            }
        }
        let cols: Vec<Column> = columns
            .iter()
            .map(|c| {
                let is_primary = c.options.iter().any(|opt| match &opt.option {
                    ColumnOption::Unique(_) => false,
                    ColumnOption::PrimaryKey(_) => true,
                    _ => false,
                });
                let default_value = c.options.iter().find_map(|opt| {
                    if let ColumnOption::Default(expr) = &opt.option {
                        Some(format!("{}", expr))
                    } else {
                        None
                    }
                });
                Column {
                    name: c.name.to_string(),
                    data_type: format!("{}", c.data_type),
                    is_primary,
                    is_indexed: is_primary,
                    index_type: if is_primary {
                        IndexType::BTree
                    } else {
                        IndexType::None
                    },
                    default_value,
                    is_nullable: !is_primary
                        && !c
                            .options
                            .iter()
                            .any(|opt| matches!(&opt.option, ColumnOption::NotNull)),
                    is_unique: is_primary
                        || c.options
                            .iter()
                            .any(|opt| matches!(&opt.option, ColumnOption::Unique(_))),
                    check_expr: c.options.iter().find_map(|opt| {
                        if let ColumnOption::Check(expr) = &opt.option {
                            Some(format!("{}", expr))
                        } else {
                            None
                        }
                    }),
                }
            })
            .collect();

        let schema = TableSchema::new(table_name.clone(), cols);
        let key = format!("schema:{}", table_name);
        let value = bincode::serialize(&schema)
            .map_err(|e| FusionError::Execution(format!("Schema serialization error: {}", e)))?;

        txn.put(key.as_bytes(), &value).await?;

        Ok(QueryResult::Success {
            message: format!("Table {} created", table_name),
        })
    }

    pub(crate) async fn handle_drop_table(
        &self,
        names: &[sqlparser::ast::ObjectName],
        if_exists: bool,
        object_type: sqlparser::ast::ObjectType,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        if object_type != sqlparser::ast::ObjectType::Table {
            return Err(FusionError::Execution(
                "Only DROP TABLE is supported".to_string(),
            ));
        }

        let mut dropped_count = 0;
        for name in names {
            let table_name = name.to_string();

            let schema_key = format!("schema:{}", table_name);
            if txn.get(schema_key.as_bytes()).await?.is_none() {
                if if_exists {
                    continue;
                } else {
                    return Err(FusionError::Execution(format!(
                        "Table {} does not exist",
                        table_name
                    )));
                }
            }

            txn.delete(schema_key.as_bytes()).await?;

            let prefix = format!("data:{}:", table_name);
            let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;
            for (k, _) in kv_pairs {
                txn.delete(&k).await?;
            }

            let index_prefix = format!("index:{}:", table_name);
            let index_entries = txn.scan_prefix(index_prefix.as_bytes(), None).await?;
            for (k, _) in index_entries {
                txn.delete(&k).await?;
            }

            dropped_count += 1;
        }

        Ok(QueryResult::Success {
            message: format!("Dropped {} tables", dropped_count),
        })
    }

    pub(crate) async fn handle_drop_index(
        &self,
        names: &[sqlparser::ast::ObjectName],
        if_exists: bool,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let mut dropped = 0;
        for name in names {
            let index_name = name.to_string();
            // Index metadata key: index_meta:<index_name>
            let meta_key = format!("index_meta:{}", index_name);
            if let Some(meta_bytes) = txn.get(meta_key.as_bytes()).await? {
                // Meta stores "table_name:column_name"
                let meta_str = String::from_utf8(meta_bytes).unwrap_or_default();
                let parts: Vec<&str> = meta_str.split(':').collect();
                if parts.len() >= 2 {
                    let table_name = parts[0];
                    let col_name = parts[1];

                    // Delete index entries
                    let index_prefix = format!("index:{}:{}:", table_name, col_name);
                    let entries = txn.scan_prefix(index_prefix.as_bytes(), None).await?;
                    for (k, _) in entries {
                        txn.delete(&k).await?;
                    }

                    // Update schema: mark column as not indexed
                    let schema_key = format!("schema:{}", table_name);
                    if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
                        if let Ok(mut schema) = bincode::deserialize::<TableSchema>(&schema_bytes) {
                            for col in &mut schema.columns {
                                if col.name == col_name {
                                    col.is_indexed = false;
                                    col.index_type = IndexType::None;
                                }
                            }
                            let new_bytes = bincode::serialize(&schema).map_err(|e| {
                                FusionError::Execution(format!("Serialize error: {}", e))
                            })?;
                            txn.put(schema_key.as_bytes(), &new_bytes).await?;
                        }
                    }
                }
                txn.delete(meta_key.as_bytes()).await?;
                dropped += 1;
            } else if !if_exists {
                return Err(FusionError::Execution(format!(
                    "Index {} does not exist",
                    index_name
                )));
            }
        }

        Ok(QueryResult::Success {
            message: format!("Dropped {} index(es)", dropped),
        })
    }

    pub(crate) async fn handle_truncate(
        &self,
        table_names: &[sqlparser::ast::TruncateTableTarget],
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let mut count = 0;
        for target in table_names {
            let table_name = target.name.to_string();
            let schema_key = format!("schema:{}", table_name);
            if txn.get(schema_key.as_bytes()).await?.is_none() {
                return Err(FusionError::Execution(format!(
                    "Table {} does not exist",
                    table_name
                )));
            }

            let prefix = format!("data:{}:", table_name);
            let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;
            for (k, _) in &kv_pairs {
                txn.delete(k).await?;
            }
            count += kv_pairs.len();

            let index_prefix = format!("index:{}:", table_name);
            let index_entries = txn.scan_prefix(index_prefix.as_bytes(), None).await?;
            for (k, _) in index_entries {
                txn.delete(&k).await?;
            }
        }

        Ok(QueryResult::Success {
            message: format!("Truncated {} rows", count),
        })
    }

    pub(crate) async fn handle_alter_table(
        &self,
        name: &sqlparser::ast::ObjectName,
        operations: &[sqlparser::ast::AlterTableOperation],
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name = name.to_string();
        let schema_key = format!("schema:{}", table_name);

        let schema_bytes = txn.get(schema_key.as_bytes()).await?.ok_or_else(|| {
            FusionError::Execution(format!("Table {} does not exist", table_name))
        })?;
        let mut schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;

        let mut messages = Vec::new();

        for op in operations {
            match op {
                sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } => {
                    let col_name = column_def.name.to_string();
                    if schema.columns.iter().any(|c| c.name == col_name) {
                        return Err(FusionError::Execution(format!(
                            "Column {} already exists in table {}",
                            col_name, table_name
                        )));
                    }
                    let is_primary = column_def
                        .options
                        .iter()
                        .any(|opt| matches!(&opt.option, ColumnOption::PrimaryKey(_)));
                    schema.columns.push(Column {
                        name: col_name.clone(),
                        data_type: format!("{}", column_def.data_type),
                        is_primary,
                        is_indexed: is_primary,
                        index_type: if is_primary {
                            IndexType::BTree
                        } else {
                            IndexType::None
                        },
                        default_value: None,
                        is_nullable: true,
                        is_unique: false,
                        check_expr: None,
                    });
                    messages.push(format!("Added column {}", col_name));
                }
                sqlparser::ast::AlterTableOperation::DropColumn {
                    column_names,
                    if_exists,
                    ..
                } => {
                    for column_ident in column_names {
                        let col_name = column_ident.to_string();
                        let col_idx = schema.columns.iter().position(|c| c.name == col_name);
                        match col_idx {
                            Some(idx) => {
                                if schema.columns[idx].is_primary {
                                    return Err(FusionError::Execution(
                                        "Cannot drop PRIMARY KEY column".to_string(),
                                    ));
                                }
                                schema.columns.remove(idx);

                                // Rewrite existing rows: remove the column at idx
                                let data_prefix = format!("data:{}:", table_name);
                                let rows = txn.scan_prefix(data_prefix.as_bytes(), None).await?;
                                for (k, v) in rows {
                                    if let Ok(mut row) =
                                        crate::common::encoding::RowDecoder::decode(&v)
                                    {
                                        if idx < row.len() {
                                            row.remove(idx);
                                            let new_v =
                                                crate::common::encoding::RowEncoder::encode(&row);
                                            txn.put(&k, &new_v).await?;
                                        }
                                    }
                                }
                                messages.push(format!("Dropped column {}", col_name));
                            }
                            None => {
                                if !*if_exists {
                                    return Err(FusionError::Execution(format!(
                                        "Column {} does not exist",
                                        col_name
                                    )));
                                }
                            }
                        }
                    }
                }
                sqlparser::ast::AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                    ..
                } => {
                    let old_name = old_column_name.to_string();
                    let new_name = new_column_name.to_string();
                    let col = schema.columns.iter_mut().find(|c| c.name == old_name);
                    match col {
                        Some(c) => {
                            c.name = new_name.clone();
                            messages.push(format!("Renamed {} to {}", old_name, new_name));
                        }
                        None => {
                            return Err(FusionError::Execution(format!(
                                "Column {} does not exist",
                                old_name
                            )));
                        }
                    }
                }
                other => {
                    return Err(FusionError::Execution(format!(
                        "Unsupported ALTER TABLE operation: {:?}",
                        other
                    )));
                }
            }
        }

        // Save updated schema
        let new_bytes = bincode::serialize(&schema)
            .map_err(|e| FusionError::Execution(format!("Schema serialization error: {}", e)))?;
        txn.put(schema_key.as_bytes(), &new_bytes).await?;

        Ok(QueryResult::Success {
            message: messages.join("; "),
        })
    }

    pub(crate) async fn handle_create_view(
        &self,
        name: &sqlparser::ast::ObjectName,
        query: &sqlparser::ast::Query,
        or_replace: bool,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let view_name = name.to_string();
        let view_key = format!("view:{}", view_name);

        // Check if view already exists
        if let Some(_) = txn.get(view_key.as_bytes()).await? {
            if !or_replace {
                return Err(FusionError::Execution(format!(
                    "View {} already exists",
                    view_name
                )));
            }
        }

        // Store the query SQL as the view definition
        let view_sql = format!("{}", query);
        txn.put(view_key.as_bytes(), view_sql.as_bytes()).await?;

        Ok(QueryResult::Success {
            message: format!("View {} created", view_name),
        })
    }

    pub(crate) async fn handle_drop_view(
        &self,
        names: &[sqlparser::ast::ObjectName],
        if_exists: bool,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        for name in names {
            let view_name = name.to_string();
            let view_key = format!("view:{}", view_name);

            if txn.get(view_key.as_bytes()).await?.is_none() {
                if !if_exists {
                    return Err(FusionError::Execution(format!(
                        "View {} not found",
                        view_name
                    )));
                }
                continue;
            }

            txn.delete(view_key.as_bytes()).await?;
        }

        Ok(QueryResult::Success {
            message: format!("View(s) dropped"),
        })
    }
}
