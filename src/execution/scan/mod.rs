mod index_plan;
mod join;
mod predicate;

use index_plan::SMALL_INDEX_FETCH_THRESHOLD;

use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use futures::stream::StreamExt;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, TableFactor,
};
use std::collections::HashSet;

use super::Executor;

impl Executor {
    pub(crate) async fn scan_table_base(
        &self,
        relation: &TableFactor,
        txn: &mut dyn Transaction,
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        if let TableFactor::Table { name, .. } = relation {
            let table_name = name.to_string();
            let schema_key = format!("schema:{}", table_name);

            // Try table first
            if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
                let schema: TableSchema = bincode::deserialize(&schema_bytes).map_err(|e| {
                    FusionError::Execution(format!("Schema deserialization error: {}", e))
                })?;

                let prefix = format!("data:{}:", table_name);
                let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;
                let mut rows = Vec::with_capacity(kv_pairs.len());
                for (k, v) in kv_pairs {
                    let row: Vec<Value> = if let Ok(key_str) = std::str::from_utf8(&k) {
                        if let Some(row) = self.row_cache.get(key_str) {
                            monitor::inc_row_cache_hit();
                            row
                        } else {
                            let row =
                                crate::common::encoding::RowDecoder::decode(&v).map_err(|e| {
                                    FusionError::Execution(format!(
                                        "Data deserialization error: {}",
                                        e
                                    ))
                                })?;
                            self.row_cache.insert(key_str.to_string(), row.clone());
                            row
                        }
                    } else {
                        crate::common::encoding::RowDecoder::decode(&v).map_err(|e| {
                            FusionError::Execution(format!("Data deserialization error: {}", e))
                        })?
                    };
                    rows.push(row);
                }
                return Ok((schema, rows));
            }

            // No table schema found — check for a view definition
            let view_key = format!("view:{}", table_name);
            if let Some(view_bytes) = txn.get(view_key.as_bytes()).await? {
                let view_sql = String::from_utf8(view_bytes)
                    .map_err(|e| FusionError::Execution(format!("View decode error: {}", e)))?;
                let stmts =
                    crate::parser::parse_sql(&format!("SELECT * FROM ({}) AS _v", view_sql))?;
                if let Some(sqlparser::ast::Statement::Query(query)) = stmts.into_iter().next() {
                    let result = Box::pin(self.handle_query(&query, txn, &[])).await?;
                    if let super::QueryResult::Select { columns, rows } = result {
                        use crate::catalog::{Column, IndexType};
                        let cols: Vec<Column> = columns
                            .iter()
                            .map(|c| Column {
                                name: c.clone(),
                                data_type: "TEXT".to_string(),
                                is_primary: false,
                                is_indexed: false,
                                index_type: IndexType::None,
                                default_value: None,
                                is_nullable: true,
                                is_unique: false,
                                check_expr: None,
                            })
                            .collect();
                        let schema = TableSchema::new(table_name, cols);
                        return Ok((schema, rows));
                    }
                }
            }

            Err(FusionError::Execution(format!(
                "Table {} not found",
                table_name
            )))
        } else {
            Err(FusionError::Execution(
                "Unsupported table factor".to_string(),
            ))
        }
    }

    pub(crate) async fn scan_single_table(
        &self,
        table: &TableFactor,
        selection: &Option<Expr>,
        projection: &Option<Vec<String>>,
        txn: &mut dyn Transaction,
        params: &[Value],
        limit: Option<usize>,
        order_by: Option<&sqlparser::ast::OrderBy>,
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        if let TableFactor::Table { name, .. } = table {
            let table_name = name.to_string();
            let schema_key = format!("schema:{}", table_name);

            // Check for view — if no table schema exists, try view expansion
            let schema_bytes_opt = txn.get(schema_key.as_bytes()).await?;
            if schema_bytes_opt.is_none() {
                let view_key = format!("view:{}", table_name);
                if let Some(view_bytes) = txn.get(view_key.as_bytes()).await? {
                    let view_sql = String::from_utf8(view_bytes)
                        .map_err(|e| FusionError::Execution(format!("View decode error: {}", e)))?;
                    let stmts = crate::parser::parse_sql(&view_sql)?;
                    if let Some(sqlparser::ast::Statement::Query(query)) = stmts.into_iter().next()
                    {
                        let result = Box::pin(self.handle_query(&query, txn, params)).await?;
                        if let super::QueryResult::Select { columns, rows } = result {
                            use crate::catalog::{Column, IndexType};
                            let cols: Vec<Column> = columns
                                .iter()
                                .map(|c| Column {
                                    name: c.clone(),
                                    data_type: "TEXT".to_string(),
                                    is_primary: false,
                                    is_indexed: false,
                                    index_type: IndexType::None,
                                    default_value: None,
                                    is_nullable: true,
                                    is_unique: false,
                                    check_expr: None,
                                })
                                .collect();
                            let schema = TableSchema::new(table_name, cols);
                            return Ok((schema, rows));
                        }
                    }
                }
                return Err(FusionError::Execution(format!(
                    "Table {} not found",
                    table_name
                )));
            }

            let schema_bytes = schema_bytes_opt.unwrap();
            let schema: TableSchema = bincode::deserialize(&schema_bytes).map_err(|e| {
                FusionError::Execution(format!("Schema deserialization error: {}", e))
            })?;

            // Calculate Projection Indices (Case Insensitive)
            let projection_indices = if let Some(cols) = projection {
                let mut indices = Vec::with_capacity(cols.len());
                for name in cols {
                    let mut found = None;
                    for (i, col) in schema.columns.iter().enumerate() {
                        if col.name.eq_ignore_ascii_case(name) {
                            found = Some(i);
                            break;
                        }
                    }
                    if let Some(idx) = found {
                        indices.push(idx);
                    }
                }
                if cols.is_empty() || !indices.is_empty() {
                    Some(indices)
                } else {
                    None
                }
            } else {
                None
            };
            let zero_column_projection = projection_indices
                .as_ref()
                .is_some_and(|indices| indices.is_empty());

            // Determine if we can do Key-Only Scan (Projection Pushdown Optimization)
            let mut key_only_scan = false;
            let mut pk_index = None;
            for (i, col) in schema.columns.iter().enumerate() {
                if col.is_primary {
                    pk_index = Some(i);
                    break;
                }
            }
            if pk_index.is_none() && !schema.columns.is_empty() {
                pk_index = Some(0);
            }

            if let Some(pk_idx) = pk_index {
                let projection_is_pk_only = if let Some(proj) = projection {
                    !proj.is_empty()
                        && proj.iter().all(|name| {
                            self.resolve_column_index(name, &schema)
                                .ok()
                                .map(|idx| idx == pk_idx)
                                .unwrap_or(false)
                        })
                } else {
                    false
                };

                if projection_is_pk_only {
                    let selection_is_pk_only = if let Some(sel) = selection {
                        let mut cols = HashSet::new();
                        self.extract_columns_from_expr(sel, &mut cols);
                        cols.iter().all(|name| {
                            self.resolve_column_index(name, &schema)
                                .ok()
                                .map(|idx| idx == pk_idx)
                                .unwrap_or(false)
                        })
                    } else {
                        true
                    };

                    if selection_is_pk_only {
                        let order_by_is_pk_only = if let Some(ob) = order_by {
                            let mut cols = HashSet::new();
                            if let sqlparser::ast::OrderByKind::Expressions(exprs) = &ob.kind {
                                for expr in exprs {
                                    self.extract_columns_from_expr(&expr.expr, &mut cols);
                                }
                            }
                            cols.iter().all(|name| {
                                self.resolve_column_index(name, &schema)
                                    .ok()
                                    .map(|idx| idx == pk_idx)
                                    .unwrap_or(false)
                            })
                        } else {
                            true
                        };

                        if order_by_is_pk_only {
                            key_only_scan = true;
                        }
                    }
                }
            }

            // Determine if we can safely push down the limit to storage scans
            let effective_limit = if let Some(ob) = order_by {
                let mut is_pk_asc = false;
                if let sqlparser::ast::OrderByKind::Expressions(exprs) = &ob.kind {
                    if exprs.len() == 1 {
                        let expr = &exprs[0];
                        if expr.options.asc.unwrap_or(true) {
                            if self
                                .resolve_schema_column_index(&expr.expr, &schema)
                                .is_some_and(|idx| schema.columns[idx].is_primary || idx == 0)
                            {
                                is_pk_asc = true;
                            }
                        }
                    }
                }
                if is_pk_asc {
                    limit
                } else {
                    None
                }
            } else {
                limit
            };

            let mut rows = Vec::new();
            let mut index_used = false;
            let mut selection_fully_applied = selection.is_none();

            // Optimization: Vector Search (HNSW)
            if let Some(order_by) = order_by {
                if let Some(l) = limit {
                    if l > 0 {
                        if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
                            if exprs.len() == 1 {
                                let sort_expr = &exprs[0].expr;
                                let mut vector_search_args = None;

                                // Case 1: <-> operator
                                if let Expr::BinaryOp {
                                    left,
                                    op: BinaryOperator::Custom(op_str),
                                    right,
                                } = sort_expr
                                {
                                    if op_str == "<->" {
                                        if let Some(col_name) =
                                            Self::column_name_from_expr(left.as_ref())
                                        {
                                            vector_search_args =
                                                Some((col_name, right.as_ref().clone()));
                                        }
                                    }
                                }
                                // Case 2: VECTOR_DISTANCE function
                                else if let Expr::Function(func) = sort_expr {
                                    if func.name.to_string().to_uppercase() == "VECTOR_DISTANCE" {
                                        if let FunctionArguments::List(args) = &func.args {
                                            if args.args.len() == 2 {
                                                if let FunctionArg::Unnamed(
                                                    FunctionArgExpr::Expr(col_expr),
                                                ) = &args.args[0]
                                                {
                                                    if let FunctionArg::Unnamed(
                                                        FunctionArgExpr::Expr(val_expr),
                                                    ) = &args.args[1]
                                                    {
                                                        if let Some(col_name) =
                                                            Self::column_name_from_expr(col_expr)
                                                        {
                                                            vector_search_args =
                                                                Some((col_name, val_expr.clone()));
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                if let Some((col_name, query_expr)) = vector_search_args {
                                    if let Ok(idx) = self.resolve_column_index(&col_name, &schema) {
                                        if schema.columns[idx].index_type == IndexType::HNSW {
                                            let storage_col_name = schema.columns[idx].name.clone();
                                            let query_val = self.evaluate_value(
                                                &query_expr,
                                                &[],
                                                &schema,
                                                params,
                                            )?;
                                            if let Value::Vector(query_vec) = query_val {
                                                let idx_name = format!(
                                                    "hnsw_{}_{}",
                                                    table_name, storage_col_name
                                                );
                                                let search_results = self
                                                    .vector_index
                                                    .search(&idx_name, &query_vec, l)?;

                                                for (id, _dist) in search_results {
                                                    let key = format!("data:{}:{}", table_name, id);
                                                    if let Some(row) = self.row_cache.get(&key) {
                                                        rows.push(row);
                                                    } else if let Some(data) =
                                                        txn.get(key.as_bytes()).await?
                                                    {
                                                        if let Ok(row) =
                                                            Self::decode_row_for_projection(
                                                                &data,
                                                                projection_indices.as_deref(),
                                                            )
                                                        {
                                                            if projection_indices.is_none() {
                                                                self.row_cache
                                                                    .insert(key, row.clone());
                                                            }
                                                            rows.push(row);
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
            if let Some(sel) = selection {
                if let Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right,
                } = sel
                {
                    if let Some(value_expr) = pk_index.and_then(|pk_idx| {
                        self.equality_primary_key_value_expr(left, right, &schema, pk_idx)
                    }) {
                        let val = self
                            .evaluate_value(value_expr, &[], &schema, params)
                            .unwrap_or(Value::Null);
                        let row_id = match val {
                            Value::Integer(i) => {
                                Some(crate::common::encoding::encode_i64_comparable(i))
                            }
                            Value::String(s) => Some(s),
                            _ => None,
                        };

                        if let Some(id) = row_id {
                            let key = format!("data:{}:{}", table_name, id);

                            if key_only_scan {
                                if txn.get(key.as_bytes()).await?.is_some() {
                                    let row = Self::primary_key_row_from_id(&schema, pk_index, &id);
                                    return Ok((schema, vec![row]));
                                }
                                return Ok((schema, vec![]));
                            }

                            if let Some(row) = self.row_cache.get(&key) {
                                monitor::inc_row_cache_hit();
                                if self.evaluate_expr(sel, &row, &schema, params)? {
                                    return Ok((schema, vec![row]));
                                }
                                return Ok((schema, vec![]));
                            }

                            if let Some(v) = txn.get(key.as_bytes()).await? {
                                monitor::inc_row_read();
                                let row = Self::decode_row_for_projection(
                                    &v,
                                    projection_indices.as_deref(),
                                )
                                .map_err(|e| {
                                    FusionError::Execution(format!(
                                        "Data deserialization error: {}",
                                        e
                                    ))
                                })?;

                                if projection_indices.is_none() {
                                    self.row_cache.insert(key, row.clone());
                                }

                                if self.evaluate_expr(sel, &row, &schema, params)? {
                                    return Ok((schema, vec![row]));
                                }
                            }
                            return Ok((schema, vec![]));
                        }
                    }
                }
            }

            // Optimization: Primary Key Range Scan
            if let Some(sel) = selection {
                if !index_used {
                    if let Expr::BinaryOp { left, op, right } = sel {
                        if let Some((range_op, value_expr)) = pk_index.and_then(|pk_idx| {
                            self.primary_key_range_value_expr(left, op, right, &schema, pk_idx)
                        }) {
                            let val = self
                                .evaluate_value(value_expr, &[], &schema, params)
                                .unwrap_or(Value::Null);

                            if let Value::Integer(limit_val) = val {
                                let table_prefix = format!("data:{}:", table_name);
                                let min_key = table_prefix.as_bytes().to_vec();
                                let mut max_key = table_prefix.as_bytes().to_vec();
                                max_key.push(0xFF);

                                let (start_key, end_key) = match range_op {
                                    BinaryOperator::Gt => {
                                        let encoded =
                                            crate::common::encoding::encode_i64_comparable(
                                                limit_val,
                                            );
                                        let mut key = table_prefix.into_bytes();
                                        key.extend_from_slice(encoded.as_bytes());
                                        key.push(0x00);
                                        (Some(key), Some(max_key))
                                    }
                                    BinaryOperator::GtEq => {
                                        let encoded =
                                            crate::common::encoding::encode_i64_comparable(
                                                limit_val,
                                            );
                                        let mut key = table_prefix.into_bytes();
                                        key.extend_from_slice(encoded.as_bytes());
                                        (Some(key), Some(max_key))
                                    }
                                    BinaryOperator::Lt => {
                                        let encoded =
                                            crate::common::encoding::encode_i64_comparable(
                                                limit_val,
                                            );
                                        let mut key = table_prefix.into_bytes();
                                        key.extend_from_slice(encoded.as_bytes());
                                        (Some(min_key), Some(key))
                                    }
                                    BinaryOperator::LtEq => {
                                        let encoded =
                                            crate::common::encoding::encode_i64_comparable(
                                                limit_val,
                                            );
                                        let mut key = table_prefix.into_bytes();
                                        key.extend_from_slice(encoded.as_bytes());
                                        key.push(0x00);
                                        (Some(min_key), Some(key))
                                    }
                                    _ => (None, None),
                                };

                                if let (Some(start), Some(end)) = (start_key, end_key) {
                                    index_used = true;
                                    selection_fully_applied = true;
                                    let kv_pairs = txn.scan_range(&start, &end, limit).await?;
                                    for (k, v) in kv_pairs {
                                        let row = if key_only_scan {
                                            let k_str = String::from_utf8_lossy(&k);
                                            let prefix = format!("data:{}:", table_name);
                                            if let Some(pk_str) = k_str.strip_prefix(&prefix) {
                                                Self::primary_key_row_from_id(
                                                    &schema, pk_index, pk_str,
                                                )
                                            } else {
                                                continue;
                                            }
                                        } else {
                                            let cache_key = std::str::from_utf8(&k).ok();
                                            if let Some(key_str) = cache_key {
                                                if let Some(row) = self.row_cache.get(key_str) {
                                                    monitor::inc_row_cache_hit();
                                                    rows.push(row);
                                                    if let Some(l) = limit {
                                                        if rows.len() >= l {
                                                            break;
                                                        }
                                                    }
                                                    continue;
                                                }
                                            }

                                            let row = Self::decode_row_for_projection(
                                                &v,
                                                projection_indices.as_deref(),
                                            )
                                            .map_err(|e| {
                                                FusionError::Execution(format!(
                                                    "Data deserialization error: {}",
                                                    e
                                                ))
                                            })?;
                                            if projection_indices.is_none() {
                                                if let Some(key_str) = cache_key {
                                                    self.row_cache
                                                        .insert(key_str.to_string(), row.clone());
                                                }
                                            }
                                            row
                                        };
                                        rows.push(row);
                                        if let Some(l) = limit {
                                            if rows.len() >= l {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Try Index Scan
            if !index_used {
                if let Some(sel) = selection {
                    let index_probe_limit =
                        Self::index_candidate_cap(limit, order_by).saturating_add(1);
                    if let Some(index_plan) = self
                        .try_index_scan(
                            sel,
                            &table_name,
                            &schema,
                            txn,
                            params,
                            Some(index_probe_limit),
                        )
                        .await?
                    {
                        let mut row_ids_vec: Vec<String> = index_plan.row_ids.into_iter().collect();
                        row_ids_vec.sort_unstable();

                        if order_by.is_none() {
                            if let Some(l) = limit {
                                if row_ids_vec.len() > l {
                                    row_ids_vec.truncate(l);
                                }
                            }
                        }

                        if Self::should_use_index_plan(row_ids_vec.len(), limit, order_by) {
                            index_used = true;
                            selection_fully_applied = index_plan.exact;

                            if row_ids_vec.len() <= SMALL_INDEX_FETCH_THRESHOLD {
                                for row_id in row_ids_vec {
                                    let data_key = format!("data:{}:{}", table_name, row_id);

                                    let row = if let Some(row) = self.row_cache.get(&data_key) {
                                        monitor::inc_row_cache_hit();
                                        row
                                    } else if key_only_scan {
                                        Self::primary_key_row_from_id(&schema, pk_index, &row_id)
                                    } else if let Some(data_bytes) =
                                        txn.get(data_key.as_bytes()).await?
                                    {
                                        monitor::inc_row_read();
                                        let row: Vec<Value> = Self::decode_row_for_projection(
                                            &data_bytes,
                                            projection_indices.as_deref(),
                                        )
                                        .map_err(|e| {
                                            FusionError::Execution(format!(
                                                "Data deserialization error: {}",
                                                e
                                            ))
                                        })?;
                                        if projection_indices.is_none() {
                                            self.row_cache.insert(data_key, row.clone());
                                        }
                                        row
                                    } else {
                                        continue;
                                    };

                                    if !index_plan.exact
                                        && !self.evaluate_expr(sel, &row, &schema, params)?
                                    {
                                        continue;
                                    }

                                    rows.push(row);
                                    if let Some(l) = limit {
                                        if rows.len() >= l {
                                            break;
                                        }
                                    }
                                }
                            } else {
                                let txn_ref = &*txn;

                                let table_name_for_stream = table_name.clone();
                                let schema_cols = schema.columns.clone();
                                let projection_indices_for_stream = projection_indices.clone();
                                let executor_for_stream = self;

                                let fetch_stream = futures::stream::iter(row_ids_vec)
                                    .map(|row_id| {
                                        let table_name = table_name_for_stream.clone();
                                        let schema_cols = schema_cols.clone();
                                        let projection_indices =
                                            projection_indices_for_stream.clone();
                                        let executor = executor_for_stream;

                                        async move {
                                            let data_key =
                                                format!("data:{}:{}", table_name, row_id);

                                            if let Some(row) = executor.row_cache.get(&data_key) {
                                                return Ok::<_, FusionError>(Some((
                                                    row_id, row, true, false, false,
                                                )));
                                            }

                                            if key_only_scan {
                                                let schema = TableSchema::new(
                                                    table_name.clone(),
                                                    schema_cols,
                                                );
                                                let r = Self::primary_key_row_from_id(
                                                    &schema, pk_index, &row_id,
                                                );
                                                return Ok(Some((row_id, r, false, false, false)));
                                            }

                                            if let Some(data_bytes) =
                                                txn_ref.get(data_key.as_bytes()).await?
                                            {
                                                let cacheable = projection_indices.is_none();
                                                let row = Self::decode_row_for_projection(
                                                    &data_bytes,
                                                    projection_indices.as_deref(),
                                                )
                                                .map_err(|e| {
                                                    FusionError::Execution(format!(
                                                        "Data deserialization error: {}",
                                                        e
                                                    ))
                                                })?;
                                                Ok(Some((row_id, row, false, cacheable, true)))
                                            } else {
                                                Ok(None)
                                            }
                                        }
                                    })
                                    .buffer_unordered(128);

                                let mut stream = fetch_stream;
                                while let Some(res) = stream.next().await {
                                    let res = res?;
                                    if let Some((
                                        row_id,
                                        row,
                                        from_cache,
                                        cacheable,
                                        read_storage,
                                    )) = res
                                    {
                                        if read_storage {
                                            monitor::inc_row_read();
                                            if cacheable {
                                                let data_key =
                                                    format!("data:{}:{}", table_name, row_id);
                                                self.row_cache.insert(data_key, row.clone());
                                            }
                                        } else if from_cache {
                                            monitor::inc_row_cache_hit();
                                        }

                                        if !index_plan.exact
                                            && !self.evaluate_expr(sel, &row, &schema, params)?
                                        {
                                            continue;
                                        }

                                        rows.push(row);
                                        if let Some(l) = limit {
                                            if rows.len() >= l {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Full Table Scan
            if !index_used {
                let prefix_str = format!("data:{}:", table_name);
                let prefix = prefix_str.as_bytes().to_vec();

                let scan_limit = if selection.is_none() {
                    effective_limit
                } else {
                    None
                };

                let kv_pairs = txn.scan_prefix(&prefix, scan_limit).await?;

                for (k, v) in kv_pairs {
                    let row_res = if key_only_scan {
                        let k_str = String::from_utf8_lossy(&k);
                        let prefix_str = String::from_utf8_lossy(&prefix);
                        if let Some(pk_str) = k_str.strip_prefix(prefix_str.as_ref()) {
                            Some(Self::primary_key_row_from_id(&schema, pk_index, pk_str))
                        } else {
                            None
                        }
                    } else if zero_column_projection {
                        Some(Vec::new())
                    } else {
                        match std::str::from_utf8(&k) {
                            Ok(key_str) => {
                                if let Some(row) = self.row_cache.get(key_str) {
                                    monitor::inc_row_cache_hit();
                                    Some(row)
                                } else if projection_indices.is_none() {
                                    Self::decode_row_for_projection(&v, None).ok().map(|row| {
                                        self.row_cache.insert(key_str.to_string(), row.clone());
                                        row
                                    })
                                } else {
                                    Self::decode_row_for_projection(
                                        &v,
                                        projection_indices.as_deref(),
                                    )
                                    .ok()
                                }
                            }
                            Err(_) => {
                                Self::decode_row_for_projection(&v, projection_indices.as_deref())
                                    .ok()
                            }
                        }
                    };

                    if let Some(row) = row_res {
                        if let Some(sel) = &selection {
                            if self.evaluate_expr(sel, &row, &schema, params)? {
                                rows.push(row);
                                if let Some(l) = limit {
                                    if rows.len() >= l {
                                        break;
                                    }
                                }
                            }
                        } else {
                            rows.push(row);
                            if let Some(l) = limit {
                                if rows.len() >= l {
                                    break;
                                }
                            }
                        }
                    }
                }
            } else if let Some(sel) = selection {
                if !selection_fully_applied && rows.len() > 1000 {
                    let filtered_rows = self.parallel_filter_rows(rows, sel, &schema, params);
                    rows = filtered_rows;
                    if let Some(l) = limit {
                        if rows.len() > l {
                            rows.truncate(l);
                        }
                    }
                } else if !selection_fully_applied {
                    let capacity = limit.map_or(rows.len(), |value| rows.len().min(value));
                    let mut filtered_rows = Vec::with_capacity(capacity);
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
            }

            Ok((schema, rows))
        } else {
            Err(FusionError::Execution(
                "Unsupported table factor".to_string(),
            ))
        }
    }
}
