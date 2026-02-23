use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    TableFactor, Value as SqlValue,
};
use std::collections::{HashMap, HashSet};
use futures::stream::StreamExt;

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
                for (_, v) in kv_pairs {
                    let row: Vec<Value> = crate::common::encoding::RowDecoder::decode(&v).map_err(|e| {
                        FusionError::Execution(format!("Data deserialization error: {}", e))
                    })?;
                    rows.push(row);
                }
                return Ok((schema, rows));
            }

            // No table schema found — check for a view definition
            let view_key = format!("view:{}", table_name);
            if let Some(view_bytes) = txn.get(view_key.as_bytes()).await? {
                let view_sql = String::from_utf8(view_bytes)
                    .map_err(|e| FusionError::Execution(format!("View decode error: {}", e)))?;
                let stmts = crate::parser::parse_sql(&format!("SELECT * FROM ({}) AS _v", view_sql))?;
                if let Some(sqlparser::ast::Statement::Query(query)) = stmts.into_iter().next() {
                    let result = Box::pin(self.handle_query(&query, txn, &[])).await?;
                    if let super::QueryResult::Select { columns, rows } = result {
                        use crate::catalog::{Column, IndexType};
                        let cols: Vec<Column> = columns.iter().map(|c| Column {
                            name: c.clone(),
                            data_type: "TEXT".to_string(),
                            is_primary: false,
                            is_indexed: false,
                            index_type: IndexType::None,
                            default_value: None,
                            is_nullable: true,
                            is_unique: false,
                            check_expr: None,
                        }).collect();
                        let schema = TableSchema::new(table_name, cols);
                        return Ok((schema, rows));
                    }
                }
            }

            Err(FusionError::Execution(format!("Table {} not found", table_name)))
        } else {
            Err(FusionError::Execution(
                "Unsupported table factor".to_string(),
            ))
        }
    }

    pub(crate) fn prefix_schema_columns(
        &self,
        schema: &mut TableSchema,
        relation: &TableFactor,
    ) -> Result<()> {
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

    pub(crate) async fn execute_join(
        &self,
        from: &[sqlparser::ast::TableWithJoins],
        selection: &Option<Expr>,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
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
            let is_left_outer = matches!(
                join.join_operator,
                sqlparser::ast::JoinOperator::LeftOuter(_) | sqlparser::ast::JoinOperator::Left(_)
            );

            // Try Hash Join Optimization
            let mut hash_join_executed = false;

            // Check if we can use Hash Join (Simple Equi-Join)
            if let sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::LeftOuter(sqlparser::ast::JoinConstraint::On(
                expr,
            ))
            | sqlparser::ast::JoinOperator::Left(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::Join(sqlparser::ast::JoinConstraint::On(expr)) =
                &join.join_operator
            {
                if let Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right,
                } = expr
                {
                    let l_name = match left.as_ref() {
                        Expr::Identifier(i) => Some(i.value.clone()),
                        Expr::CompoundIdentifier(ids) => Some(
                            ids.iter()
                                .map(|i| i.value.clone())
                                .collect::<Vec<_>>()
                                .join("."),
                        ),
                        _ => None,
                    };

                    let r_name = match right.as_ref() {
                        Expr::Identifier(i) => Some(i.value.clone()),
                        Expr::CompoundIdentifier(ids) => Some(
                            ids.iter()
                                .map(|i| i.value.clone())
                                .collect::<Vec<_>>()
                                .join("."),
                        ),
                        _ => None,
                    };

                    if let (Some(ln), Some(rn)) = (l_name, r_name) {
                        let l_idx_opt = self.resolve_column_index(&ln, &schema).ok();
                        let r_idx_opt = self.resolve_column_index(&rn, &right_schema).ok();

                        let (left_key_idx, right_key_idx) =
                            if let (Some(l_idx), Some(r_idx)) = (l_idx_opt, r_idx_opt) {
                                (l_idx, r_idx)
                            } else {
                                let l_idx_rev = self.resolve_column_index(&ln, &right_schema).ok();
                                let r_idx_rev = self.resolve_column_index(&rn, &schema).ok();
                                if let (Some(l_idx), Some(r_idx)) = (l_idx_rev, r_idx_rev) {
                                    (r_idx, l_idx)
                                } else {
                                    (usize::MAX, usize::MAX)
                                }
                            };

                        if left_key_idx != usize::MAX {
                            hash_join_executed = true;
                            monitor::inc_plan();

                            // Build Phase (Right Table) — pre-allocate HashMap
                            let mut hash_map: HashMap<Value, Vec<&Vec<Value>>> = HashMap::with_capacity(right_rows.len());
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
                let row_width = schema.columns.len() + right_schema.columns.len();
                for left_row in &rows {
                    let mut matched = false;
                    for right_row in &right_rows {
                        let mut joined_row = Vec::with_capacity(row_width);
                        joined_row.extend_from_slice(left_row);
                        joined_row.extend_from_slice(right_row);

                        let mut match_join = true;
                        match &join.join_operator {
                            sqlparser::ast::JoinOperator::Inner(constraint)
                            | sqlparser::ast::JoinOperator::LeftOuter(constraint)
                            | sqlparser::ast::JoinOperator::Left(constraint)
                            | sqlparser::ast::JoinOperator::RightOuter(constraint)
                            | sqlparser::ast::JoinOperator::Right(constraint)
                            | sqlparser::ast::JoinOperator::FullOuter(constraint)
                            | sqlparser::ast::JoinOperator::Join(constraint) => {
                                if let sqlparser::ast::JoinConstraint::On(expr) = constraint {
                                    let res =
                                        self.evaluate_expr(expr, &joined_row, &new_schema, params)?;
                                    if !res {
                                        match_join = false;
                                    }
                                }
                            }
                            sqlparser::ast::JoinOperator::CrossJoin(_) => {}
                            _ => {}
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
            let (right_schema_base, right_rows) =
                self.scan_table_base(&table.relation, txn).await?;
            let mut right_schema = right_schema_base;
            self.prefix_schema_columns(&mut right_schema, &table.relation)?;

            let mut new_columns = schema.columns.clone();
            new_columns.extend(right_schema.columns.clone());
            let new_schema = TableSchema::new("join_result".to_string(), new_columns);

            let mut new_rows = Vec::with_capacity(rows.len() * right_rows.len());
            for left_row in &rows {
                for right_row in &right_rows {
                    let mut joined_row = Vec::with_capacity(left_row.len() + right_row.len());
                    joined_row.extend_from_slice(left_row);
                    joined_row.extend_from_slice(right_row);
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

    #[allow(clippy::type_complexity)]
    pub(crate) fn try_index_scan<'a>(
        &'a self,
        expr: &'a Expr,
        table_name: &'a str,
        schema: &'a TableSchema,
        txn: &'a mut dyn Transaction,
        params: &'a [Value],
        limit: Option<usize>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<HashSet<String>>>> + Send + 'a>,
    > {
        Box::pin(async move {
            match expr {
                Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right,
                } => {
                    let col_name = if let Expr::Identifier(ident) = left.as_ref() {
                        Some(ident.value.clone())
                    } else {
                        None
                    };

                    if let Some(col_name) = col_name {
                        if let Some(col_idx) = schema.get_column_index(&col_name) {
                            if schema.columns[col_idx].is_indexed
                                && schema.columns[col_idx].index_type == IndexType::BTree
                            {
                                let val = self
                                    .evaluate_value(right, &[], schema, params)
                                    .unwrap_or(Value::Null);
                                if let Some(val_str) = self.value_to_index_string(&val) {
                                    let index_prefix =
                                        format!("index:{}:{}:{}:", table_name, col_name, val_str);
                                    let index_entries =
                                                txn.scan_prefix(index_prefix.as_bytes(), limit).await?;

                                    let mut row_ids = HashSet::new();
                                    for (k, _) in index_entries {
                                        let parts: Vec<&str> =
                                            std::str::from_utf8(&k).unwrap().split(':').collect();
                                        let row_id = parts.last().unwrap();
                                        row_ids.insert(row_id.to_string());
                                    }
                                    return Ok(Some(row_ids));
                                }
                            }
                        }
                    }
                }
                Expr::MatchAgainst {
                    columns,
                    match_value,
                    ..
                } => {
                    if columns.len() == 1 {
                        let col_ident = &columns[0];
                        let col_name = col_ident.to_string();

                        if let Some(col_idx) = schema.get_column_index(&col_name) {
                            if schema.columns[col_idx].is_indexed
                                && schema.columns[col_idx].index_type == IndexType::FTS
                            {
                                monitor::inc_fts_search();
                                let match_val = if let SqlValue::SingleQuotedString(s) = match_value
                                {
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
                                            let index_prefix = format!(
                                                "fts:{}:{}:{}:",
                                                table_name, col_name, token
                                            );
                                            let index_entries =
                                                txn.scan_prefix(index_prefix.as_bytes(), None).await?;

                                            let mut current_token_row_ids = HashSet::new();
                                            for (k, _) in index_entries {
                                                let parts: Vec<&str> = std::str::from_utf8(&k)
                                                    .unwrap()
                                                    .split(':')
                                                    .collect();
                                                let row_id = parts.last().unwrap();
                                                current_token_row_ids.insert(row_id.to_string());
                                            }

                                            if let Some(candidates) = candidate_row_ids {
                                                candidate_row_ids = Some(
                                                    candidates
                                                        .intersection(&current_token_row_ids)
                                                        .cloned()
                                                        .collect(),
                                                );
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
                }
                Expr::InList {
                    expr: col_expr,
                    list,
                    negated,
                } => {
                    if *negated {
                        return Ok(None);
                    }
                    
                    if let Expr::Identifier(ident) = col_expr.as_ref() {
                         if let Some(col_idx) = schema.get_column_index(&ident.value) {
                             let col = &schema.columns[col_idx];
                             if col.is_indexed {
                                 let mut all_row_ids = HashSet::new();
                                 for item in list {
                                     let val = self.evaluate_value(item, &[], schema, params).unwrap_or(Value::Null);
                                     
                                     if col.is_primary {
                                          let val_str = match &val {
                                              Value::Integer(i) => Some(crate::common::encoding::encode_i64_comparable(*i)),
                                              Value::String(s) => Some(s.clone()),
                                              _ => None,
                                          };
                                          if let Some(s) = val_str {
                                              let key = format!("data:{}:{}", table_name, s);
                                              if txn.get(key.as_bytes()).await?.is_some() {
                                                  all_row_ids.insert(s);
                                              }
                                          }
                                     } else if let Some(val_str) = self.value_to_index_string(&val) {
                                           let index_prefix = format!("index:{}:{}:{}:", table_name, col.name, val_str);
                                           let kv = txn.scan_prefix(index_prefix.as_bytes(), limit).await?;
                                           for (k, _) in kv {
                                               let parts: Vec<&str> = std::str::from_utf8(&k).unwrap().split(':').collect();
                                               if let Some(row_id) = parts.last() {
                                                   all_row_ids.insert(row_id.to_string());
                                               }
                                           }
                                     }
                                 }
                                 return Ok(Some(all_row_ids));
                              }
                         }
                     }
                }
                Expr::Like { expr, pattern, negated, .. } => {
                      if *negated {
                          return Ok(None);
                      }
                      // Check if it's a prefix scan: LIKE 'prefix%'
                      if let (Expr::Identifier(ident), Expr::Value(val_with_span)) = (expr.as_ref(), pattern.as_ref()) {
                           if let SqlValue::SingleQuotedString(pattern_str) = &val_with_span.value {
                               if !pattern_str.starts_with('%') && !pattern_str.starts_with('_') {
                                   let prefix: String = pattern_str.chars().take_while(|c| *c != '%' && *c != '_').collect();
                                   
                                   if !prefix.is_empty() {
                                       if let Some(col_idx) = schema.get_column_index(&ident.value) {
                                           let col = &schema.columns[col_idx];
                                           if col.is_indexed {
                                               let mut all_row_ids = HashSet::new();
                                               
                                               if col.is_primary {
                                                   let key_prefix = format!("data:{}:{}", table_name, prefix);
                                                   let kv = txn.scan_prefix(key_prefix.as_bytes(), limit).await?;
                                                   for (k, _) in kv {
                                                       let parts: Vec<&str> = std::str::from_utf8(&k).unwrap().split(':').collect();
                                                       if let Some(row_id) = parts.last() {
                                                           all_row_ids.insert(row_id.to_string());
                                                       }
                                                   }
                                               } else {
                                                   let index_prefix = format!("index:{}:{}:{}", table_name, col.name, prefix);
                                                   let kv = txn.scan_prefix(index_prefix.as_bytes(), limit).await?;
                                                   for (k, _) in kv {
                                                       let parts: Vec<&str> = std::str::from_utf8(&k).unwrap().split(':').collect();
                                                       if let Some(row_id) = parts.last() {
                                                            all_row_ids.insert(row_id.to_string());
                                                       }
                                                   }
                                               }
                                               
                                               if !all_row_ids.is_empty() {
                                                   return Ok(Some(all_row_ids));
                                               }
                                           }
                                       }
                                   }
                               }
                               
                               // Trigram Index Fallback for wildcard LIKE
                               if let Some(col_idx) = schema.get_column_index(&ident.value) {
                                   let col = &schema.columns[col_idx];
                                   if col.is_indexed {
                                       if let Some(ftxn) = txn.as_any().downcast_ref::<crate::storage::fusion::FusionTransaction>() {
                                           let storage = &ftxn.storage;
                                           let idx_guard = storage.trigram_index.read().unwrap();
                                           if let Some(ids) = idx_guard.search(table_name, &col.name, pattern_str) {
                                               let row_keys = idx_guard.map_ids_to_row_keys(table_name, &ids);
                                               if !row_keys.is_empty() {
                                                   let mut set = HashSet::new();
                                                   for s in row_keys {
                                                       set.insert(s);
                                                   }
                                                   return Ok(Some(set));
                                               }
                                           }
                                       }
                                   }
                               }
                           }
                      }
                 }
                Expr::BinaryOp {
                    left,
                    op: BinaryOperator::And,
                    right,
                } => {
                    let left_res = self
                        .try_index_scan(left, table_name, schema, txn, params, None)
                        .await?;
                    let right_res = self
                        .try_index_scan(right, table_name, schema, txn, params, None)
                        .await?;

                    match (left_res, right_res) {
                        (Some(l), Some(r)) => {
                            // AND: intersect both index results for tighter filtering
                            return Ok(Some(l.intersection(&r).cloned().collect()));
                        }
                        (Some(s), None) | (None, Some(s)) => return Ok(Some(s)),
                        (None, None) => {}
                    }
                }
                Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Or,
                    right,
                } => {
                    let left_res = self
                        .try_index_scan(left, table_name, schema, txn, params, None)
                        .await?;
                    let right_res = self
                        .try_index_scan(right, table_name, schema, txn, params, None)
                        .await?;

                    // OR: both sides must have index results to be useful
                    if let (Some(l), Some(r)) = (left_res, right_res) {
                        return Ok(Some(l.union(&r).cloned().collect()));
                    }
                }
                Expr::Nested(inner) => {
                    return self
                        .try_index_scan(inner, table_name, schema, txn, params, limit)
                        .await;
                }
                _ => {}
            }
            Ok(None)
        })
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
                    if let Some(sqlparser::ast::Statement::Query(query)) = stmts.into_iter().next() {
                        let result = Box::pin(self.handle_query(&query, txn, params)).await?;
                        if let super::QueryResult::Select { columns, rows } = result {
                            use crate::catalog::{Column, IndexType};
                            let cols: Vec<Column> = columns.iter().map(|c| Column {
                                name: c.clone(),
                                data_type: "TEXT".to_string(),
                                is_primary: false,
                                is_indexed: false,
                                index_type: IndexType::None,
                                default_value: None,
                                is_nullable: true,
                                is_unique: false,
                                check_expr: None,
                            }).collect();
                            let schema = TableSchema::new(table_name, cols);
                            return Ok((schema, rows));
                        }
                    }
                }
                return Err(FusionError::Execution(format!("Table {} not found", table_name)));
            }

            let schema_bytes = schema_bytes_opt.unwrap();
            let schema: TableSchema = bincode::deserialize(&schema_bytes).map_err(|e| {
                FusionError::Execution(format!("Schema deserialization error: {}", e))
            })?;

            // Calculate Projection Indices (Case Insensitive)
            let projection_indices = if let Some(cols) = projection {
                let mut indices = Vec::new();
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
                if indices.is_empty() { None } else { Some(indices) }
            } else {
                None
            };

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
                    proj.iter().all(|name| {
                        schema.get_column_index(name).map(|idx| idx == pk_idx).unwrap_or(false)
                    })
                } else {
                    false
                };

                if projection_is_pk_only {
                     let selection_is_pk_only = if let Some(sel) = selection {
                         let mut cols = HashSet::new();
                         self.extract_columns_from_expr(sel, &mut cols);
                         cols.iter().all(|name| {
                            schema.get_column_index(name).map(|idx| idx == pk_idx).unwrap_or(false)
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
                                schema.get_column_index(name).map(|idx| idx == pk_idx).unwrap_or(false)
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
                               if let Expr::Identifier(ident) = &expr.expr {
                                    if let Some(idx) = schema.get_column_index(&ident.value) {
                                         if schema.columns[idx].is_primary || idx == 0 {
                                             is_pk_asc = true;
                                         }
                                    }
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
                                        if let Expr::Identifier(ident) = left.as_ref() {
                                            vector_search_args =
                                                Some((ident.value.clone(), right.as_ref().clone()));
                                        }
                                    }
                                }
                                // Case 2: VECTOR_DISTANCE function
                                else if let Expr::Function(func) = sort_expr {
                                    if func.name.to_string().to_uppercase() == "VECTOR_DISTANCE" {
                                        if let FunctionArguments::List(args) = &func.args {
                                            if args.args.len() == 2 {
                                                if let FunctionArg::Unnamed(
                                                    FunctionArgExpr::Expr(Expr::Identifier(ident)),
                                                ) = &args.args[0]
                                                {
                                                    if let FunctionArg::Unnamed(
                                                        FunctionArgExpr::Expr(val_expr),
                                                    ) = &args.args[1]
                                                    {
                                                        vector_search_args = Some((
                                                            ident.value.clone(),
                                                            val_expr.clone(),
                                                        ));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                if let Some((col_name, query_expr)) = vector_search_args {
                                    if let Some(idx) = schema.get_column_index(&col_name) {
                                        if schema.columns[idx].index_type == IndexType::HNSW {
                                            let query_val = self.evaluate_value(
                                                &query_expr,
                                                &[],
                                                &schema,
                                                params,
                                            )?;
                                            if let Value::Vector(query_vec) = query_val {
                                                let idx_name =
                                                    format!("hnsw_{}_{}", table_name, col_name);
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
                                                            crate::common::encoding::RowDecoder::decode(&data)
                                                        {
                                                            self.row_cache.insert(key, row.clone());
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
                    let is_pk = if let Expr::Identifier(ident) = left.as_ref() {
                        self.resolve_column_index(&ident.value, &schema).ok() == Some(0)
                    } else {
                        false
                    };

                    if is_pk {
                        let val = self
                            .evaluate_value(right, &[], &schema, params)
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

                            if let Some(v) = txn.get(key.as_bytes()).await? {
                                monitor::inc_row_read();
                                let row: Vec<Value> = crate::common::encoding::RowDecoder::decode(&v).map_err(|e| {
                                    FusionError::Execution(format!(
                                        "Data deserialization error: {}",
                                        e
                                    ))
                                })?;

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
                        let is_pk = if let Expr::Identifier(ident) = left.as_ref() {
                            self.resolve_column_index(&ident.value, &schema).ok() == Some(0)
                        } else {
                            false
                        };

                        if is_pk {
                            let val = self
                                .evaluate_value(right, &[], &schema, params)
                                .unwrap_or(Value::Null);
                            
                            if let Value::Integer(limit_val) = val {
                                let table_prefix = format!("data:{}:", table_name);
                                let min_key = table_prefix.as_bytes().to_vec();
                                let mut max_key = table_prefix.as_bytes().to_vec();
                                max_key.push(0xFF); 

                                let (start_key, end_key) = match op {
                                    BinaryOperator::Gt => {
                                        let encoded = crate::common::encoding::encode_i64_comparable(limit_val);
                                        let mut key = table_prefix.into_bytes();
                                        key.extend_from_slice(encoded.as_bytes());
                                        key.push(0x00);
                                        (Some(key), Some(max_key))
                                    }
                                    BinaryOperator::GtEq => {
                                        let encoded = crate::common::encoding::encode_i64_comparable(limit_val);
                                        let mut key = table_prefix.into_bytes();
                                        key.extend_from_slice(encoded.as_bytes());
                                        (Some(key), Some(max_key))
                                    }
                                    BinaryOperator::Lt => {
                                        let encoded = crate::common::encoding::encode_i64_comparable(limit_val);
                                        let mut key = table_prefix.into_bytes();
                                        key.extend_from_slice(encoded.as_bytes());
                                        (Some(min_key), Some(key))
                                    }
                                    BinaryOperator::LtEq => {
                                        let encoded = crate::common::encoding::encode_i64_comparable(limit_val);
                                        let mut key = table_prefix.into_bytes();
                                        key.extend_from_slice(encoded.as_bytes());
                                        key.push(0x00);
                                        (Some(min_key), Some(key))
                                    }
                                    _ => (None, None),
                                };

                                if let (Some(start), Some(end)) = (start_key, end_key) {
                                    index_used = true;
                                    let kv_pairs = txn.scan_range(&start, &end, limit).await?;
                                    for (k, v) in kv_pairs {
                                        let row = if key_only_scan {
                                            let k_str = String::from_utf8_lossy(&k);
                                            let prefix = format!("data:{}:", table_name);
                                            if let Some(pk_str) = k_str.strip_prefix(&prefix) {
                                                let mut r = vec![Value::Null; schema.columns.len()];
                                                if let Some(pk_idx) = pk_index {
                                                    let is_int = matches!(schema.columns[pk_idx].data_type.as_str(), "INTEGER" | "BIGINT");
                                                    let pk_val = if is_int {
                                                         if let Some(i) = crate::common::encoding::decode_i64_comparable(pk_str) {
                                                             Value::Integer(i)
                                                         } else {
                                                             Value::String(pk_str.to_string())
                                                         }
                                                    } else {
                                                        Value::String(pk_str.to_string())
                                                    };
                                                    r[pk_idx] = pk_val;
                                                }
                                                r
                                            } else {
                                                continue;
                                            }
                                        } else {
                                            if let Some(indices) = &projection_indices {
                                                crate::common::encoding::RowDecoder::decode_partial(&v, indices).map_err(|e| {
                                                    FusionError::Execution(format!("Data partial deserialization error: {}", e))
                                                })?
                                            } else {
                                                crate::common::encoding::RowDecoder::decode(&v).map_err(|e| {
                                                    FusionError::Execution(format!("Data deserialization error: {}", e))
                                                })?
                                            }
                                        };
                                        let matches = self.evaluate_expr(sel, &row, &schema, params)?;
                                        if matches {
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
            }

            // Try Index Scan
            if !index_used {
                if let Some(sel) = selection {
                    if let Some(row_ids) = self
                        .try_index_scan(sel, &table_name, &schema, txn, params, limit)
                        .await?
                    {
                        let mut row_ids_vec: Vec<String> = row_ids.into_iter().collect();
                        
                        if order_by.is_none() {
                             if let Some(l) = limit {
                                 if row_ids_vec.len() > l {
                                     row_ids_vec.truncate(l);
                                 }
                             }
                        }

                        if row_ids_vec.len() <= 10000 {
                            index_used = true;
                        
                            let txn_ref = &*txn;
                        
                        let table_name_for_stream = table_name.clone();
                        let schema_cols = schema.columns.clone();
                        let projection_indices_for_stream = projection_indices.clone();
                        let executor_for_stream = self;

                        let fetch_stream = futures::stream::iter(row_ids_vec)
                            .map(|row_id| {
                                let table_name = table_name_for_stream.clone();
                                let schema_cols = schema_cols.clone();
                                let projection_indices = projection_indices_for_stream.clone();
                                let executor = executor_for_stream;
                                
                                async move {
                                let data_key = format!("data:{}:{}", table_name, row_id);
                                
                                if let Some(row) = executor.row_cache.get(&data_key) {
                                    return Ok::<_, FusionError>(Some((row_id, row, true)));
                                }
                                
                                if key_only_scan {
                                    let mut r = vec![Value::Null; schema_cols.len()];
                                    if let Some(pk_idx) = pk_index {
                                        let is_int = matches!(schema_cols[pk_idx].data_type.as_str(), "INTEGER" | "BIGINT");
                                        let pk_val = if is_int {
                                             if let Some(i) = crate::common::encoding::decode_i64_comparable(&row_id) {
                                                 Value::Integer(i)
                                             } else {
                                                 Value::String(row_id.clone())
                                             }
                                        } else {
                                            Value::String(row_id.clone())
                                        };
                                        r[pk_idx] = pk_val;
                                    }
                                    return Ok(Some((row_id, r, true)));
                                }

                                if let Some(data_bytes) = txn_ref.get(data_key.as_bytes()).await? {
                                    let row: Vec<Value> = if let Some(indices) = &projection_indices {
                                        crate::common::encoding::RowDecoder::decode_partial(&data_bytes, indices).map_err(|e| {
                                            FusionError::Execution(format!("Data partial deserialization error: {}", e))
                                        })?
                                    } else {
                                        crate::common::encoding::RowDecoder::decode(&data_bytes).map_err(|e| {
                                            FusionError::Execution(format!("Data deserialization error: {}", e))
                                        })?
                                    };
                                    Ok(Some((row_id, row, false)))
                                } else {
                                    Ok(None)
                                }
                            }
                            })
                            .buffer_unordered(128);

                            let mut stream = fetch_stream;
                        while let Some(res) = stream.next().await {
                            let res = res?;
                            if let Some((row_id, row, from_cache)) = res {
                                if !from_cache {
                                    monitor::inc_row_read();
                                    let data_key = format!("data:{}:{}", table_name, row_id);
                                    self.row_cache.insert(data_key, row.clone());
                                } else {
                                    monitor::inc_row_cache_hit();
                                }

                                if self.evaluate_expr(sel, &row, &schema, params)? {
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
                             let mut r = vec![Value::Null; schema.columns.len()];
                             if let Some(pk_idx) = pk_index {
                                 let is_int = matches!(schema.columns[pk_idx].data_type.as_str(), "INTEGER" | "BIGINT");
                                 let pk_val = if is_int {
                                      if let Some(i) = crate::common::encoding::decode_i64_comparable(pk_str) {
                                          Value::Integer(i)
                                      } else {
                                          Value::String(pk_str.to_string())
                                          }
                                 } else {
                                     Value::String(pk_str.to_string())
                                 };
                                 r[pk_idx] = pk_val;
                             }
                             Some(r)
                         } else {
                             None
                         }
                    } else {
                        if let Some(indices) = &projection_indices {
                            crate::common::encoding::RowDecoder::decode_partial(&v, indices).ok()
                        } else {
                            crate::common::encoding::RowDecoder::decode(&v).ok()
                        }
                    };

                    if let Some(row) = row_res {
                         if let Some(sel) = &selection {
                             if self.evaluate_expr(sel, &row, &schema, params)? {
                                 rows.push(row);
                                 if let Some(l) = limit {
                                     if rows.len() >= l { break; }
                                 }
                             }
                         } else {
                             rows.push(row);
                             if let Some(l) = limit {
                                 if rows.len() >= l { break; }
                             }
                         }
                    }
                }

            } else if let Some(sel) = selection {
                if rows.len() > 1000 {
                    let filtered_rows = self.parallel_filter_rows(rows, sel, &schema, params);
                    rows = filtered_rows;
                    if let Some(l) = limit {
                        if rows.len() > l {
                            rows.truncate(l);
                        }
                    }
                } else {
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
            }

            Ok((schema, rows))
        } else {
            Err(FusionError::Execution(
                "Unsupported table factor".to_string(),
            ))
        }
    }
}
