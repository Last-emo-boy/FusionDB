mod index_plan;
mod predicate;

use index_plan::SMALL_INDEX_FETCH_THRESHOLD;

use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use futures::stream::StreamExt;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, TableFactor,
    TableWithJoins,
};
use std::collections::{HashMap, HashSet};

use super::Executor;

const JOIN_INDEX_PROBE_THRESHOLD: usize = 128;


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

    async fn scan_join_base(
        &self,
        relation: &TableFactor,
        projection: &Option<Vec<String>>,
        pending_predicates: &[Expr],
        join_column_refs: &HashSet<String>,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        if projection.is_none() {
            return self.scan_table_base(relation, txn).await;
        }

        let TableFactor::Table { name, .. } = relation else {
            return self.scan_table_base(relation, txn).await;
        };

        let table_name = name.to_string();
        let schema_key = format!("schema:{}", table_name);
        let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? else {
            return self.scan_table_base(relation, txn).await;
        };

        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        let mut prefixed_schema = schema.clone();
        self.prefix_schema_columns(&mut prefixed_schema, relation)?;
        let stage_projection = self.build_stage_join_projection(
            &prefixed_schema,
            projection,
            pending_predicates,
            join_column_refs,
        );

        let Some(stage_projection) = stage_projection else {
            return self.scan_table_base(relation, txn).await;
        };

        let base_projection: Vec<String> = stage_projection
            .iter()
            .filter_map(|column| {
                self.resolve_column_index(column, &schema)
                    .ok()
                    .map(|index| schema.columns[index].name.clone())
            })
            .collect();

        if base_projection.is_empty() || base_projection.len() >= schema.columns.len() {
            return self.scan_table_base(relation, txn).await;
        }

        self.scan_single_table(
            relation,
            &None,
            &Some(base_projection),
            txn,
            params,
            None,
            None,
        )
        .await
    }

    pub(crate) fn relation_names(&self, relation: &TableFactor) -> HashSet<String> {
        let mut names = HashSet::with_capacity(2);
        if let TableFactor::Table { name, alias, .. } = relation {
            names.insert(name.to_string());
            if let Some(alias) = alias {
                names.insert(alias.name.value.clone());
            }
        }
        names
    }

    fn join_constraint_expr<'a>(
        join_operator: &'a sqlparser::ast::JoinOperator,
    ) -> Option<&'a Expr> {
        match join_operator {
            sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::LeftOuter(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::Left(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::RightOuter(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::Right(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::FullOuter(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::Join(sqlparser::ast::JoinConstraint::On(expr)) => {
                Some(expr)
            }
            _ => None,
        }
    }

    fn extract_join_keys(
        &self,
        expr: &Expr,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Option<(Vec<usize>, Vec<usize>, Option<Expr>)> {
        let predicates = Self::collect_conjunctive_predicates(expr);

        let predicate_count = predicates.len();
        let mut left_key_indices = Vec::with_capacity(predicate_count);
        let mut right_key_indices = Vec::with_capacity(predicate_count);
        let mut residual = Vec::with_capacity(predicate_count);

        for predicate in predicates {
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = &predicate
            else {
                residual.push(predicate);
                continue;
            };

            let left_in_left = self.resolve_schema_column_index_strict(left, left_schema);
            let left_in_right = self.resolve_schema_column_index_strict(left, right_schema);
            let right_in_left = self.resolve_schema_column_index_strict(right, left_schema);
            let right_in_right = self.resolve_schema_column_index_strict(right, right_schema);

            if let (Some(l_idx), Some(r_idx)) = (left_in_left, right_in_right) {
                if left_in_right.is_none() && right_in_left.is_none() {
                    left_key_indices.push(l_idx);
                    right_key_indices.push(r_idx);
                    continue;
                }
            }

            if let (Some(l_idx), Some(r_idx)) = (right_in_left, left_in_right) {
                if right_in_right.is_none() && left_in_left.is_none() {
                    left_key_indices.push(l_idx);
                    right_key_indices.push(r_idx);
                    continue;
                }
            }

            residual.push(predicate);
        }

        if left_key_indices.is_empty() {
            None
        } else {
            Some((
                left_key_indices,
                right_key_indices,
                Self::combine_predicates(residual),
            ))
        }
    }

    fn row_key(row: &[Value], indices: &[usize]) -> Vec<Value> {
        let mut key = Vec::with_capacity(indices.len());
        for index in indices {
            key.push(row[*index].clone());
        }
        key
    }

    fn row_keys_equal(
        left_row: &[Value],
        left_indices: &[usize],
        right_row: &[Value],
        right_indices: &[usize],
    ) -> bool {
        left_indices.len() == right_indices.len()
            && left_indices
                .iter()
                .zip(right_indices)
                .all(|(left_index, right_index)| left_row[*left_index] == right_row[*right_index])
    }

    fn append_join_probe_matches(
        &self,
        left_row: &[Value],
        candidates: &[Vec<Value>],
        left_key_indices: &[usize],
        right_key_indices: &[usize],
        residual_expr: &Option<Expr>,
        new_schema: &TableSchema,
        params: &[Value],
        is_left_outer: bool,
        right_width: usize,
        limit: Option<usize>,
        probed_rows: &mut Vec<Vec<Value>>,
    ) -> Result<bool> {
        let mut matched = false;
        for right_row in candidates {
            if !Self::row_keys_equal(left_row, left_key_indices, right_row, right_key_indices) {
                continue;
            }

            let mut joined_row = Vec::with_capacity(left_row.len() + right_row.len());
            joined_row.extend_from_slice(left_row);
            joined_row.extend_from_slice(right_row);
            if let Some(residual) = residual_expr {
                if !self.evaluate_expr(residual, &joined_row, new_schema, params)? {
                    continue;
                }
            }
            matched = true;
            probed_rows.push(joined_row);
            if limit.is_some_and(|value| probed_rows.len() >= value) {
                return Ok(true);
            }
        }

        if !matched && is_left_outer {
            let mut joined_row = Vec::with_capacity(left_row.len() + right_width);
            joined_row.extend_from_slice(left_row);
            joined_row.extend(vec![Value::Null; right_width]);
            probed_rows.push(joined_row);
        }

        Ok(limit.is_some_and(|value| probed_rows.len() >= value))
    }

    fn projection_matches_schema(
        &self,
        projection: &Option<Vec<String>>,
        schema: &TableSchema,
    ) -> bool {
        let Some(columns) = projection else {
            return false;
        };

        if columns.len() != schema.columns.len() {
            return false;
        }

        columns
            .iter()
            .zip(schema.columns.iter())
            .all(|(projected, actual)| projected.eq_ignore_ascii_case(&actual.name))
    }

    fn project_join_rows(
        &self,
        schema: TableSchema,
        rows: Vec<Vec<Value>>,
        projection: &Option<Vec<String>>,
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        let Some(columns) = projection else {
            return Ok((schema, rows));
        };

        if columns.is_empty() || self.projection_matches_schema(projection, &schema) {
            return Ok((schema, rows));
        }

        let mut projection_indices = Vec::with_capacity(columns.len());
        let mut projected_columns = Vec::with_capacity(columns.len());
        for column in columns {
            let index = self.resolve_column_index(column, &schema)?;
            projection_indices.push(index);
            projected_columns.push(schema.columns[index].clone());
        }

        let mut projected_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut projected_row = Vec::with_capacity(projection_indices.len());
            for index in &projection_indices {
                projected_row.push(row[*index].clone());
            }
            projected_rows.push(projected_row);
        }

        Ok((
            TableSchema::new(schema.name.clone(), projected_columns),
            projected_rows,
        ))
    }

    fn collect_join_column_references(&self, from: &[TableWithJoins]) -> HashSet<String> {
        let join_count = from.iter().map(|table| table.joins.len()).sum::<usize>();
        let mut columns = HashSet::with_capacity(join_count.saturating_mul(2));

        for table in from {
            for join in &table.joins {
                if let Some(expr) = Self::join_constraint_expr(&join.join_operator) {
                    self.extract_columns_from_expr(expr, &mut columns);
                }
            }
        }

        columns
    }

    fn build_stage_join_projection(
        &self,
        schema: &TableSchema,
        projection: &Option<Vec<String>>,
        pending_predicates: &[Expr],
        join_column_refs: &HashSet<String>,
    ) -> Option<Vec<String>> {
        let Some(projected_columns) = projection else {
            return None;
        };

        let mut required_indices = HashSet::new();

        for column in projected_columns {
            if let Ok(index) = self.resolve_column_index(column, schema) {
                required_indices.insert(index);
            }
        }

        for predicate in pending_predicates {
            let mut columns = HashSet::new();
            self.extract_columns_from_expr(predicate, &mut columns);
            for column in columns {
                if let Ok(index) = self.resolve_column_index(&column, schema) {
                    required_indices.insert(index);
                }
            }
        }

        for column in join_column_refs {
            if let Ok(index) = self.resolve_column_index(column, schema) {
                required_indices.insert(index);
            }
        }

        if required_indices.is_empty() || required_indices.len() >= schema.columns.len() {
            return None;
        }

        let stage_projection: Vec<String> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(index, _)| required_indices.contains(index))
            .map(|(_, column)| column.name.clone())
            .collect();

        if stage_projection.is_empty() || stage_projection.len() >= schema.columns.len() {
            None
        } else {
            Some(stage_projection)
        }
    }

    fn apply_stage_join_projection(
        &self,
        schema: TableSchema,
        rows: Vec<Vec<Value>>,
        projection: &Option<Vec<String>>,
        pending_predicates: &[Expr],
        join_column_refs: &HashSet<String>,
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        let stage_projection = self.build_stage_join_projection(
            &schema,
            projection,
            pending_predicates,
            join_column_refs,
        );
        self.project_join_rows(schema, rows, &stage_projection)
    }

    async fn fetch_rows_by_join_key(
        &self,
        table_name: &str,
        schema: &TableSchema,
        key_col_idx: usize,
        key_value: &Value,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<Vec<Value>>> {
        let column = &schema.columns[key_col_idx];

        if column.is_primary {
            if let Some(row_id) = Self::value_to_primary_row_id(key_value) {
                if let Some(row) = self.fetch_full_row_by_id(table_name, &row_id, txn).await? {
                    return Ok(vec![row]);
                }
            }
            return Ok(Vec::new());
        }

        if !(column.is_indexed && column.index_type == IndexType::BTree) {
            return Ok(Vec::new());
        }

        let Some(value_str) = self.value_to_index_string(key_value) else {
            return Ok(Vec::new());
        };

        let index_prefix = format!("index:{}:{}:{}:", table_name, column.name, value_str);
        let index_entries = txn.scan_prefix(index_prefix.as_bytes(), None).await?;
        let mut seen_row_ids = HashSet::with_capacity(index_entries.len());
        let mut rows = Vec::with_capacity(index_entries.len());

        for (key, _) in index_entries {
            let Some(row_id) = Self::row_id_from_key(&key) else {
                continue;
            };
            if !seen_row_ids.insert(row_id.to_string()) {
                continue;
            }
            if let Some(row) = self.fetch_full_row_by_id(table_name, row_id, txn).await? {
                rows.push(row);
            }
        }

        Ok(rows)
    }

    fn choose_indexed_join_probe_pair(
        &self,
        left_key_indices: &[usize],
        right_key_indices: &[usize],
        right_schema: &TableSchema,
    ) -> Option<(usize, usize)> {
        left_key_indices
            .iter()
            .copied()
            .zip(right_key_indices.iter().copied())
            .find(|(_, right_idx)| {
                let column = &right_schema.columns[*right_idx];
                column.is_primary || (column.is_indexed && column.index_type == IndexType::BTree)
            })
    }

    fn should_attempt_join_probe(
        left_rows_len: usize,
        distinct_probe_keys: usize,
        limit: Option<usize>,
    ) -> bool {
        distinct_probe_keys > 0
            && (left_rows_len <= JOIN_INDEX_PROBE_THRESHOLD
                || distinct_probe_keys <= JOIN_INDEX_PROBE_THRESHOLD
                || limit.is_some_and(|value| value <= JOIN_INDEX_PROBE_THRESHOLD))
    }

    fn combine_optional_predicates(predicates: Vec<Option<Expr>>) -> Option<Expr> {
        Self::combine_predicates(predicates.into_iter().flatten().collect())
    }

    fn filter_rows_with_expr(
        &self,
        rows: Vec<Vec<Value>>,
        schema: &TableSchema,
        expr: &Expr,
        params: &[Value],
    ) -> Result<Vec<Vec<Value>>> {
        let mut filtered = Vec::with_capacity(rows.len());
        for row in rows {
            if self.evaluate_expr(expr, &row, schema, params)? {
                filtered.push(row);
            }
        }
        Ok(filtered)
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

    async fn apply_join_step(
        &self,
        left_schema: TableSchema,
        left_rows: Vec<Vec<Value>>,
        relation: &TableFactor,
        join_operator: Option<&sqlparser::ast::JoinOperator>,
        pending_predicates: &mut Vec<Expr>,
        projection: &Option<Vec<String>>,
        txn: &mut dyn Transaction,
        params: &[Value],
        limit: Option<usize>,
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        let right_relation_names = self.relation_names(relation);
        let mut left_rows = left_rows;
        let mut join_predicates =
            if let Some(on_expr) = join_operator.and_then(Self::join_constraint_expr) {
                Self::collect_conjunctive_predicates(on_expr)
            } else {
                Vec::new()
            };

        if let Some(left_local) = self.take_schema_predicate(&mut join_predicates, &left_schema) {
            left_rows = self.filter_rows_with_expr(left_rows, &left_schema, &left_local, params)?;
        }

        let where_right = self.take_relation_predicate(pending_predicates, &right_relation_names);
        let join_right = self.take_relation_predicate(&mut join_predicates, &right_relation_names);
        let right_selection = Self::combine_optional_predicates(vec![where_right, join_right]);

        let right_table_name = match relation {
            TableFactor::Table { name, .. } => Some(name.to_string()),
            _ => None,
        };
        let schema_for_probe = if let Some(table_name) = &right_table_name {
            let schema_key = format!("schema:{}", table_name);
            txn.get(schema_key.as_bytes())
                .await?
                .map(|schema_bytes| {
                    bincode::deserialize::<TableSchema>(&schema_bytes).map_err(|e| {
                        FusionError::Execution(format!("Schema deserialization error: {}", e))
                    })
                })
                .transpose()?
        } else {
            None
        };

        let is_left_outer = matches!(
            join_operator,
            Some(
                sqlparser::ast::JoinOperator::LeftOuter(_) | sqlparser::ast::JoinOperator::Left(_)
            )
        );
        let supports_left_driven_probe = matches!(
            join_operator,
            Some(
                sqlparser::ast::JoinOperator::Inner(_)
                    | sqlparser::ast::JoinOperator::Join(_)
                    | sqlparser::ast::JoinOperator::LeftOuter(_)
                    | sqlparser::ast::JoinOperator::Left(_)
            )
        );

        let join_expr = Self::combine_predicates(join_predicates.clone());
        if supports_left_driven_probe && right_table_name.is_some() {
            if let (Some(expr), Some(probe_schema)) = (&join_expr, schema_for_probe.as_ref()) {
                let mut right_schema_for_probe = probe_schema.clone();
                self.prefix_schema_columns(&mut right_schema_for_probe, relation)?;

                let mut new_columns = left_schema.columns.clone();
                new_columns.extend(right_schema_for_probe.columns.clone());
                let new_schema = TableSchema::new("join_result".to_string(), new_columns);

                if let Some((left_key_indices, right_key_indices, residual_expr)) =
                    self.extract_join_keys(expr, &left_schema, &right_schema_for_probe)
                {
                    if let Some((probe_left_idx, probe_right_idx)) = self
                        .choose_indexed_join_probe_pair(
                            &left_key_indices,
                            &right_key_indices,
                            probe_schema,
                        )
                    {
                        let mut distinct_probe_keys = HashSet::with_capacity(left_rows.len());
                        for left_row in &left_rows {
                            distinct_probe_keys.insert(left_row[probe_left_idx].clone());
                        }

                        if Self::should_attempt_join_probe(
                            left_rows.len(),
                            distinct_probe_keys.len(),
                            limit,
                        ) {
                            monitor::inc_plan();
                            let right_table_name = right_table_name.unwrap();
                            let probed_capacity =
                                limit.map_or(left_rows.len(), |value| left_rows.len().min(value));
                            let mut probed_rows = Vec::with_capacity(probed_capacity);
                            let mut probe_cache: HashMap<Value, Vec<Vec<Value>>> =
                                HashMap::with_capacity(distinct_probe_keys.len());

                            for left_row in &left_rows {
                                let probe_key = left_row[probe_left_idx].clone();
                                if let Some(candidates) = probe_cache.get(&probe_key) {
                                    if self.append_join_probe_matches(
                                        left_row,
                                        candidates,
                                        &left_key_indices,
                                        &right_key_indices,
                                        &residual_expr,
                                        &new_schema,
                                        params,
                                        is_left_outer,
                                        right_schema_for_probe.columns.len(),
                                        limit,
                                        &mut probed_rows,
                                    )? {
                                        break;
                                    }
                                    continue;
                                }

                                let mut candidates = self
                                    .fetch_rows_by_join_key(
                                        &right_table_name,
                                        probe_schema,
                                        probe_right_idx,
                                        &probe_key,
                                        txn,
                                    )
                                    .await?;
                                if let Some(selection) = &right_selection {
                                    candidates = self.filter_rows_with_expr(
                                        candidates,
                                        probe_schema,
                                        selection,
                                        params,
                                    )?;
                                }
                                let reached_limit = self.append_join_probe_matches(
                                    left_row,
                                    &candidates,
                                    &left_key_indices,
                                    &right_key_indices,
                                    &residual_expr,
                                    &new_schema,
                                    params,
                                    is_left_outer,
                                    right_schema_for_probe.columns.len(),
                                    limit,
                                    &mut probed_rows,
                                )?;
                                probe_cache.insert(probe_key, candidates);
                                if reached_limit {
                                    break;
                                }
                            }

                            return self.project_join_rows(new_schema, probed_rows, projection);
                        }
                    }
                }
            }
        }

        let (right_schema_base, right_rows) = if right_selection.is_some() {
            self.scan_single_table(relation, &right_selection, &None, txn, params, None, None)
                .await?
        } else {
            self.scan_table_base(relation, txn).await?
        };
        let mut right_schema = right_schema_base.clone();
        self.prefix_schema_columns(&mut right_schema, relation)?;

        let mut new_columns = left_schema.columns.clone();
        new_columns.extend(right_schema.columns.clone());
        let new_schema = TableSchema::new("join_result".to_string(), new_columns);

        let row_width = left_schema.columns.len() + right_schema.columns.len();
        let mut new_rows = Vec::new();
        let mut hash_join_executed = false;

        if !matches!(
            join_operator,
            Some(sqlparser::ast::JoinOperator::CrossJoin(_)) | None
        ) {
            if let Some(expr) = &join_expr {
                if let Some((left_key_indices, right_key_indices, residual_expr)) =
                    self.extract_join_keys(expr, &left_schema, &right_schema)
                {
                    hash_join_executed = true;
                    monitor::inc_plan();

                    let build_right = is_left_outer || right_rows.len() <= left_rows.len();
                    if build_right {
                        let mut hash_map: HashMap<Vec<Value>, Vec<&Vec<Value>>> =
                            HashMap::with_capacity(right_rows.len());
                        for right_row in &right_rows {
                            let key = Self::row_key(right_row, &right_key_indices);
                            hash_map.entry(key).or_default().push(right_row);
                        }

                        for left_row in &left_rows {
                            let key = Self::row_key(left_row, &left_key_indices);
                            let mut matched = false;
                            if let Some(matches) = hash_map.get(&key) {
                                for right_row in matches {
                                    let mut joined_row = Vec::with_capacity(row_width);
                                    joined_row.extend_from_slice(left_row);
                                    joined_row.extend_from_slice(right_row);
                                    if let Some(residual) = &residual_expr {
                                        if !self.evaluate_expr(
                                            residual,
                                            &joined_row,
                                            &new_schema,
                                            params,
                                        )? {
                                            continue;
                                        }
                                    }
                                    matched = true;
                                    new_rows.push(joined_row);
                                    if limit.is_some_and(|value| new_rows.len() >= value) {
                                        break;
                                    }
                                }
                            }

                            if !matched && is_left_outer {
                                let mut joined_row = Vec::with_capacity(row_width);
                                joined_row.extend_from_slice(left_row);
                                for _ in 0..right_schema.columns.len() {
                                    joined_row.push(Value::Null);
                                }
                                new_rows.push(joined_row);
                            }

                            if limit.is_some_and(|value| new_rows.len() >= value) {
                                break;
                            }
                        }
                    } else {
                        let mut hash_map: HashMap<Vec<Value>, Vec<&Vec<Value>>> =
                            HashMap::with_capacity(left_rows.len());
                        for left_row in &left_rows {
                            let key = Self::row_key(left_row, &left_key_indices);
                            hash_map.entry(key).or_default().push(left_row);
                        }

                        for right_row in &right_rows {
                            let key = Self::row_key(right_row, &right_key_indices);
                            if let Some(matches) = hash_map.get(&key) {
                                for left_row in matches {
                                    let mut joined_row = Vec::with_capacity(row_width);
                                    joined_row.extend_from_slice(left_row);
                                    joined_row.extend_from_slice(right_row);
                                    if let Some(residual) = &residual_expr {
                                        if !self.evaluate_expr(
                                            residual,
                                            &joined_row,
                                            &new_schema,
                                            params,
                                        )? {
                                            continue;
                                        }
                                    }
                                    new_rows.push(joined_row);
                                    if limit.is_some_and(|value| new_rows.len() >= value) {
                                        break;
                                    }
                                }
                            }

                            if limit.is_some_and(|value| new_rows.len() >= value) {
                                break;
                            }
                        }
                    }
                }
            }
        }

        if !hash_join_executed {
            for left_row in &left_rows {
                let mut matched = false;
                for right_row in &right_rows {
                    let mut joined_row = Vec::with_capacity(row_width);
                    joined_row.extend_from_slice(left_row);
                    joined_row.extend_from_slice(right_row);

                    let match_join = if let Some(expr) = &join_expr {
                        self.evaluate_expr(expr, &joined_row, &new_schema, params)?
                    } else {
                        true
                    };

                    if match_join {
                        new_rows.push(joined_row);
                        matched = true;
                        if limit.is_some_and(|value| new_rows.len() >= value) {
                            break;
                        }
                    }
                }

                if !matched && is_left_outer {
                    let mut joined_row = Vec::with_capacity(row_width);
                    joined_row.extend_from_slice(left_row);
                    for _ in 0..right_schema.columns.len() {
                        joined_row.push(Value::Null);
                    }
                    new_rows.push(joined_row);
                }

                if limit.is_some_and(|value| new_rows.len() >= value) {
                    break;
                }
            }
        }

        self.project_join_rows(new_schema, new_rows, projection)
    }

    pub(crate) async fn execute_join(
        &self,
        from: &[sqlparser::ast::TableWithJoins],
        selection: &Option<Expr>,
        projection: &Option<Vec<String>>,
        txn: &mut dyn Transaction,
        params: &[Value],
        limit: Option<usize>,
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        let first = &from[0];
        let join_column_refs = self.collect_join_column_references(from);
        let mut pending_predicates = if let Some(expr) = selection {
            Self::collect_conjunctive_predicates(expr)
        } else {
            Vec::new()
        };

        let first_relation_names = self.relation_names(&first.relation);
        let first_selection =
            self.take_relation_predicate(&mut pending_predicates, &first_relation_names);
        let (mut schema, mut rows) = if first_selection.is_some() {
            self.scan_single_table(
                &first.relation,
                &first_selection,
                &None,
                txn,
                params,
                None,
                None,
            )
            .await?
        } else {
            self.scan_join_base(
                &first.relation,
                projection,
                &pending_predicates,
                &join_column_refs,
                txn,
                params,
            )
            .await?
        };

        self.prefix_schema_columns(&mut schema, &first.relation)?;

        for join in &first.joins {
            (schema, rows) = self
                .apply_join_step(
                    schema,
                    rows,
                    &join.relation,
                    Some(&join.join_operator),
                    &mut pending_predicates,
                    &None,
                    txn,
                    params,
                    limit,
                )
                .await?;
            (schema, rows) = self.apply_stage_join_projection(
                schema,
                rows,
                projection,
                &pending_predicates,
                &join_column_refs,
            )?;
        }

        for table in from.iter().skip(1) {
            (schema, rows) = self
                .apply_join_step(
                    schema,
                    rows,
                    &table.relation,
                    None,
                    &mut pending_predicates,
                    &None,
                    txn,
                    params,
                    limit,
                )
                .await?;
            (schema, rows) = self.apply_stage_join_projection(
                schema,
                rows,
                projection,
                &pending_predicates,
                &join_column_refs,
            )?;

            for join in &table.joins {
                (schema, rows) = self
                    .apply_join_step(
                        schema,
                        rows,
                        &join.relation,
                        Some(&join.join_operator),
                        &mut pending_predicates,
                        &None,
                        txn,
                        params,
                        limit,
                    )
                    .await?;
                (schema, rows) = self.apply_stage_join_projection(
                    schema,
                    rows,
                    projection,
                    &pending_predicates,
                    &join_column_refs,
                )?;
            }
        }

        if !pending_predicates.is_empty() {
            let remaining_selection = Self::combine_predicates(pending_predicates);
            if let Some(expr) = &remaining_selection {
                let capacity = limit.map_or(rows.len(), |value| rows.len().min(value));
                let mut filtered_rows = Vec::with_capacity(capacity);
                for row in rows {
                    if self.evaluate_expr(expr, &row, &schema, params)? {
                        filtered_rows.push(row);
                        if limit.is_some_and(|value| filtered_rows.len() >= value) {
                            break;
                        }
                    }
                }
                rows = filtered_rows;
            }
        } else if let Some(value) = limit {
            if rows.len() > value {
                rows.truncate(value);
            }
        }

        self.project_join_rows(schema, rows, projection)
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
