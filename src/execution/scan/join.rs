use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use sqlparser::ast::{BinaryOperator, Expr, TableFactor, TableWithJoins};
use std::collections::{HashMap, HashSet};

use super::Executor;

const JOIN_INDEX_PROBE_THRESHOLD: usize = 128;
const JOIN_UNIQUE_INDEX_PROBE_THRESHOLD: usize = 16_384;

fn join_match_bucket<T>() -> Vec<T> {
    Vec::with_capacity(1)
}

fn join_data_key_for_row_id(table_name: &str, row_id: &str) -> String {
    let mut key = String::with_capacity("data:".len() + table_name.len() + 1 + row_id.len());
    key.push_str("data:");
    key.push_str(table_name);
    key.push(':');
    key.push_str(row_id);
    key
}

struct ExprJoinProbePlan {
    left_expr: Expr,
    right_key_index: usize,
    residual_expr: Option<Expr>,
}

struct ExprHashJoinPlan {
    left_key_index: usize,
    right_expr: Expr,
    residual_expr: Option<Expr>,
}

struct CommaJoinRelationPlan {
    table: TableWithJoins,
    schema: TableSchema,
    row_count: usize,
}

impl Executor {
    fn apply_generate_subscripts_join(
        &self,
        left_schema: &TableSchema,
        left_rows: Vec<Vec<Value>>,
        relation: &TableFactor,
        params: &[Value],
        projection: &Option<Vec<String>>,
    ) -> Result<Option<(TableSchema, Vec<Vec<Value>>)>> {
        let Some(right_schema) = Self::generate_subscripts_schema(relation) else {
            return Ok(None);
        };
        let mut prefixed_right_schema = right_schema.clone();
        self.prefix_schema_columns(&mut prefixed_right_schema, relation)?;

        let mut columns = left_schema.columns.clone();
        columns.extend(prefixed_right_schema.columns.clone());
        let joined_schema = TableSchema::new("join_result".to_string(), columns);
        let mut joined_rows = Vec::with_capacity(left_rows.len().min(4096));

        for left_row in left_rows {
            let Some(result) =
                self.evaluate_generate_subscripts(relation, &left_row, left_schema, params)
            else {
                return Ok(None);
            };
            let (_, right_rows) = result?;
            for right_row in right_rows {
                let mut row = Vec::with_capacity(left_row.len() + right_row.len());
                row.extend_from_slice(&left_row);
                row.extend(right_row);
                joined_rows.push(row);
            }
        }

        self.project_join_rows(joined_schema, joined_rows, projection)
            .map(Some)
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
            return self.scan_table_base(relation, txn, params).await;
        }

        let TableFactor::Table { name, .. } = relation else {
            return self.scan_table_base(relation, txn, params).await;
        };

        let table_name = name.to_string();
        let schema_key = format!("schema:{}", table_name);
        let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? else {
            return self.scan_table_base(relation, txn, params).await;
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
            return self.scan_table_base(relation, txn, params).await;
        };

        let mut base_projection = Vec::with_capacity(stage_projection.len());
        for column in &stage_projection {
            if let Some(index) = Self::resolve_base_projection_column_index(column, &schema) {
                base_projection.push(schema.columns[index].name.clone());
            }
        }

        if base_projection.is_empty() || base_projection.len() >= schema.columns.len() {
            return self.scan_table_base(relation, txn, params).await;
        }

        self.scan_single_table(
            relation,
            &None,
            &Some(base_projection),
            txn,
            params,
            None,
            None,
            None,
        )
        .await
        .map(|(schema, rows, _)| (schema, rows))
    }

    pub(crate) fn relation_names(&self, relation: &TableFactor) -> HashSet<String> {
        let mut names = HashSet::with_capacity(2);
        match relation {
            TableFactor::Table { name, alias, .. } => {
                names.insert(name.to_string());
                if let Some(alias) = alias {
                    names.insert(alias.name.value.clone());
                }
            }
            TableFactor::Derived { alias, .. } => {
                if let Some(alias) = alias {
                    names.insert(alias.name.value.clone());
                }
            }
            _ => {}
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

    fn extract_expr_join_probe(
        &self,
        expr: &Expr,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Option<ExprJoinProbePlan> {
        let predicates = Self::collect_conjunctive_predicates(expr);
        let mut residual = Vec::with_capacity(predicates.len());
        let mut probe = None;

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

            let left_in_right = self.resolve_schema_column_index_strict(left, right_schema);
            let right_in_right = self.resolve_schema_column_index_strict(right, right_schema);

            if probe.is_none()
                && left_in_right.is_none()
                && right_in_right.is_some()
                && !matches!(
                    left.as_ref(),
                    Expr::Identifier(_) | Expr::CompoundIdentifier(_)
                )
                && self.expr_uses_only_schema(left, left_schema, right_schema)
            {
                probe = Some((left.as_ref().clone(), right_in_right.unwrap()));
                continue;
            }

            if probe.is_none()
                && right_in_right.is_none()
                && left_in_right.is_some()
                && !matches!(
                    right.as_ref(),
                    Expr::Identifier(_) | Expr::CompoundIdentifier(_)
                )
                && self.expr_uses_only_schema(right, left_schema, right_schema)
            {
                probe = Some((right.as_ref().clone(), left_in_right.unwrap()));
                continue;
            }

            residual.push(predicate);
        }

        let (left_expr, right_key_index) = probe?;
        let column = &right_schema.columns[right_key_index];
        if !(column.is_primary || (column.is_indexed && column.index_type == IndexType::BTree)) {
            return None;
        }

        Some(ExprJoinProbePlan {
            left_expr,
            right_key_index,
            residual_expr: Self::combine_predicates(residual),
        })
    }

    fn extract_expr_hash_join(
        &self,
        expr: &Expr,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Option<ExprHashJoinPlan> {
        let predicates = Self::collect_conjunctive_predicates(expr);
        let mut residual = Vec::with_capacity(predicates.len());
        let mut plan = None;

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

            if plan.is_none() {
                if let Some(left_key_index) =
                    self.resolve_schema_column_index_strict(left, left_schema)
                {
                    if self
                        .resolve_schema_column_index_strict(right, left_schema)
                        .is_none()
                        && self
                            .resolve_schema_column_index_strict(right, right_schema)
                            .is_none()
                        && self.expr_uses_only_schema(right, right_schema, left_schema)
                    {
                        plan = Some((left_key_index, right.as_ref().clone()));
                        continue;
                    }
                }

                if let Some(left_key_index) =
                    self.resolve_schema_column_index_strict(right, left_schema)
                {
                    if self
                        .resolve_schema_column_index_strict(left, left_schema)
                        .is_none()
                        && self
                            .resolve_schema_column_index_strict(left, right_schema)
                            .is_none()
                        && self.expr_uses_only_schema(left, right_schema, left_schema)
                    {
                        plan = Some((left_key_index, left.as_ref().clone()));
                        continue;
                    }
                }
            }

            residual.push(predicate);
        }

        let (left_key_index, right_expr) = plan?;
        Some(ExprHashJoinPlan {
            left_key_index,
            right_expr,
            residual_expr: Self::combine_predicates(residual),
        })
    }

    fn expr_uses_only_schema(
        &self,
        expr: &Expr,
        target_schema: &TableSchema,
        other_schema: &TableSchema,
    ) -> bool {
        let mut columns = HashSet::with_capacity(
            target_schema
                .columns
                .len()
                .saturating_add(other_schema.columns.len()),
        );
        self.extract_columns_from_expr(expr, &mut columns);
        if columns.is_empty() {
            return false;
        }
        columns.iter().all(|column| {
            self.schema_contains_column_name_strict(column, target_schema)
                && !self.schema_contains_column_name_strict(column, other_schema)
        })
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
        probe_key_guaranteed: bool,
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
            if !probe_key_guaranteed
                && !Self::row_keys_equal(left_row, left_key_indices, right_row, right_key_indices)
            {
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

        let required_capacity = projected_columns
            .len()
            .saturating_add(pending_predicates.len())
            .saturating_add(join_column_refs.len())
            .min(schema.columns.len());
        let mut required_indices = HashSet::with_capacity(required_capacity);

        for column in projected_columns {
            if let Ok(index) = self.resolve_column_index(column, schema) {
                required_indices.insert(index);
            }
        }

        for predicate in pending_predicates {
            let mut columns = HashSet::with_capacity(schema.columns.len());
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

        let mut stage_projection = Vec::with_capacity(required_indices.len());
        for (index, column) in schema.columns.iter().enumerate() {
            if required_indices.contains(&index) {
                stage_projection.push(column.name.clone());
            }
        }

        if stage_projection.is_empty() || stage_projection.len() >= schema.columns.len() {
            None
        } else {
            Some(stage_projection)
        }
    }

    fn build_stage_join_base_projection(
        &self,
        relation: &TableFactor,
        schema: &TableSchema,
        projection: &Option<Vec<String>>,
        pending_predicates: &[Expr],
        join_column_refs: &HashSet<String>,
    ) -> Option<Vec<String>> {
        let mut prefixed_schema = schema.clone();
        self.prefix_schema_columns(&mut prefixed_schema, relation)
            .ok()?;

        let stage_projection = self.build_stage_join_projection(
            &prefixed_schema,
            projection,
            pending_predicates,
            join_column_refs,
        )?;

        let mut base_projection = Vec::with_capacity(stage_projection.len());
        for column in &stage_projection {
            if let Some(index) = Self::resolve_base_projection_column_index(column, schema) {
                base_projection.push(schema.columns[index].name.clone());
            }
        }

        if base_projection.is_empty() || base_projection.len() >= schema.columns.len() {
            None
        } else {
            Some(base_projection)
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
        projection_indices: Option<&[usize]>,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<Vec<Value>>> {
        let column = &schema.columns[key_col_idx];

        if column.is_primary && projection_indices.is_none() {
            if let Some(row_id) = Self::value_to_primary_row_id(key_value) {
                if let Some(row) = self.fetch_full_row_by_id(table_name, &row_id, txn).await? {
                    return Ok(vec![row]);
                }
            }
            return Ok(Vec::new());
        } else if column.is_primary {
            let Some(row_id) = Self::value_to_primary_row_id(key_value) else {
                return Ok(Vec::new());
            };
            let data_key = join_data_key_for_row_id(table_name, &row_id);
            if let Some(row) = self.row_cache.get(&data_key) {
                monitor::inc_row_cache_hit();
                return Ok(vec![row]);
            }

            let Some(data_bytes) = txn.get(data_key.as_bytes()).await? else {
                return Ok(Vec::new());
            };
            monitor::inc_row_read();
            let row =
                Self::decode_row_for_projection(&data_bytes, projection_indices).map_err(|e| {
                    FusionError::Execution(format!("Data deserialization error: {}", e))
                })?;
            return Ok(vec![row]);
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
            if projection_indices.is_none() {
                if let Some(row) = self.fetch_full_row_by_id(table_name, row_id, txn).await? {
                    rows.push(row);
                }
            } else {
                let data_key = join_data_key_for_row_id(table_name, row_id);
                if let Some(row) = self.row_cache.get(&data_key) {
                    monitor::inc_row_cache_hit();
                    rows.push(row);
                    continue;
                }

                let Some(data_bytes) = txn.get(data_key.as_bytes()).await? else {
                    continue;
                };
                monitor::inc_row_read();
                let row = Self::decode_row_for_projection(&data_bytes, projection_indices)
                    .map_err(|e| {
                        FusionError::Execution(format!("Data deserialization error: {}", e))
                    })?;
                rows.push(row);
            }
        }

        Ok(rows)
    }

    fn projection_indices_for_schema(
        &self,
        projection: &Option<Vec<String>>,
        schema: &TableSchema,
    ) -> Option<Vec<usize>> {
        let columns = projection.as_ref()?;
        let mut indices = Vec::with_capacity(columns.len());
        for column in columns {
            if let Some(index) = Self::resolve_base_projection_column_index(column, schema) {
                indices.push(index);
            }
        }
        if columns.is_empty() || !indices.is_empty() {
            Some(indices)
        } else {
            None
        }
    }

    fn remove_projection_column(
        &self,
        projection: &Option<Vec<String>>,
        schema: &TableSchema,
        column_idx: usize,
    ) -> Option<Vec<String>> {
        let Some(columns) = projection.as_ref() else {
            let mut filtered = Vec::with_capacity(schema.columns.len().saturating_sub(1));
            for (index, column) in schema.columns.iter().enumerate() {
                if index != column_idx {
                    filtered.push(column.name.clone());
                }
            }
            return Some(filtered);
        };
        let mut filtered = Vec::with_capacity(columns.len());
        for column in columns {
            if Self::resolve_base_projection_column_index(column, schema)
                .is_none_or(|index| index != column_idx)
            {
                filtered.push(column.clone());
            }
        }

        if filtered.len() == columns.len() {
            projection.clone()
        } else {
            Some(filtered)
        }
    }

    fn resolve_base_projection_column_index(column: &str, schema: &TableSchema) -> Option<usize> {
        if let Some(index) = schema
            .columns
            .iter()
            .position(|schema_column| schema_column.name.eq_ignore_ascii_case(column))
        {
            return Some(index);
        }

        let suffix = column.rsplit('.').next().unwrap_or(column);
        schema.columns.iter().position(|schema_column| {
            schema_column.name.eq_ignore_ascii_case(suffix)
                || schema_column
                    .name
                    .rsplit('.')
                    .next()
                    .is_some_and(|schema_suffix| schema_suffix.eq_ignore_ascii_case(suffix))
        })
    }

    fn restore_probe_key_column(rows: &mut [Vec<Value>], column_idx: usize, key_value: &Value) {
        for row in rows {
            if let Some(value) = row.get_mut(column_idx) {
                *value = key_value.clone();
            }
        }
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
        unique_probe_key: bool,
    ) -> bool {
        distinct_probe_keys > 0
            && (left_rows_len <= JOIN_INDEX_PROBE_THRESHOLD
                || distinct_probe_keys <= JOIN_INDEX_PROBE_THRESHOLD
                || limit.is_some_and(|value| value <= JOIN_INDEX_PROBE_THRESHOLD)
                || (unique_probe_key && distinct_probe_keys <= JOIN_UNIQUE_INDEX_PROBE_THRESHOLD))
    }

    fn combine_optional_predicates(predicates: Vec<Option<Expr>>) -> Option<Expr> {
        let mut combined = Vec::with_capacity(predicates.len());
        for predicate in predicates {
            if let Some(predicate) = predicate {
                combined.push(predicate);
            }
        }
        Self::combine_predicates(combined)
    }

    async fn reorder_comma_join_from(
        &self,
        from: &[TableWithJoins],
        selection: &Option<Expr>,
        _projection: &Option<Vec<String>>,
        _has_deferred_subquery_filter: bool,
        txn: &mut dyn Transaction,
    ) -> Result<Option<Vec<TableWithJoins>>> {
        let Some(selection) = selection else {
            return Ok(None);
        };
        if from.len() < 3 || from.iter().any(|table| !table.joins.is_empty()) {
            return Ok(None);
        }

        let mut relations = Vec::with_capacity(from.len());
        let mut passthrough = Vec::with_capacity(from.len());
        for (original_index, table) in from.iter().enumerate() {
            let TableFactor::Table { name, args, .. } = &table.relation else {
                passthrough.push((original_index, table.clone()));
                continue;
            };
            if args.is_some() {
                return Ok(None);
            };
            let table_name = name.to_string();
            let schema_key = format!("schema:{}", name);
            let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? else {
                return Ok(None);
            };
            let mut schema: TableSchema = bincode::deserialize(&schema_bytes).map_err(|e| {
                FusionError::Execution(format!("Schema deserialization error: {}", e))
            })?;
            self.prefix_schema_columns(&mut schema, &table.relation)?;
            let row_count = if let Some(stats) = self.load_table_stats(&table_name, txn).await? {
                stats.row_count
            } else {
                let data_prefix = format!("data:{}:", table_name);
                txn.count_prefix(data_prefix.as_bytes())
                    .await
                    .unwrap_or(usize::MAX)
            };
            relations.push(CommaJoinRelationPlan {
                table: table.clone(),
                schema,
                row_count,
            });
        }

        if relations.len() < 3 {
            return Ok(None);
        }

        let predicates = Self::collect_conjunctive_predicates(selection);
        let relation_count = relations.len();
        let mut local_counts = vec![0usize; relation_count];
        let mut edge_counts = vec![vec![0usize; relation_count]; relation_count];
        let mut schemas = Vec::with_capacity(relation_count);
        for relation in &relations {
            schemas.push(relation.schema.clone());
        }

        for predicate in &predicates {
            let Some(members) = self.predicate_schema_members(predicate, &schemas) else {
                continue;
            };

            if members.len() == 1 {
                local_counts[members[0]] += 1;
            } else if members.len() == 2 {
                edge_counts[members[0]][members[1]] += 1;
                edge_counts[members[1]][members[0]] += 1;
            }
        }

        let has_join_edge = edge_counts
            .iter()
            .any(|row| row.iter().any(|count| *count > 0));
        if !has_join_edge {
            return Ok(None);
        }

        let degree =
            |index: usize, edge_counts: &[Vec<usize>]| -> usize { edge_counts[index].iter().sum() };

        let mut remaining = vec![true; relation_count];
        let mut order = Vec::with_capacity(relation_count);
        let mut first = 0usize;
        let mut first_score = 0usize;
        let mut first_rows = usize::MAX;
        for index in 0..relation_count {
            let score = local_counts[index]
                .saturating_mul(100)
                .saturating_add(degree(index, &edge_counts));
            let rows = relations[index].row_count;
            if score > first_score
                || (score == first_score && rows < first_rows)
                || (score == first_score && rows == first_rows && index > first)
            {
                first = index;
                first_score = score;
                first_rows = rows;
            }
        }
        remaining[first] = false;
        order.push(first);

        while remaining.iter().any(|value| *value) {
            let mut next = None;
            let mut next_score = 0usize;
            let mut next_rows = usize::MAX;
            for candidate in 0..relation_count {
                if !remaining[candidate] {
                    continue;
                }
                let connected_edges = order
                    .iter()
                    .map(|placed| edge_counts[candidate][*placed])
                    .sum::<usize>();
                let score = connected_edges
                    .saturating_mul(1000)
                    .saturating_add(local_counts[candidate].saturating_mul(100))
                    .saturating_add(degree(candidate, &edge_counts));
                let rows = relations[candidate].row_count;
                if next.is_none()
                    || score > next_score
                    || (score == next_score && rows < next_rows)
                    || (score == next_score
                        && rows == next_rows
                        && candidate > next.unwrap_or(candidate))
                {
                    next = Some(candidate);
                    next_score = score;
                    next_rows = rows;
                }
            }
            let next = next.unwrap();
            remaining[next] = false;
            order.push(next);
        }

        let mut reordered = Vec::with_capacity(order.len() + passthrough.len());
        for index in &order {
            reordered.push(relations[*index].table.clone());
        }
        passthrough.sort_by_key(|(original_index, _)| *original_index);
        reordered.extend(passthrough.into_iter().map(|(_, table)| table));

        let changed = reordered
            .iter()
            .zip(from.iter())
            .any(|(left, right)| left.relation != right.relation);
        if !changed {
            return Ok(None);
        }

        Ok(Some(reordered))
    }

    fn predicate_schema_members(&self, expr: &Expr, schemas: &[TableSchema]) -> Option<Vec<usize>> {
        let column_capacity = schemas.iter().fold(0usize, |capacity, schema| {
            capacity.saturating_add(schema.columns.len())
        });
        let mut columns = HashSet::with_capacity(column_capacity);
        self.extract_columns_from_expr(expr, &mut columns);
        if columns.is_empty() {
            return None;
        }

        let mut members = HashSet::with_capacity(schemas.len());
        for column in columns {
            let mut matched = false;
            for (index, schema) in schemas.iter().enumerate() {
                if self.schema_contains_column_name_strict(&column, schema) {
                    members.insert(index);
                    matched = true;
                }
            }
            if !matched {
                return None;
            }
        }

        let mut sorted_members = Vec::with_capacity(members.len());
        sorted_members.extend(members);
        sorted_members.sort_unstable();
        Some(sorted_members)
    }

    fn schema_contains_column_name_strict(&self, col_name: &str, schema: &TableSchema) -> bool {
        if col_name.contains('.') {
            schema
                .columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(col_name))
        } else {
            self.resolve_column_index(col_name, schema).is_ok()
        }
    }

    fn predicate_schema_membership(&self, expr: &Expr, schema: &TableSchema) -> Option<bool> {
        let mut columns = HashSet::with_capacity(schema.columns.len());
        self.extract_columns_from_expr(expr, &mut columns);
        if columns.is_empty() {
            return None;
        }

        Some(
            columns
                .iter()
                .any(|column| self.schema_contains_column_name_strict(column, schema)),
        )
    }

    fn predicate_uses_only_target_schema(
        &self,
        expr: &Expr,
        target_schema: &TableSchema,
        other_schema: &TableSchema,
    ) -> bool {
        let mut columns = HashSet::with_capacity(
            target_schema
                .columns
                .len()
                .saturating_add(other_schema.columns.len()),
        );
        self.extract_columns_from_expr(expr, &mut columns);
        if columns.is_empty() {
            return false;
        }

        columns.iter().all(|column| {
            self.schema_contains_column_name_strict(column, target_schema)
                && !self.schema_contains_column_name_strict(column, other_schema)
        })
    }

    fn predicate_spans_schema_pair(
        &self,
        expr: &Expr,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> bool {
        matches!(
            (
                self.predicate_schema_membership(expr, left_schema),
                self.predicate_schema_membership(expr, right_schema),
            ),
            (Some(true), Some(true))
        )
    }

    fn take_exclusive_schema_predicate(
        &self,
        predicates: &mut Vec<Expr>,
        target_schema: &TableSchema,
        other_schema: &TableSchema,
    ) -> Option<Expr> {
        let predicate_count = predicates.len();
        let mut local = Vec::with_capacity(predicate_count);
        let mut remaining = Vec::with_capacity(predicate_count);

        for predicate in predicates.drain(..) {
            if self.predicate_uses_only_target_schema(&predicate, target_schema, other_schema) {
                local.push(predicate);
            } else {
                remaining.push(predicate);
            }
        }

        *predicates = remaining;
        Self::combine_predicates(local)
    }

    fn take_schema_pair_predicates(
        &self,
        predicates: &mut Vec<Expr>,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Vec<Expr> {
        let predicate_count = predicates.len();
        let mut join_predicates = Vec::with_capacity(predicate_count);
        let mut remaining = Vec::with_capacity(predicate_count);

        for predicate in predicates.drain(..) {
            if self.predicate_spans_schema_pair(&predicate, left_schema, right_schema) {
                join_predicates.push(predicate);
            } else {
                remaining.push(predicate);
            }
        }

        *predicates = remaining;
        join_predicates
    }

    pub(super) fn filter_rows_with_expr(
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
        let prefix = match relation {
            TableFactor::Table { name, alias, .. } => alias
                .as_ref()
                .map(|alias| alias.name.value.clone())
                .unwrap_or_else(|| name.to_string()),
            TableFactor::Derived { alias, .. } => {
                let Some(alias) = alias else {
                    return Ok(());
                };
                alias.name.value.clone()
            }
            _ => return Ok(()),
        };

        for col in &mut schema.columns {
            if !col.name.contains('.') {
                col.name = format!("{}.{}", prefix, col.name);
            }
        }
        Ok(())
    }

    async fn apply_join_step(
        &self,
        left_schema: TableSchema,
        left_rows: Vec<Vec<Value>>,
        relation: &TableFactor,
        join_operator: Option<&sqlparser::ast::JoinOperator>,
        pending_predicates: &mut Vec<Expr>,
        projection: &Option<Vec<String>>,
        stage_projection_hint: &Option<Vec<String>>,
        join_column_refs: &HashSet<String>,
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

        if let Some(result) = self.apply_generate_subscripts_join(
            &left_schema,
            left_rows.clone(),
            relation,
            params,
            projection,
        )? {
            return Ok(result);
        }

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

        let right_schema_for_predicates = schema_for_probe
            .as_ref()
            .map(|schema| {
                let mut prefixed = schema.clone();
                self.prefix_schema_columns(&mut prefixed, relation)?;
                Ok::<TableSchema, FusionError>(prefixed)
            })
            .transpose()?;

        if let Some(right_schema) = &right_schema_for_predicates {
            if let Some(left_local) =
                self.take_exclusive_schema_predicate(pending_predicates, &left_schema, right_schema)
            {
                left_rows =
                    self.filter_rows_with_expr(left_rows, &left_schema, &left_local, params)?;
            }

            if join_operator.is_none() {
                join_predicates.extend(self.take_schema_pair_predicates(
                    pending_predicates,
                    &left_schema,
                    right_schema,
                ));
            }
        }

        let where_right = self.take_relation_predicate(pending_predicates, &right_relation_names);
        let where_right_schema = right_schema_for_predicates
            .as_ref()
            .and_then(|right_schema| {
                self.take_exclusive_schema_predicate(pending_predicates, right_schema, &left_schema)
            });
        let join_right = self.take_relation_predicate(&mut join_predicates, &right_relation_names);
        let right_selection =
            Self::combine_optional_predicates(vec![where_right, where_right_schema, join_right]);

        let is_left_outer = matches!(
            join_operator,
            Some(
                sqlparser::ast::JoinOperator::LeftOuter(_) | sqlparser::ast::JoinOperator::Left(_)
            )
        );
        let supports_left_driven_probe = matches!(
            join_operator,
            None | Some(
                sqlparser::ast::JoinOperator::Inner(_)
                    | sqlparser::ast::JoinOperator::Join(_)
                    | sqlparser::ast::JoinOperator::LeftOuter(_)
                    | sqlparser::ast::JoinOperator::Left(_)
            )
        );

        let join_expr = Self::combine_predicates(join_predicates.clone());
        let mut right_stage_predicates = Vec::with_capacity(
            pending_predicates.len()
                + usize::from(right_selection.is_some())
                + usize::from(join_expr.is_some()),
        );
        if let Some(predicate) = &right_selection {
            right_stage_predicates.push(predicate.clone());
        }
        if let Some(predicate) = &join_expr {
            right_stage_predicates.push(predicate.clone());
        }
        right_stage_predicates.extend(pending_predicates.iter().cloned());
        let right_projection = schema_for_probe.as_ref().and_then(|schema| {
            self.build_stage_join_base_projection(
                relation,
                schema,
                stage_projection_hint,
                &right_stage_predicates,
                join_column_refs,
            )
        });
        let right_projection_indices = schema_for_probe
            .as_ref()
            .and_then(|schema| self.projection_indices_for_schema(&right_projection, schema));
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
                            probe_schema.columns[probe_right_idx].is_unique,
                        ) {
                            monitor::inc_plan();
                            let right_table_name = right_table_name.unwrap();
                            let single_key_indexed_probe = left_key_indices.len() == 1
                                && right_key_indices.len() == 1
                                && residual_expr.is_none()
                                && right_selection.is_none();
                            let probe_key_guaranteed =
                                single_key_indexed_probe && probe_right_idx == right_key_indices[0];
                            let probe_projection_indices = if probe_key_guaranteed {
                                let reduced_projection = self.remove_projection_column(
                                    &right_projection,
                                    probe_schema,
                                    probe_right_idx,
                                );
                                self.projection_indices_for_schema(
                                    &reduced_projection,
                                    probe_schema,
                                )
                            } else {
                                right_projection_indices.clone()
                            };
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
                                        probe_key_guaranteed,
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
                                        probe_projection_indices.as_deref(),
                                        txn,
                                    )
                                    .await?;
                                if probe_key_guaranteed {
                                    Self::restore_probe_key_column(
                                        &mut candidates,
                                        probe_right_idx,
                                        &probe_key,
                                    );
                                }
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
                                    probe_key_guaranteed,
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
                } else if let Some(expr_probe) =
                    self.extract_expr_join_probe(expr, &left_schema, &right_schema_for_probe)
                {
                    let mut distinct_probe_keys = HashSet::with_capacity(left_rows.len());
                    for left_row in &left_rows {
                        let probe_key = self.evaluate_value(
                            &expr_probe.left_expr,
                            left_row,
                            &left_schema,
                            params,
                        )?;
                        distinct_probe_keys.insert(probe_key);
                    }

                    if Self::should_attempt_join_probe(
                        left_rows.len(),
                        distinct_probe_keys.len(),
                        limit,
                        probe_schema.columns[expr_probe.right_key_index].is_unique,
                    ) {
                        monitor::inc_plan();
                        let right_table_name = right_table_name.unwrap();
                        let probe_projection_indices = right_projection_indices.clone();
                        let probed_capacity =
                            limit.map_or(left_rows.len(), |value| left_rows.len().min(value));
                        let mut probed_rows = Vec::with_capacity(probed_capacity);
                        let mut probe_cache: HashMap<Value, Vec<Vec<Value>>> =
                            HashMap::with_capacity(distinct_probe_keys.len());

                        for left_row in &left_rows {
                            let probe_key = self.evaluate_value(
                                &expr_probe.left_expr,
                                left_row,
                                &left_schema,
                                params,
                            )?;
                            if let Some(candidates) = probe_cache.get(&probe_key) {
                                for right_row in candidates {
                                    let mut joined_row =
                                        Vec::with_capacity(left_row.len() + right_row.len());
                                    joined_row.extend_from_slice(left_row);
                                    joined_row.extend_from_slice(right_row);
                                    if let Some(residual) = &expr_probe.residual_expr {
                                        if !self.evaluate_expr(
                                            residual,
                                            &joined_row,
                                            &new_schema,
                                            params,
                                        )? {
                                            continue;
                                        }
                                    }
                                    probed_rows.push(joined_row);
                                    if limit.is_some_and(|value| probed_rows.len() >= value) {
                                        return self.project_join_rows(
                                            new_schema,
                                            probed_rows,
                                            projection,
                                        );
                                    }
                                }
                                continue;
                            }

                            let mut candidates = self
                                .fetch_rows_by_join_key(
                                    &right_table_name,
                                    probe_schema,
                                    expr_probe.right_key_index,
                                    &probe_key,
                                    probe_projection_indices.as_deref(),
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
                            for right_row in &candidates {
                                let mut joined_row =
                                    Vec::with_capacity(left_row.len() + right_row.len());
                                joined_row.extend_from_slice(left_row);
                                joined_row.extend_from_slice(right_row);
                                if let Some(residual) = &expr_probe.residual_expr {
                                    if !self.evaluate_expr(
                                        residual,
                                        &joined_row,
                                        &new_schema,
                                        params,
                                    )? {
                                        continue;
                                    }
                                }
                                probed_rows.push(joined_row);
                                if limit.is_some_and(|value| probed_rows.len() >= value) {
                                    probe_cache.insert(probe_key, candidates);
                                    return self.project_join_rows(
                                        new_schema,
                                        probed_rows,
                                        projection,
                                    );
                                }
                            }
                            probe_cache.insert(probe_key, candidates);
                        }

                        return self.project_join_rows(new_schema, probed_rows, projection);
                    }
                }
            }
        }

        let (right_schema_base, mut right_rows) = if right_selection.is_some() {
            let (schema, rows, _) = self
                .scan_single_table(
                    relation,
                    &right_selection,
                    &right_projection,
                    txn,
                    params,
                    None,
                    None,
                    None,
                )
                .await?;
            (schema, rows)
        } else {
            self.scan_join_base(
                relation,
                stage_projection_hint,
                &right_stage_predicates,
                join_column_refs,
                txn,
                params,
            )
            .await?
        };
        let mut right_schema = right_schema_base.clone();
        self.prefix_schema_columns(&mut right_schema, relation)?;

        if right_schema_for_predicates.is_none() {
            if let Some(left_local) = self.take_exclusive_schema_predicate(
                pending_predicates,
                &left_schema,
                &right_schema,
            ) {
                left_rows =
                    self.filter_rows_with_expr(left_rows, &left_schema, &left_local, params)?;
            }

            if join_operator.is_none() {
                join_predicates.extend(self.take_schema_pair_predicates(
                    pending_predicates,
                    &left_schema,
                    &right_schema,
                ));
            }

            if let Some(right_local) = self.take_exclusive_schema_predicate(
                pending_predicates,
                &right_schema,
                &left_schema,
            ) {
                right_rows =
                    self.filter_rows_with_expr(right_rows, &right_schema, &right_local, params)?;
            }
        }

        let join_expr = Self::combine_predicates(join_predicates.clone());
        let mut new_columns = left_schema.columns.clone();
        new_columns.extend(right_schema.columns.clone());
        let new_schema = TableSchema::new("join_result".to_string(), new_columns);

        let row_width = left_schema.columns.len() + right_schema.columns.len();
        let expected_rows = if is_left_outer {
            left_rows.len()
        } else {
            left_rows.len().min(right_rows.len())
        };
        let output_capacity = limit
            .map_or(expected_rows, |limit| expected_rows.min(limit))
            .min(4096);
        let mut new_rows = Vec::with_capacity(output_capacity);
        let mut hash_join_executed = false;

        if !matches!(
            join_operator,
            Some(sqlparser::ast::JoinOperator::CrossJoin(_))
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
                            hash_map
                                .entry(key)
                                .or_insert_with(join_match_bucket)
                                .push(right_row);
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
                            hash_map
                                .entry(key)
                                .or_insert_with(join_match_bucket)
                                .push(left_row);
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
                } else if let Some(expr_plan) =
                    self.extract_expr_hash_join(expr, &left_schema, &right_schema)
                {
                    hash_join_executed = true;
                    monitor::inc_plan();

                    let mut hash_map: HashMap<Value, Vec<&Vec<Value>>> =
                        HashMap::with_capacity(left_rows.len());
                    for left_row in &left_rows {
                        hash_map
                            .entry(left_row[expr_plan.left_key_index].clone())
                            .or_default()
                            .push(left_row);
                    }

                    for right_row in &right_rows {
                        let right_key = self.evaluate_value(
                            &expr_plan.right_expr,
                            right_row,
                            &right_schema,
                            params,
                        )?;
                        if matches!(right_key, Value::Null) {
                            continue;
                        }
                        if let Some(matches) = hash_map.get(&right_key) {
                            for left_row in matches {
                                let mut joined_row = Vec::with_capacity(row_width);
                                joined_row.extend_from_slice(left_row);
                                joined_row.extend_from_slice(right_row);
                                if let Some(residual) = &expr_plan.residual_expr {
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
        has_deferred_subquery_filter: bool,
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        let reordered_from = self
            .reorder_comma_join_from(
                from,
                selection,
                projection,
                has_deferred_subquery_filter,
                txn,
            )
            .await?;
        let planned_from = reordered_from.as_deref().unwrap_or(from);

        let first = &planned_from[0];
        let join_column_refs = self.collect_join_column_references(planned_from);
        let mut pending_predicates = if let Some(expr) = selection {
            Self::collect_conjunctive_predicates(expr)
        } else {
            Vec::new()
        };

        let first_relation_names = self.relation_names(&first.relation);
        let first_selection =
            self.take_relation_predicate(&mut pending_predicates, &first_relation_names);
        let first_projection = if first_selection.is_some() {
            if let TableFactor::Table { name, .. } = &first.relation {
                let schema_key = format!("schema:{}", name);
                txn.get(schema_key.as_bytes())
                    .await?
                    .map(|schema_bytes| {
                        bincode::deserialize::<TableSchema>(&schema_bytes).map_err(|e| {
                            FusionError::Execution(format!("Schema deserialization error: {}", e))
                        })
                    })
                    .transpose()?
                    .and_then(|schema| {
                        let mut local_predicates = Vec::with_capacity(
                            pending_predicates.len() + usize::from(first_selection.is_some()),
                        );
                        if let Some(predicate) = &first_selection {
                            local_predicates.push(predicate.clone());
                        }
                        local_predicates.extend(pending_predicates.iter().cloned());
                        self.build_stage_join_base_projection(
                            &first.relation,
                            &schema,
                            projection,
                            &local_predicates,
                            &join_column_refs,
                        )
                    })
            } else {
                None
            }
        } else {
            None
        };
        let (mut schema, mut rows) = if first_selection.is_some() {
            let (schema, rows, _) = self
                .scan_single_table(
                    &first.relation,
                    &first_selection,
                    &first_projection,
                    txn,
                    params,
                    None,
                    None,
                    None,
                )
                .await?;
            (schema, rows)
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
                    projection,
                    &join_column_refs,
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

        for table in planned_from.iter().skip(1) {
            (schema, rows) = self
                .apply_join_step(
                    schema,
                    rows,
                    &table.relation,
                    None,
                    &mut pending_predicates,
                    &None,
                    projection,
                    &join_column_refs,
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
                        projection,
                        &join_column_refs,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_match_bucket_preallocates_first_match() {
        let bucket: Vec<Vec<Value>> = join_match_bucket();
        assert!(bucket.capacity() >= 1);
    }

    #[test]
    fn join_data_key_for_row_id_preallocates_exact_key() {
        let key = join_data_key_for_row_id("orders", "00042");

        assert_eq!(key, "data:orders:00042");
        assert!(key.capacity() >= key.len());
    }
}
