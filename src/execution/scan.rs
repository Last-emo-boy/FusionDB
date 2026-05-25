use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use futures::stream::StreamExt;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, TableFactor,
    TableWithJoins, Value as SqlValue,
};
use std::collections::{HashMap, HashSet};

use super::Executor;

const SMALL_INDEX_FETCH_THRESHOLD: usize = 64;
const JOIN_INDEX_PROBE_THRESHOLD: usize = 128;

pub(crate) struct IndexScanPlan {
    row_ids: HashSet<String>,
    exact: bool,
}

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
        let mut names = HashSet::new();
        if let TableFactor::Table { name, alias, .. } = relation {
            names.insert(name.to_string());
            if let Some(alias) = alias {
                names.insert(alias.name.value.clone());
            }
        }
        names
    }

    fn split_conjunctive_predicates(expr: &Expr, out: &mut Vec<Expr>) {
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } = expr
        {
            Self::split_conjunctive_predicates(left, out);
            Self::split_conjunctive_predicates(right, out);
        } else {
            out.push(expr.clone());
        }
    }

    fn combine_predicates(predicates: Vec<Expr>) -> Option<Expr> {
        let mut iter = predicates.into_iter();
        let first = iter.next()?;
        Some(iter.fold(first, |acc, expr| Expr::BinaryOp {
            left: Box::new(acc),
            op: BinaryOperator::And,
            right: Box::new(expr),
        }))
    }

    fn predicate_uses_only_relations(&self, expr: &Expr, relation_names: &HashSet<String>) -> bool {
        let mut columns = HashSet::new();
        self.extract_columns_from_expr(expr, &mut columns);
        if columns.is_empty() {
            return false;
        }

        columns.into_iter().all(|column| {
            column
                .split('.')
                .next()
                .map(|prefix| relation_names.contains(prefix))
                .unwrap_or(false)
        })
    }

    fn take_relation_predicate(
        &self,
        predicates: &mut Vec<Expr>,
        relation_names: &HashSet<String>,
    ) -> Option<Expr> {
        let mut local = Vec::new();
        let mut remaining = Vec::new();

        for predicate in predicates.drain(..) {
            if self.predicate_uses_only_relations(&predicate, relation_names) {
                local.push(predicate);
            } else {
                remaining.push(predicate);
            }
        }

        *predicates = remaining;
        Self::combine_predicates(local)
    }

    fn predicate_uses_only_schema(&self, expr: &Expr, schema: &TableSchema) -> bool {
        let mut columns = HashSet::new();
        self.extract_columns_from_expr(expr, &mut columns);
        if columns.is_empty() {
            return false;
        }

        columns
            .into_iter()
            .all(|column| self.schema_contains_column_reference(&column, schema))
    }

    fn take_schema_predicate(
        &self,
        predicates: &mut Vec<Expr>,
        schema: &TableSchema,
    ) -> Option<Expr> {
        let mut local = Vec::new();
        let mut remaining = Vec::new();

        for predicate in predicates.drain(..) {
            if self.predicate_uses_only_schema(&predicate, schema) {
                local.push(predicate);
            } else {
                remaining.push(predicate);
            }
        }

        *predicates = remaining;
        Self::combine_predicates(local)
    }

    fn column_name_from_expr(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            Expr::CompoundIdentifier(idents) => Some(Self::scan_compound_identifier_name(idents)),
            _ => None,
        }
    }

    fn scan_compound_identifier_name(idents: &[sqlparser::ast::Ident]) -> String {
        let capacity = idents.iter().map(|ident| ident.value.len()).sum::<usize>()
            + idents.len().saturating_sub(1);
        let mut name = String::with_capacity(capacity);

        for (index, ident) in idents.iter().enumerate() {
            if index > 0 {
                name.push('.');
            }
            name.push_str(&ident.value);
        }

        name
    }

    fn resolve_schema_column_index(&self, expr: &Expr, schema: &TableSchema) -> Option<usize> {
        let col_name = Self::column_name_from_expr(expr)?;
        self.resolve_column_index(&col_name, schema).ok()
    }

    fn resolve_schema_column_index_strict(
        &self,
        expr: &Expr,
        schema: &TableSchema,
    ) -> Option<usize> {
        let col_name = Self::column_name_from_expr(expr)?;
        if col_name.contains('.') {
            schema
                .columns
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(&col_name))
        } else {
            self.resolve_column_index(&col_name, schema).ok()
        }
    }

    fn schema_contains_column_reference(&self, col_name: &str, schema: &TableSchema) -> bool {
        if col_name.contains('.') {
            schema
                .columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(col_name))
        } else {
            self.resolve_column_index(col_name, schema).is_ok()
        }
    }

    fn resolve_schema_column_name(
        &self,
        expr: &Expr,
        schema: &TableSchema,
    ) -> Option<(usize, String)> {
        let idx = self.resolve_schema_column_index(expr, schema)?;
        Some((idx, schema.columns[idx].name.clone()))
    }

    fn equality_schema_column_value_expr<'a>(
        &self,
        left: &'a Expr,
        right: &'a Expr,
        schema: &TableSchema,
    ) -> Option<(usize, String, &'a Expr)> {
        if let Some((idx, name)) = self.resolve_schema_column_name(left, schema) {
            if self.expr_has_column_reference(right) {
                None
            } else {
                Some((idx, name, right))
            }
        } else {
            self.resolve_schema_column_name(right, schema)
                .and_then(|(idx, name)| {
                    if self.expr_has_column_reference(left) {
                        None
                    } else {
                        Some((idx, name, left))
                    }
                })
        }
    }

    fn equality_primary_key_value_expr<'a>(
        &self,
        left: &'a Expr,
        right: &'a Expr,
        schema: &TableSchema,
        pk_idx: usize,
    ) -> Option<&'a Expr> {
        if self.resolve_schema_column_index(left, schema) == Some(pk_idx) {
            if self.expr_has_column_reference(right) {
                None
            } else {
                Some(right)
            }
        } else if self.resolve_schema_column_index(right, schema) == Some(pk_idx) {
            if self.expr_has_column_reference(left) {
                None
            } else {
                Some(left)
            }
        } else {
            None
        }
    }

    fn primary_key_range_value_expr<'a>(
        &self,
        left: &'a Expr,
        op: &BinaryOperator,
        right: &'a Expr,
        schema: &TableSchema,
        pk_idx: usize,
    ) -> Option<(BinaryOperator, &'a Expr)> {
        let normalized_op = match op {
            BinaryOperator::Gt => BinaryOperator::Gt,
            BinaryOperator::GtEq => BinaryOperator::GtEq,
            BinaryOperator::Lt => BinaryOperator::Lt,
            BinaryOperator::LtEq => BinaryOperator::LtEq,
            _ => return None,
        };

        if self.resolve_schema_column_index(left, schema) == Some(pk_idx) {
            if self.expr_has_column_reference(right) {
                None
            } else {
                Some((normalized_op, right))
            }
        } else if self.resolve_schema_column_index(right, schema) == Some(pk_idx) {
            if self.expr_has_column_reference(left) {
                return None;
            }
            let flipped_op = match op {
                BinaryOperator::Gt => BinaryOperator::Lt,
                BinaryOperator::GtEq => BinaryOperator::LtEq,
                BinaryOperator::Lt => BinaryOperator::Gt,
                BinaryOperator::LtEq => BinaryOperator::GtEq,
                _ => return None,
            };
            Some((flipped_op, left))
        } else {
            None
        }
    }

    fn expr_has_column_reference(&self, expr: &Expr) -> bool {
        let mut cols = HashSet::new();
        self.extract_columns_from_expr(expr, &mut cols);
        !cols.is_empty()
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
        let mut predicates = Vec::new();
        Self::split_conjunctive_predicates(expr, &mut predicates);

        let mut left_key_indices = Vec::new();
        let mut right_key_indices = Vec::new();
        let mut residual = Vec::new();

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
        let mut columns = HashSet::new();

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

        let mut required = HashSet::new();

        for column in projected_columns {
            if let Ok(index) = self.resolve_column_index(column, schema) {
                required.insert(schema.columns[index].name.to_ascii_lowercase());
            }
        }

        for predicate in pending_predicates {
            let mut columns = HashSet::new();
            self.extract_columns_from_expr(predicate, &mut columns);
            for column in columns {
                if let Ok(index) = self.resolve_column_index(&column, schema) {
                    required.insert(schema.columns[index].name.to_ascii_lowercase());
                }
            }
        }

        for column in join_column_refs {
            if let Ok(index) = self.resolve_column_index(column, schema) {
                required.insert(schema.columns[index].name.to_ascii_lowercase());
            }
        }

        if required.is_empty() || required.len() >= schema.columns.len() {
            return None;
        }

        let stage_projection: Vec<String> = schema
            .columns
            .iter()
            .filter(|column| required.contains(&column.name.to_ascii_lowercase()))
            .map(|column| column.name.clone())
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

    fn value_to_primary_row_id(value: &Value) -> Option<String> {
        match value {
            Value::Integer(i) => Some(crate::common::encoding::encode_i64_comparable(*i)),
            Value::String(s) => Some(s.clone()),
            _ => None,
        }
    }

    fn row_id_from_key(key: &[u8]) -> Option<&str> {
        std::str::from_utf8(key)
            .ok()?
            .rsplit(':')
            .next()
            .filter(|row_id| !row_id.is_empty())
    }

    fn primary_key_row_from_id(
        schema: &TableSchema,
        pk_index: Option<usize>,
        row_id: &str,
    ) -> Vec<Value> {
        let mut row = vec![Value::Null; schema.columns.len()];
        if let Some(pk_idx) = pk_index {
            if pk_idx < schema.columns.len() {
                let is_int = matches!(
                    schema.columns[pk_idx].data_type.as_str(),
                    "INTEGER" | "BIGINT"
                );
                row[pk_idx] = if is_int {
                    crate::common::encoding::decode_i64_comparable(row_id)
                        .map(Value::Integer)
                        .unwrap_or_else(|| Value::String(row_id.to_string()))
                } else {
                    Value::String(row_id.to_string())
                };
            }
        }
        row
    }

    fn decode_row_for_projection(
        data: &[u8],
        projection_indices: Option<&[usize]>,
    ) -> bincode::Result<Vec<Value>> {
        match projection_indices {
            Some(indices) if indices.is_empty() => Ok(Vec::new()),
            Some(indices) => crate::common::encoding::RowDecoder::decode_partial(data, indices),
            None => crate::common::encoding::RowDecoder::decode(data),
        }
    }

    async fn fetch_full_row_by_id(
        &self,
        table_name: &str,
        row_id: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Option<Vec<Value>>> {
        let data_key = format!("data:{}:{}", table_name, row_id);
        if let Some(row) = self.row_cache.get(&data_key) {
            monitor::inc_row_cache_hit();
            return Ok(Some(row));
        }

        if let Some(data_bytes) = txn.get(data_key.as_bytes()).await? {
            monitor::inc_row_read();
            let row = crate::common::encoding::RowDecoder::decode(&data_bytes).map_err(|e| {
                FusionError::Execution(format!("Data deserialization error: {}", e))
            })?;
            self.row_cache.insert(data_key, row.clone());
            Ok(Some(row))
        } else {
            Ok(None)
        }
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
        let mut seen_row_ids = HashSet::new();
        let mut rows = Vec::new();

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

    fn index_candidate_cap(
        limit: Option<usize>,
        order_by: Option<&sqlparser::ast::OrderBy>,
    ) -> usize {
        let base = limit.unwrap_or(128).max(32);
        let multiplier = if order_by.is_some() { 16 } else { 8 };
        base.saturating_mul(multiplier).clamp(128, 4096)
    }

    fn should_use_index_plan(
        candidate_count: usize,
        limit: Option<usize>,
        order_by: Option<&sqlparser::ast::OrderBy>,
    ) -> bool {
        candidate_count > 0 && candidate_count <= Self::index_candidate_cap(limit, order_by)
    }

    fn filter_rows_with_expr(
        &self,
        rows: Vec<Vec<Value>>,
        schema: &TableSchema,
        expr: &Expr,
        params: &[Value],
    ) -> Result<Vec<Vec<Value>>> {
        let mut filtered = Vec::new();
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
        let mut join_predicates = Vec::new();

        if let Some(on_expr) = join_operator.and_then(Self::join_constraint_expr) {
            Self::split_conjunctive_predicates(on_expr, &mut join_predicates);
        }

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
                    bincode::deserialize(&schema_bytes).map_err(|e| {
                        FusionError::Execution(format!("Schema deserialization error: {}", e))
                    })
                })
                .transpose()?
        } else {
            None
        };

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
            if let Some(expr) = &join_expr {
                if let Some((left_key_indices, right_key_indices, residual_expr)) =
                    self.extract_join_keys(expr, &left_schema, &right_schema)
                {
                    let probe_schema = schema_for_probe.as_ref().unwrap_or(&right_schema_base);
                    if let Some((probe_left_idx, probe_right_idx)) = self
                        .choose_indexed_join_probe_pair(
                            &left_key_indices,
                            &right_key_indices,
                            probe_schema,
                        )
                    {
                        let mut distinct_probe_keys = HashSet::new();
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
                            let mut probed_rows = Vec::new();
                            let mut probe_cache: HashMap<Value, Vec<Vec<Value>>> = HashMap::new();

                            for left_row in &left_rows {
                                let probe_key = left_row[probe_left_idx].clone();
                                if !probe_cache.contains_key(&probe_key) {
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
                                    probe_cache.insert(probe_key.clone(), candidates);
                                }

                                let candidates = probe_cache.get(&probe_key).unwrap();
                                let mut matched = false;
                                for right_row in candidates {
                                    if !Self::row_keys_equal(
                                        left_row,
                                        &left_key_indices,
                                        right_row,
                                        &right_key_indices,
                                    ) {
                                        continue;
                                    }

                                    let mut joined_row = left_row.clone();
                                    joined_row.extend(right_row.clone());
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
                                    probed_rows.push(joined_row);
                                    if limit.is_some_and(|value| probed_rows.len() >= value) {
                                        break;
                                    }
                                }

                                if !matched && is_left_outer {
                                    let mut joined_row = left_row.clone();
                                    joined_row
                                        .extend(vec![Value::Null; right_schema.columns.len()]);
                                    probed_rows.push(joined_row);
                                }

                                if limit.is_some_and(|value| probed_rows.len() >= value) {
                                    break;
                                }
                            }

                            return self.project_join_rows(new_schema, probed_rows, projection);
                        }
                    }
                }
            }
        }

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
                                    let mut joined_row = left_row.clone();
                                    joined_row.extend((*right_row).clone());
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
                                let mut joined_row = left_row.clone();
                                joined_row.extend(vec![Value::Null; right_schema.columns.len()]);
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
                                    let mut joined_row = (*left_row).clone();
                                    joined_row.extend(right_row.clone());
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
            let row_width = left_schema.columns.len() + right_schema.columns.len();
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
                    let mut joined_row = left_row.clone();
                    joined_row.extend(vec![Value::Null; right_schema.columns.len()]);
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
        let mut pending_predicates = Vec::new();
        let join_column_refs = self.collect_join_column_references(from);
        if let Some(expr) = selection {
            Self::split_conjunctive_predicates(expr, &mut pending_predicates);
        }

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
                let mut filtered_rows = Vec::new();
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
        Box<dyn std::future::Future<Output = Result<Option<IndexScanPlan>>> + Send + 'a>,
    > {
        Box::pin(async move {
            match expr {
                Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right,
                } => {
                    if let Some((col_idx, storage_col_name, value_expr)) =
                        self.equality_schema_column_value_expr(left, right, schema)
                    {
                        if schema.columns[col_idx].is_indexed
                            && schema.columns[col_idx].index_type == IndexType::BTree
                        {
                            let val = self
                                .evaluate_value(value_expr, &[], schema, params)
                                .unwrap_or(Value::Null);
                            if let Some(val_str) = self.value_to_index_string(&val) {
                                let index_prefix = format!(
                                    "index:{}:{}:{}:",
                                    table_name, storage_col_name, val_str
                                );
                                let index_entries =
                                    txn.scan_prefix(index_prefix.as_bytes(), limit).await?;

                                let mut row_ids = HashSet::new();
                                for (k, _) in index_entries {
                                    if let Some(row_id) = Self::row_id_from_key(&k) {
                                        row_ids.insert(row_id.to_string());
                                    }
                                }
                                return Ok(Some(IndexScanPlan {
                                    row_ids,
                                    exact: true,
                                }));
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
                                            let index_entries = txn
                                                .scan_prefix(index_prefix.as_bytes(), None)
                                                .await?;

                                            let mut current_token_row_ids = HashSet::new();
                                            for (k, _) in index_entries {
                                                if let Some(row_id) = Self::row_id_from_key(&k) {
                                                    current_token_row_ids
                                                        .insert(row_id.to_string());
                                                }
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
                                                return Ok(Some(IndexScanPlan {
                                                    row_ids: HashSet::new(),
                                                    exact: false,
                                                }));
                                            }
                                        }
                                        if let Some(res) = &candidate_row_ids {
                                            monitor::add_fts_hits(res.len() as u64);
                                        }
                                        return Ok(candidate_row_ids.map(|row_ids| {
                                            IndexScanPlan {
                                                row_ids,
                                                exact: false,
                                            }
                                        }));
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

                    if let Some((col_idx, storage_col_name)) =
                        self.resolve_schema_column_name(col_expr, schema)
                    {
                        let col = &schema.columns[col_idx];
                        if col.is_indexed {
                            let mut all_row_ids = HashSet::new();
                            for item in list {
                                let val = self
                                    .evaluate_value(item, &[], schema, params)
                                    .unwrap_or(Value::Null);

                                if col.is_primary {
                                    let val_str = match &val {
                                        Value::Integer(i) => {
                                            Some(crate::common::encoding::encode_i64_comparable(*i))
                                        }
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
                                    let index_prefix = format!(
                                        "index:{}:{}:{}:",
                                        table_name, storage_col_name, val_str
                                    );
                                    let kv =
                                        txn.scan_prefix(index_prefix.as_bytes(), limit).await?;
                                    for (k, _) in kv {
                                        if let Some(row_id) = Self::row_id_from_key(&k) {
                                            all_row_ids.insert(row_id.to_string());
                                        }
                                    }
                                }
                            }
                            return Ok(Some(IndexScanPlan {
                                row_ids: all_row_ids,
                                exact: true,
                            }));
                        }
                    }
                }
                Expr::Like {
                    expr,
                    pattern,
                    negated,
                    ..
                } => {
                    if *negated {
                        return Ok(None);
                    }
                    // Check if it's a prefix scan: LIKE 'prefix%'
                    if let (Some((col_idx, storage_col_name)), Expr::Value(val_with_span)) = (
                        self.resolve_schema_column_name(expr, schema),
                        pattern.as_ref(),
                    ) {
                        if let SqlValue::SingleQuotedString(pattern_str) = &val_with_span.value {
                            if let Some(prefix) = Self::like_fixed_prefix(pattern_str) {
                                let col = &schema.columns[col_idx];
                                if col.is_indexed {
                                    let mut all_row_ids = HashSet::new();

                                    if col.is_primary {
                                        let key_prefix = format!("data:{}:{}", table_name, prefix);
                                        let kv =
                                            txn.scan_prefix(key_prefix.as_bytes(), limit).await?;
                                        for (k, _) in kv {
                                            if let Some(row_id) = Self::row_id_from_key(&k) {
                                                all_row_ids.insert(row_id.to_string());
                                            }
                                        }
                                    } else {
                                        let index_prefix = format!(
                                            "index:{}:{}:{}",
                                            table_name, storage_col_name, prefix
                                        );
                                        let kv =
                                            txn.scan_prefix(index_prefix.as_bytes(), limit).await?;
                                        for (k, _) in kv {
                                            if let Some(row_id) = Self::row_id_from_key(&k) {
                                                all_row_ids.insert(row_id.to_string());
                                            }
                                        }
                                    }

                                    if !all_row_ids.is_empty() {
                                        return Ok(Some(IndexScanPlan {
                                            row_ids: all_row_ids,
                                            exact: true,
                                        }));
                                    }
                                }
                            }

                            // Trigram Index Fallback for wildcard LIKE
                            let col = &schema.columns[col_idx];
                            if col.is_indexed {
                                if let Some(ftxn) =
                                    txn.as_any()
                                        .downcast_ref::<crate::storage::fusion::FusionTransaction>()
                                {
                                    let storage = &ftxn.storage;
                                    let idx_guard = storage.trigram_index.read().unwrap();
                                    if let Some(ids) =
                                        idx_guard.search(table_name, &storage_col_name, pattern_str)
                                    {
                                        let row_keys =
                                            idx_guard.map_ids_to_row_keys(table_name, &ids);
                                        if !row_keys.is_empty() {
                                            let mut set = HashSet::new();
                                            for s in row_keys {
                                                set.insert(s);
                                            }
                                            return Ok(Some(IndexScanPlan {
                                                row_ids: set,
                                                exact: false,
                                            }));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Expr::Between {
                    expr,
                    low,
                    high,
                    negated,
                } => {
                    if *negated {
                        return Ok(None);
                    }
                    if let Some((col_idx, _storage_col_name)) =
                        self.resolve_schema_column_name(expr, schema)
                    {
                        let col = &schema.columns[col_idx];
                        let low_val = self
                            .evaluate_value(low, &[], schema, params)
                            .unwrap_or(Value::Null);
                        let high_val = self
                            .evaluate_value(high, &[], schema, params)
                            .unwrap_or(Value::Null);

                        if col.is_primary {
                            let low_encoded = match low_val {
                                Value::Integer(i) => {
                                    Some(crate::common::encoding::encode_i64_comparable(i))
                                }
                                Value::String(s) => Some(s),
                                _ => None,
                            };
                            let high_encoded = match high_val {
                                Value::Integer(i) => {
                                    Some(crate::common::encoding::encode_i64_comparable(i))
                                }
                                Value::String(s) => Some(s),
                                _ => None,
                            };

                            if let (Some(low_key), Some(high_key)) = (low_encoded, high_encoded) {
                                let start = format!("data:{}:{}", table_name, low_key);
                                let end = format!("data:{}:{}\u{0}", table_name, high_key);
                                let kv = txn
                                    .scan_range(start.as_bytes(), end.as_bytes(), limit)
                                    .await?;
                                let mut row_ids = HashSet::new();
                                for (k, _) in kv {
                                    if let Some(row_id) = Self::row_id_from_key(&k) {
                                        row_ids.insert(row_id.to_string());
                                    }
                                }
                                return Ok(Some(IndexScanPlan {
                                    row_ids,
                                    exact: true,
                                }));
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
                            return Ok(Some(IndexScanPlan {
                                row_ids: l.row_ids.intersection(&r.row_ids).cloned().collect(),
                                exact: l.exact && r.exact,
                            }));
                        }
                        (Some(s), None) | (None, Some(s)) => {
                            return Ok(Some(IndexScanPlan {
                                row_ids: s.row_ids,
                                exact: false,
                            }))
                        }
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
                        return Ok(Some(IndexScanPlan {
                            row_ids: l.row_ids.union(&r.row_ids).cloned().collect(),
                            exact: l.exact && r.exact,
                        }));
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
