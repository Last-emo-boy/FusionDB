use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, OrderByKind,
    Value as SqlValue,
};
use std::collections::{HashMap, HashSet};

use super::Executor;

pub(super) const SMALL_INDEX_FETCH_THRESHOLD: usize = 64;

pub(crate) struct IndexScanPlan {
    pub(crate) row_ids: HashSet<String>,
    pub(crate) ordered_row_ids: Option<Vec<String>>,
    pub(crate) exact: bool,
    pub(crate) ordered_topk_counted: bool,
    pub(crate) covered: Option<CoveredIndexRows>,
}

pub(crate) struct CoveredIndexRows {
    pub(crate) column_indices: Vec<usize>,
    pub(crate) rows: HashMap<String, Vec<Value>>,
}

struct SecondaryIndexRangeBound {
    value_key: String,
    inclusive: bool,
}

impl Executor {
    #[cfg(test)]
    fn data_key_for_row_id(table_name: &str, row_id: &str) -> String {
        let mut key = String::with_capacity("data:".len() + table_name.len() + 1 + row_id.len());
        key.push_str("data:");
        key.push_str(table_name);
        key.push(':');
        key.push_str(row_id);
        key
    }

    #[cfg(test)]
    fn data_key_upper_bound_for_row_id(table_name: &str, row_id: &str) -> String {
        let mut key =
            String::with_capacity("data:".len() + table_name.len() + 1 + row_id.len() + 1);
        key.push_str("data:");
        key.push_str(table_name);
        key.push(':');
        key.push_str(row_id);
        key.push('\0');
        key
    }

    #[cfg(test)]
    fn secondary_index_prefix_for_value(
        table_name: &str,
        column_name: &str,
        value: &str,
    ) -> String {
        let mut prefix = String::with_capacity(
            "index:".len() + table_name.len() + 1 + column_name.len() + 1 + value.len() + 1,
        );
        prefix.push_str("index:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix.push_str(column_name);
        prefix.push(':');
        prefix.push_str(value);
        prefix.push(':');
        prefix
    }

    #[cfg(test)]
    fn secondary_index_prefix_for_value_start(
        table_name: &str,
        column_name: &str,
        value_prefix: &str,
    ) -> String {
        let mut prefix = String::with_capacity(
            "index:".len() + table_name.len() + 1 + column_name.len() + 1 + value_prefix.len(),
        );
        prefix.push_str("index:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix.push_str(column_name);
        prefix.push(':');
        prefix.push_str(value_prefix);
        prefix
    }

    fn order_by_primary_key_direction(
        &self,
        order_by: Option<&sqlparser::ast::OrderBy>,
        schema: &TableSchema,
    ) -> Option<bool> {
        let order_by = order_by?;
        let OrderByKind::Expressions(exprs) = &order_by.kind else {
            return None;
        };
        let [order_expr] = exprs.as_slice() else {
            return None;
        };
        let order_col = Self::order_limit_column_name(&order_expr.expr)?;
        let order_idx = self.resolve_column_index(&order_col, schema).ok()?;

        schema
            .columns
            .get(order_idx)
            .is_some_and(|column| column.is_primary)
            .then_some(order_expr.options.asc.unwrap_or(true))
    }

    pub(crate) fn equality_schema_column_value_expr<'a>(
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

    pub(super) fn equality_primary_key_value_expr<'a>(
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

    pub(crate) fn primary_key_range_value_expr<'a>(
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

    fn schema_column_range_value_expr<'a>(
        &self,
        left: &'a Expr,
        op: &BinaryOperator,
        right: &'a Expr,
        schema: &TableSchema,
    ) -> Option<(usize, String, BinaryOperator, &'a Expr)> {
        let normalized_op = match op {
            BinaryOperator::Gt => BinaryOperator::Gt,
            BinaryOperator::GtEq => BinaryOperator::GtEq,
            BinaryOperator::Lt => BinaryOperator::Lt,
            BinaryOperator::LtEq => BinaryOperator::LtEq,
            _ => return None,
        };

        if let Some((idx, name)) = self.resolve_schema_column_name(left, schema) {
            if self.expr_has_column_reference(right) {
                None
            } else {
                Some((idx, name, normalized_op, right))
            }
        } else {
            self.resolve_schema_column_name(right, schema)
                .and_then(|(idx, name)| {
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
                    Some((idx, name, flipped_op, left))
                })
        }
    }

    fn secondary_index_range_value_key(&self, value: &Value) -> Option<String> {
        match value {
            Value::Integer(_)
            | Value::Boolean(_)
            | Value::Date(_)
            | Value::Timestamp(_)
            | Value::Interval(_) => self.value_to_index_string(value),
            _ => None,
        }
    }

    fn secondary_index_order_direction(
        &self,
        order_by: Option<&sqlparser::ast::OrderBy>,
        schema: &TableSchema,
        column_index: usize,
    ) -> Option<bool> {
        let order_by = order_by?;
        let OrderByKind::Expressions(exprs) = &order_by.kind else {
            return None;
        };
        let [order_expr] = exprs.as_slice() else {
            return None;
        };
        let order_col = Self::order_limit_column_name(&order_expr.expr)?;
        let order_idx = self.resolve_column_index(&order_col, schema).ok()?;
        (order_idx == column_index).then_some(order_expr.options.asc.unwrap_or(true))
    }

    pub(crate) fn secondary_index_order_type_supported(column_type: &str) -> bool {
        crate::execution::Executor::is_integer_type_name(column_type)
            || Self::is_boolean_type_name(column_type)
            || Self::is_date_type_name(column_type)
            || Self::is_timestamp_type_name(column_type)
            || Self::is_interval_type_name(column_type)
    }

    fn secondary_index_range_start(
        prefix: &str,
        lower: Option<&SecondaryIndexRangeBound>,
    ) -> Vec<u8> {
        let mut key = prefix.as_bytes().to_vec();
        if let Some(bound) = lower {
            key.extend_from_slice(bound.value_key.as_bytes());
            key.push(b':');
            if !bound.inclusive {
                key.push(0xFF);
            }
        }
        key
    }

    fn secondary_index_range_end(
        prefix: &str,
        upper: Option<&SecondaryIndexRangeBound>,
    ) -> Vec<u8> {
        let mut key = prefix.as_bytes().to_vec();
        if let Some(bound) = upper {
            key.extend_from_slice(bound.value_key.as_bytes());
            key.push(b':');
            if bound.inclusive {
                key.push(0xFF);
            }
        } else {
            key.push(0xFF);
        }
        key
    }

    async fn scan_secondary_index_range(
        &self,
        table_name: &str,
        column_name: &str,
        schema: &TableSchema,
        column_index: usize,
        pk_index: Option<usize>,
        txn: &mut dyn Transaction,
        lower: Option<SecondaryIndexRangeBound>,
        upper: Option<SecondaryIndexRangeBound>,
        limit: Option<usize>,
        order_direction: Option<bool>,
        include_indices: &[usize],
    ) -> Result<IndexScanPlan> {
        let ordered_scan_limit = if order_direction.is_some() {
            limit
        } else {
            None
        };
        let mut row_ids = HashSet::new();
        let mut ordered_row_ids = order_direction.map(|_| Vec::new());
        let mut covered_rows = HashMap::new();
        let mut include_payloads_complete = true;
        let column_type = schema
            .columns
            .get(column_index)
            .map(|column| column.data_type.as_str())
            .unwrap_or_default();
        let mut entry_visit_count = 0usize;

        for prefix in self.routed_index_prefixes_for_column(table_name, column_name) {
            let remaining = ordered_scan_limit.map(|limit| limit.saturating_sub(entry_visit_count));
            if remaining == Some(0) {
                break;
            }

            let start = Self::secondary_index_range_start(&prefix, lower.as_ref());
            let end = Self::secondary_index_range_end(&prefix, upper.as_ref());
            if start >= end {
                continue;
            }

            let mut visitor = |key: &[u8], payload: &[u8]| {
                if let Some(row_id) = Self::row_id_from_key(key) {
                    let row_id = row_id.to_string();
                    if row_ids.insert(row_id.clone()) {
                        if let Some(ordered) = &mut ordered_row_ids {
                            ordered.push(row_id.clone());
                        }
                        if let Some(value_key) =
                            Self::secondary_index_value_key_from_prefixed_key(key, &prefix)
                        {
                            if let Some(value) =
                                Self::secondary_index_value_from_key(value_key, column_type)
                            {
                                let include_values =
                                    Self::secondary_index_payload_values(payload, include_indices);
                                if !include_indices.is_empty() && include_values.is_none() {
                                    include_payloads_complete = false;
                                }
                                covered_rows.insert(
                                    row_id.clone(),
                                    Self::secondary_index_covered_row(
                                        schema,
                                        pk_index,
                                        &row_id,
                                        column_index,
                                        value,
                                        include_indices,
                                        include_values,
                                    ),
                                );
                            }
                        }
                    }
                }
                true
            };
            let visited = if order_direction == Some(false) {
                txn.scan_range_reverse_for_each(&start, &end, remaining, &mut visitor)
                    .await?
            } else {
                txn.scan_range_for_each(&start, &end, remaining, &mut visitor)
                    .await?
            };
            entry_visit_count += visited;
            if ordered_scan_limit.is_some_and(|limit| entry_visit_count >= limit) {
                break;
            }
        }
        let ordered_topk_counted = order_direction.is_some() && ordered_scan_limit.is_some();
        if let (Some(order_asc), true) = (order_direction, ordered_topk_counted) {
            crate::monitor::inc_index_ordered_topk_scan();
            if !order_asc {
                crate::monitor::inc_index_ordered_topk_reverse_scan();
            }
            crate::monitor::add_index_ordered_topk_entry_visits(entry_visit_count as u64);
        }

        if let (Some(ordered), Some(_)) = (&mut ordered_row_ids, order_direction) {
            if let Some(limit) = limit {
                ordered.truncate(limit);
            }
        }

        Ok(IndexScanPlan {
            row_ids,
            ordered_row_ids,
            exact: true,
            ordered_topk_counted,
            covered: (!covered_rows.is_empty()).then(|| CoveredIndexRows {
                column_indices: Self::secondary_index_covered_column_indices(
                    pk_index,
                    column_index,
                    if include_payloads_complete {
                        include_indices
                    } else {
                        &[]
                    },
                ),
                rows: covered_rows,
            }),
        })
    }

    pub(super) async fn try_ordered_secondary_index_scan(
        &self,
        table_name: &str,
        schema: &TableSchema,
        txn: &mut dyn Transaction,
        order_by: Option<&sqlparser::ast::OrderBy>,
        ordered_limit: Option<usize>,
    ) -> Result<Option<IndexScanPlan>> {
        let Some(limit) = ordered_limit else {
            return Ok(None);
        };
        let Some(order_by) = order_by else {
            return Ok(None);
        };
        if self.shard_router.is_some()
            || !Self::legacy_delimited_index_row_ids_are_unambiguous(schema)
        {
            return Ok(None);
        }

        let OrderByKind::Expressions(exprs) = &order_by.kind else {
            return Ok(None);
        };
        let [order_expr] = exprs.as_slice() else {
            return Ok(None);
        };
        let order_asc = order_expr.options.asc.unwrap_or(true);
        if !order_asc && !txn.supports_bounded_scan_range_reverse() {
            return Ok(None);
        }
        let Some(order_col) = Self::order_limit_column_name(&order_expr.expr) else {
            return Ok(None);
        };
        let Ok(col_idx) = self.resolve_column_index(&order_col, schema) else {
            return Ok(None);
        };
        let Some(col) = schema.columns.get(col_idx) else {
            return Ok(None);
        };
        if col.is_primary
            || !col.is_indexed
            || col.index_type != IndexType::BTree
            || col.is_nullable
            || !Self::secondary_index_order_type_supported(&col.data_type)
        {
            return Ok(None);
        }

        if limit == 0 {
            return Ok(Some(IndexScanPlan {
                row_ids: HashSet::new(),
                ordered_row_ids: Some(Vec::new()),
                exact: true,
                ordered_topk_counted: false,
                covered: None,
            }));
        }

        let pk_index = schema.get_primary_key_index();
        let single_column_index_includes = self
            .load_single_column_index_includes_for_table(table_name, schema, txn)
            .await?;
        let include_indices = single_column_index_includes
            .get(&col_idx)
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        self.scan_secondary_index_range(
            table_name,
            &col.name,
            schema,
            col_idx,
            pk_index,
            txn,
            None,
            None,
            Some(limit),
            Some(order_asc),
            include_indices,
        )
        .await
        .map(Some)
    }

    pub(crate) fn expr_has_column_reference(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => true,
            Expr::BinaryOp { left, right, .. } => {
                self.expr_has_column_reference(left) || self.expr_has_column_reference(right)
            }
            Expr::Nested(expr)
            | Expr::UnaryOp { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::Extract { expr, .. } => self.expr_has_column_reference(expr),
            Expr::Array(array) => array
                .elem
                .iter()
                .any(|expr| self.expr_has_column_reference(expr)),
            Expr::CompoundFieldAccess { root, access_chain } => {
                self.expr_has_column_reference(root)
                    || access_chain.iter().any(|access| {
                        matches!(
                            access,
                            sqlparser::ast::AccessExpr::Subscript(
                                sqlparser::ast::Subscript::Index { index }
                            ) if self.expr_has_column_reference(index)
                        )
                    })
            }
            Expr::Function(func) => {
                if let FunctionArguments::List(args) = &func.args {
                    args.args.iter().any(|arg| {
                        matches!(
                            arg,
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                                if self.expr_has_column_reference(expr)
                        )
                    })
                } else {
                    false
                }
            }
            Expr::InList { expr, list, .. } => {
                self.expr_has_column_reference(expr)
                    || list.iter().any(|expr| self.expr_has_column_reference(expr))
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                self.expr_has_column_reference(expr)
                    || substring_from
                        .as_ref()
                        .is_some_and(|expr| self.expr_has_column_reference(expr))
                    || substring_for
                        .as_ref()
                        .is_some_and(|expr| self.expr_has_column_reference(expr))
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.expr_has_column_reference(expr)
                    || self.expr_has_column_reference(low)
                    || self.expr_has_column_reference(high)
            }
            Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
                self.expr_has_column_reference(left) || self.expr_has_column_reference(right)
            }
            Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::InSubquery { expr, .. }
            | Expr::Like { expr, .. }
            | Expr::ILike { expr, .. } => self.expr_has_column_reference(expr),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                operand
                    .as_ref()
                    .is_some_and(|expr| self.expr_has_column_reference(expr))
                    || conditions.iter().any(|cw| {
                        self.expr_has_column_reference(&cw.condition)
                            || self.expr_has_column_reference(&cw.result)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|expr| self.expr_has_column_reference(expr))
            }
            _ => false,
        }
    }

    pub(crate) fn value_to_primary_row_id(value: &Value) -> Option<String> {
        match value {
            Value::Integer(i) => Some(crate::common::encoding::encode_i64_comparable(*i)),
            Value::String(s) => Some(s.clone()),
            Value::Date(days) => Some(crate::common::encoding::encode_i64_comparable(*days as i64)),
            Value::Timestamp(micros) => {
                Some(crate::common::encoding::encode_i64_comparable(*micros))
            }
            _ => None,
        }
    }

    pub(crate) fn row_id_from_key(key: &[u8]) -> Option<&str> {
        std::str::from_utf8(key)
            .ok()?
            .rsplit(':')
            .next()
            .filter(|row_id| !row_id.is_empty())
    }

    fn secondary_index_value_key_from_prefixed_key<'a>(
        key: &'a [u8],
        prefix: &str,
    ) -> Option<&'a str> {
        let key = std::str::from_utf8(key).ok()?;
        let suffix = key.strip_prefix(prefix)?;
        let (value_key, row_id) = suffix.rsplit_once(':')?;
        if value_key.is_empty() || row_id.is_empty() {
            return None;
        }
        Some(value_key)
    }

    pub(crate) fn secondary_index_value_from_key(
        value_key: &str,
        column_type: &str,
    ) -> Option<Value> {
        if crate::execution::Executor::is_integer_type_name(column_type) {
            crate::common::encoding::decode_i64_comparable(value_key).map(Value::Integer)
        } else if Self::is_boolean_type_name(column_type) {
            if value_key.eq_ignore_ascii_case("false") {
                Some(Value::Boolean(false))
            } else if value_key.eq_ignore_ascii_case("true") {
                Some(Value::Boolean(true))
            } else {
                None
            }
        } else if Self::is_date_type_name(column_type) {
            crate::common::encoding::decode_i64_comparable(value_key)
                .map(|days| Value::Date(days as i32))
        } else if Self::is_timestamp_type_name(column_type) {
            crate::common::encoding::decode_i64_comparable(value_key).map(Value::Timestamp)
        } else if Self::is_interval_type_name(column_type) {
            crate::common::encoding::decode_i64_comparable(value_key).map(Value::Interval)
        } else {
            None
        }
    }

    fn secondary_index_covered_column_indices(
        pk_index: Option<usize>,
        column_index: usize,
        include_indices: &[usize],
    ) -> Vec<usize> {
        let mut indices = Vec::with_capacity(2 + include_indices.len());
        if let Some(pk_idx) = pk_index {
            indices.push(pk_idx);
        }
        if !indices.contains(&column_index) {
            indices.push(column_index);
        }
        for &include_idx in include_indices {
            if !indices.contains(&include_idx) {
                indices.push(include_idx);
            }
        }
        indices
    }

    fn secondary_index_covered_row(
        schema: &TableSchema,
        pk_index: Option<usize>,
        row_id: &str,
        column_index: usize,
        column_value: Value,
        include_indices: &[usize],
        include_values: Option<Vec<Value>>,
    ) -> Vec<Value> {
        let mut row = Self::primary_key_row_from_id(schema, pk_index, row_id);
        if let Some(value) = row.get_mut(column_index) {
            *value = column_value;
        }
        if let Some(include_values) = include_values {
            for (&include_idx, include_value) in include_indices.iter().zip(include_values) {
                if let Some(value) = row.get_mut(include_idx) {
                    *value = include_value;
                }
            }
        }
        row
    }

    pub(crate) fn primary_key_row_from_id(
        schema: &TableSchema,
        pk_index: Option<usize>,
        row_id: &str,
    ) -> Vec<Value> {
        let pk_type = pk_index
            .and_then(|pk_idx| schema.columns.get(pk_idx))
            .map(|column| column.data_type.as_str());
        Self::primary_key_row_from_parts(schema.columns.len(), pk_index, pk_type, row_id)
    }

    fn primary_key_type_starts_with_ascii_case_insensitive(value: &str, prefix: &str) -> bool {
        value
            .as_bytes()
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
    }

    pub(super) fn primary_key_row_from_parts(
        schema_width: usize,
        pk_index: Option<usize>,
        pk_type: Option<&str>,
        row_id: &str,
    ) -> Vec<Value> {
        let mut row = vec![Value::Null; schema_width];
        let (Some(pk_idx), Some(pk_type)) = (pk_index, pk_type) else {
            return row;
        };
        if pk_idx >= schema_width {
            return row;
        }

        row[pk_idx] = if crate::execution::Executor::is_integer_type_name(pk_type) {
            crate::common::encoding::decode_i64_comparable(row_id)
                .map(Value::Integer)
                .unwrap_or_else(|| Value::String(row_id.to_string()))
        } else if pk_type.eq_ignore_ascii_case("DATE") {
            crate::common::encoding::decode_i64_comparable(row_id)
                .map(|days| Value::Date(days as i32))
                .unwrap_or_else(|| Value::String(row_id.to_string()))
        } else if Self::primary_key_type_starts_with_ascii_case_insensitive(pk_type, "TIMESTAMP")
            || pk_type.eq_ignore_ascii_case("DATETIME")
        {
            crate::common::encoding::decode_i64_comparable(row_id)
                .map(Value::Timestamp)
                .unwrap_or_else(|| Value::String(row_id.to_string()))
        } else {
            Value::String(row_id.to_string())
        };
        row
    }

    pub(super) fn decode_row_for_projection(
        data: &[u8],
        projection_indices: Option<&[usize]>,
    ) -> bincode::Result<Vec<Value>> {
        match projection_indices {
            Some(indices) if indices.is_empty() => Ok(Vec::new()),
            Some(indices) => crate::common::encoding::RowDecoder::decode_partial(data, indices),
            None => crate::common::encoding::RowDecoder::decode(data),
        }
    }

    pub(super) async fn fetch_full_row_by_id(
        &self,
        table_name: &str,
        row_id: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Option<Vec<Value>>> {
        let data_key = self.routed_data_key_for_row_id(table_name, row_id);
        if let Some(data_bytes) = txn.get(data_key.as_bytes()).await? {
            monitor::inc_row_read();
            if let Some(row) = self.row_cache_lookup(&data_key, &data_bytes) {
                return Ok(Some(row));
            }
            let row = crate::common::encoding::RowDecoder::decode(&data_bytes).map_err(|e| {
                FusionError::Execution(format!("Data deserialization error: {}", e))
            })?;
            self.row_cache_store(data_key, &data_bytes, &row);
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    pub(super) fn index_candidate_cap(
        limit: Option<usize>,
        order_by: Option<&sqlparser::ast::OrderBy>,
    ) -> usize {
        let base = limit.unwrap_or(128).max(32);
        let multiplier = if order_by.is_some() { 16 } else { 8 };
        base.saturating_mul(multiplier).clamp(128, 4096)
    }

    pub(super) fn should_use_index_plan(candidate_count: usize, candidate_cap: usize) -> bool {
        candidate_count > 0 && candidate_count <= candidate_cap
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn try_index_scan<'a>(
        &'a self,
        expr: &'a Expr,
        table_name: &'a str,
        schema: &'a TableSchema,
        txn: &'a mut dyn Transaction,
        params: &'a [Value],
        limit: Option<usize>,
        order_by: Option<&'a sqlparser::ast::OrderBy>,
        ordered_limit: Option<usize>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<IndexScanPlan>>> + Send + 'a>,
    > {
        Box::pin(async move {
            let delimited_row_ids_are_unambiguous =
                Self::legacy_delimited_index_row_ids_are_unambiguous(schema);
            if delimited_row_ids_are_unambiguous {
                if let Some(plan) = self
                    .try_composite_index_scan(
                        expr,
                        table_name,
                        schema,
                        txn,
                        params,
                        limit,
                        order_by,
                        ordered_limit,
                    )
                    .await?
                {
                    return Ok(Some(plan));
                }
            }

            let pk_index = schema.get_primary_key_index();
            let mut single_column_index_includes = None;
            match expr {
                Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right,
                } => {
                    if let Some((col_idx, storage_col_name, value_expr)) =
                        self.equality_schema_column_value_expr(left, right, schema)
                    {
                        if delimited_row_ids_are_unambiguous
                            && schema.columns[col_idx].is_indexed
                            && schema.columns[col_idx].index_type == IndexType::BTree
                        {
                            let val = self
                                .evaluate_value(value_expr, &[], schema, params)
                                .unwrap_or(Value::Null);
                            let val = Self::coerce_value_to_column_type(
                                val,
                                &schema.columns[col_idx].data_type,
                            )
                            .unwrap_or(Value::Null);
                            if let Some(val_str) = self.value_to_index_string(&val) {
                                let primary_order_asc = if self.shard_router.is_none() {
                                    self.order_by_primary_key_direction(order_by, schema)
                                } else {
                                    None
                                };
                                let scan_limit = if primary_order_asc == Some(true) {
                                    ordered_limit.or(limit)
                                } else if primary_order_asc == Some(false) {
                                    None
                                } else {
                                    limit
                                };
                                let index_entries = self
                                    .scan_routed_prefixes(
                                        self.routed_index_prefixes_for_value(
                                            table_name,
                                            &storage_col_name,
                                            &val_str,
                                        ),
                                        txn,
                                        scan_limit,
                                    )
                                    .await?;

                                let mut row_ids = HashSet::with_capacity(index_entries.len());
                                let mut ordered_row_ids = primary_order_asc
                                    .map(|_| Vec::with_capacity(index_entries.len()));
                                let mut covered_rows = if schema.columns[col_idx].is_primary {
                                    None
                                } else {
                                    Some(HashMap::with_capacity(index_entries.len()))
                                };
                                if covered_rows.is_some() && single_column_index_includes.is_none()
                                {
                                    single_column_index_includes = Some(
                                        self.load_single_column_index_includes_for_table(
                                            table_name, schema, txn,
                                        )
                                        .await?,
                                    );
                                }
                                let include_indices = single_column_index_includes
                                    .as_ref()
                                    .and_then(|includes| includes.get(&col_idx))
                                    .map(Vec::as_slice)
                                    .unwrap_or(&[]);
                                let mut include_payloads_complete = true;
                                for (k, payload) in index_entries {
                                    if let Some(row_id) = Self::row_id_from_key(&k) {
                                        let row_id = row_id.to_string();
                                        if row_ids.insert(row_id.clone()) {
                                            if let Some(ordered) = &mut ordered_row_ids {
                                                ordered.push(row_id.clone());
                                            }
                                            if let Some(rows) = &mut covered_rows {
                                                let include_values =
                                                    Self::secondary_index_payload_values(
                                                        &payload,
                                                        include_indices,
                                                    );
                                                if !include_indices.is_empty()
                                                    && include_values.is_none()
                                                {
                                                    include_payloads_complete = false;
                                                }
                                                rows.insert(
                                                    row_id.clone(),
                                                    Self::secondary_index_covered_row(
                                                        schema,
                                                        pk_index,
                                                        &row_id,
                                                        col_idx,
                                                        val.clone(),
                                                        include_indices,
                                                        include_values,
                                                    ),
                                                );
                                            }
                                        }
                                    }
                                }
                                if let (Some(ordered), Some(asc)) =
                                    (&mut ordered_row_ids, primary_order_asc)
                                {
                                    if !asc {
                                        ordered.reverse();
                                    }
                                    if let Some(limit) = ordered_limit.or(limit) {
                                        ordered.truncate(limit);
                                    }
                                }
                                return Ok(Some(IndexScanPlan {
                                    row_ids,
                                    ordered_row_ids,
                                    exact: true,
                                    ordered_topk_counted: false,
                                    covered: covered_rows.and_then(|rows| {
                                        (!rows.is_empty()).then(|| CoveredIndexRows {
                                            column_indices:
                                                Self::secondary_index_covered_column_indices(
                                                    pk_index,
                                                    col_idx,
                                                    if include_payloads_complete {
                                                        include_indices
                                                    } else {
                                                        &[]
                                                    },
                                                ),
                                            rows,
                                        })
                                    }),
                                }));
                            }
                        }
                    }
                }
                Expr::BinaryOp { left, op, right }
                    if matches!(
                        op,
                        BinaryOperator::Gt
                            | BinaryOperator::GtEq
                            | BinaryOperator::Lt
                            | BinaryOperator::LtEq
                    ) =>
                {
                    if let Some((col_idx, storage_col_name, range_op, value_expr)) =
                        self.schema_column_range_value_expr(left, op, right, schema)
                    {
                        let col = &schema.columns[col_idx];
                        if delimited_row_ids_are_unambiguous
                            && col.is_indexed
                            && col.index_type == IndexType::BTree
                            && !col.is_primary
                        {
                            let val = self
                                .evaluate_value(value_expr, &[], schema, params)
                                .unwrap_or(Value::Null);
                            let val = Self::coerce_value_to_column_type(val, &col.data_type)
                                .unwrap_or(Value::Null);
                            if let Some(value_key) = self.secondary_index_range_value_key(&val) {
                                let bound = SecondaryIndexRangeBound {
                                    value_key,
                                    inclusive: matches!(
                                        range_op,
                                        BinaryOperator::GtEq | BinaryOperator::LtEq
                                    ),
                                };
                                let (lower, upper) = match range_op {
                                    BinaryOperator::Gt | BinaryOperator::GtEq => {
                                        (Some(bound), None)
                                    }
                                    BinaryOperator::Lt | BinaryOperator::LtEq => {
                                        (None, Some(bound))
                                    }
                                    _ => unreachable!(),
                                };
                                let order_direction = if self.shard_router.is_none() {
                                    self.secondary_index_order_direction(order_by, schema, col_idx)
                                } else {
                                    None
                                };
                                let order_direction = match order_direction {
                                    Some(false) if !txn.supports_bounded_scan_range_reverse() => {
                                        None
                                    }
                                    direction => direction,
                                };
                                let scan_limit = if order_direction.is_some() {
                                    ordered_limit.or(limit)
                                } else {
                                    limit
                                };
                                if single_column_index_includes.is_none() {
                                    single_column_index_includes = Some(
                                        self.load_single_column_index_includes_for_table(
                                            table_name, schema, txn,
                                        )
                                        .await?,
                                    );
                                }
                                let include_indices = single_column_index_includes
                                    .as_ref()
                                    .and_then(|includes| includes.get(&col_idx))
                                    .map(Vec::as_slice)
                                    .unwrap_or(&[]);
                                return Ok(Some(
                                    self.scan_secondary_index_range(
                                        table_name,
                                        &storage_col_name,
                                        schema,
                                        col_idx,
                                        pk_index,
                                        txn,
                                        lower,
                                        upper,
                                        scan_limit,
                                        order_direction,
                                        include_indices,
                                    )
                                    .await?,
                                ));
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
                            if delimited_row_ids_are_unambiguous
                                && schema.columns[col_idx].is_indexed
                                && schema.columns[col_idx].index_type == IndexType::FTS
                            {
                                monitor::inc_fts_search();
                                let match_val = if let SqlValue::SingleQuotedString(s) = match_value
                                {
                                    Value::String(s.clone())
                                } else if let SqlValue::Placeholder(p) = match_value {
                                    let idx = Self::placeholder_index(p);
                                    if idx > 0 && idx <= params.len() {
                                        params[idx - 1].clone()
                                    } else {
                                        Value::Null
                                    }
                                } else {
                                    Value::Null
                                };

                                if let Value::String(query_text) = match_val {
                                    let tokens = Self::tokenize_unique(&query_text);
                                    if !tokens.is_empty() {
                                        let mut candidate_row_ids: Option<HashSet<String>> = None;

                                        for token in tokens {
                                            let index_entries = self
                                                .scan_routed_prefixes(
                                                    self.routed_fts_prefixes_for_token(
                                                        table_name, &col_name, &token,
                                                    ),
                                                    txn,
                                                    None,
                                                )
                                                .await?;

                                            let mut current_token_row_ids =
                                                HashSet::with_capacity(index_entries.len());
                                            for (k, _) in index_entries {
                                                if let Some(row_id) = Self::row_id_from_key(&k) {
                                                    current_token_row_ids
                                                        .insert(row_id.to_string());
                                                }
                                            }

                                            if let Some(mut candidates) = candidate_row_ids {
                                                candidates.retain(|row_id| {
                                                    current_token_row_ids.contains(row_id)
                                                });
                                                candidate_row_ids = Some(candidates);
                                            } else {
                                                candidate_row_ids = Some(current_token_row_ids);
                                            }

                                            if candidate_row_ids.as_ref().unwrap().is_empty() {
                                                return Ok(Some(IndexScanPlan {
                                                    row_ids: HashSet::new(),
                                                    ordered_row_ids: Some(Vec::new()),
                                                    exact: true,
                                                    ordered_topk_counted: false,
                                                    covered: None,
                                                }));
                                            }
                                        }
                                        if let Some(res) = &candidate_row_ids {
                                            monitor::add_fts_hits(res.len() as u64);
                                        }
                                        return Ok(candidate_row_ids.map(|row_ids| {
                                            IndexScanPlan {
                                                row_ids,
                                                ordered_row_ids: None,
                                                exact: true,
                                                ordered_topk_counted: false,
                                                covered: None,
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
                        if col.is_indexed && (col.is_primary || delimited_row_ids_are_unambiguous) {
                            let mut all_row_ids = HashSet::with_capacity(list.len());
                            let mut covered_rows = if col.is_primary {
                                None
                            } else {
                                Some(HashMap::new())
                            };
                            if covered_rows.is_some() && single_column_index_includes.is_none() {
                                single_column_index_includes = Some(
                                    self.load_single_column_index_includes_for_table(
                                        table_name, schema, txn,
                                    )
                                    .await?,
                                );
                            }
                            let include_indices = single_column_index_includes
                                .as_ref()
                                .and_then(|includes| includes.get(&col_idx))
                                .map(Vec::as_slice)
                                .unwrap_or(&[]);
                            let mut include_payloads_complete = true;
                            for item in list {
                                let val = self
                                    .evaluate_value(item, &[], schema, params)
                                    .unwrap_or(Value::Null);
                                let val = Self::coerce_value_to_column_type(val, &col.data_type)
                                    .unwrap_or(Value::Null);

                                if col.is_primary {
                                    let val_str = Self::value_to_primary_row_id(&val);
                                    if let Some(s) = val_str {
                                        let key = self.routed_data_key_for_row_id(table_name, &s);
                                        if txn.get(key.as_bytes()).await?.is_some() {
                                            all_row_ids.insert(s);
                                        }
                                    }
                                } else if let Some(val_str) = self.value_to_index_string(&val) {
                                    let kv = self
                                        .scan_routed_prefixes(
                                            self.routed_index_prefixes_for_value(
                                                table_name,
                                                &storage_col_name,
                                                &val_str,
                                            ),
                                            txn,
                                            limit,
                                        )
                                        .await?;
                                    all_row_ids.reserve(kv.len());
                                    for (k, payload) in kv {
                                        if let Some(row_id) = Self::row_id_from_key(&k) {
                                            let row_id = row_id.to_string();
                                            if all_row_ids.insert(row_id.clone()) {
                                                if let Some(rows) = &mut covered_rows {
                                                    let include_values =
                                                        Self::secondary_index_payload_values(
                                                            &payload,
                                                            include_indices,
                                                        );
                                                    if !include_indices.is_empty()
                                                        && include_values.is_none()
                                                    {
                                                        include_payloads_complete = false;
                                                    }
                                                    rows.insert(
                                                        row_id.clone(),
                                                        Self::secondary_index_covered_row(
                                                            schema,
                                                            pk_index,
                                                            &row_id,
                                                            col_idx,
                                                            val.clone(),
                                                            include_indices,
                                                            include_values,
                                                        ),
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            return Ok(Some(IndexScanPlan {
                                row_ids: all_row_ids,
                                ordered_row_ids: None,
                                exact: true,
                                ordered_topk_counted: false,
                                covered: covered_rows.and_then(|rows| {
                                    (!rows.is_empty()).then(|| CoveredIndexRows {
                                        column_indices:
                                            Self::secondary_index_covered_column_indices(
                                                pk_index,
                                                col_idx,
                                                if include_payloads_complete {
                                                    include_indices
                                                } else {
                                                    &[]
                                                },
                                            ),
                                        rows,
                                    })
                                }),
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
                            if let Some(prefix) = Self::scan_predicate_like_prefix(pattern_str) {
                                let col = &schema.columns[col_idx];
                                if col.is_indexed
                                    && (col.is_primary || delimited_row_ids_are_unambiguous)
                                {
                                    let all_row_ids = if col.is_primary {
                                        let mut kv = Vec::new();
                                        for mut key_prefix in
                                            self.routed_data_prefixes_for_table(table_name)
                                        {
                                            key_prefix.push_str(&prefix);
                                            let remaining =
                                                limit.map(|limit| limit.saturating_sub(kv.len()));
                                            if remaining == Some(0) {
                                                break;
                                            }
                                            let mut scanned = txn
                                                .scan_prefix(key_prefix.as_bytes(), remaining)
                                                .await?;
                                            kv.append(&mut scanned);
                                            if limit.is_some_and(|limit| kv.len() >= limit) {
                                                break;
                                            }
                                        }
                                        let mut row_ids = HashSet::with_capacity(kv.len());
                                        for (k, _) in kv {
                                            let row_id = self.legacy_row_id_from_routed_data_key(
                                                table_name, &k,
                                            )?;
                                            row_ids.insert(row_id.to_string());
                                        }
                                        row_ids
                                    } else {
                                        let kv = self
                                            .scan_routed_prefixes(
                                                self.routed_index_prefixes_for_value_start(
                                                    table_name,
                                                    &storage_col_name,
                                                    &prefix,
                                                ),
                                                txn,
                                                limit,
                                            )
                                            .await?;
                                        let mut row_ids = HashSet::with_capacity(kv.len());
                                        for (k, _) in kv {
                                            if let Some(row_id) = Self::row_id_from_key(&k) {
                                                row_ids.insert(row_id.to_string());
                                            }
                                        }
                                        row_ids
                                    };

                                    if !all_row_ids.is_empty() {
                                        return Ok(Some(IndexScanPlan {
                                            row_ids: all_row_ids,
                                            ordered_row_ids: None,
                                            exact: true,
                                            ordered_topk_counted: false,
                                            covered: None,
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
                                    if let Some(_visibility_guard) =
                                        ftxn.current_side_index_read_guard().await
                                    {
                                        let storage = &ftxn.storage;
                                        let idx_guard = storage.trigram_index.read().unwrap();
                                        if let Some(ids) = idx_guard.search(
                                            table_name,
                                            &storage_col_name,
                                            pattern_str,
                                        ) {
                                            let row_keys =
                                                idx_guard.map_ids_to_row_keys(table_name, &ids);
                                            if !row_keys.is_empty() {
                                                let mut set =
                                                    HashSet::with_capacity(row_keys.len());
                                                for s in row_keys {
                                                    set.insert(s);
                                                }
                                                return Ok(Some(IndexScanPlan {
                                                    row_ids: set,
                                                    ordered_row_ids: None,
                                                    exact: false,
                                                    ordered_topk_counted: false,
                                                    covered: None,
                                                }));
                                            }
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
                    if let Some((col_idx, storage_col_name)) =
                        self.resolve_schema_column_name(expr, schema)
                    {
                        if self.expr_has_column_reference(low)
                            || self.expr_has_column_reference(high)
                        {
                            return Ok(None);
                        }
                        let col = &schema.columns[col_idx];
                        let low_val = self
                            .evaluate_value(low, &[], schema, params)
                            .unwrap_or(Value::Null);
                        let high_val = self
                            .evaluate_value(high, &[], schema, params)
                            .unwrap_or(Value::Null);
                        let low_val = Self::coerce_value_to_column_type(low_val, &col.data_type)
                            .unwrap_or(Value::Null);
                        let high_val = Self::coerce_value_to_column_type(high_val, &col.data_type)
                            .unwrap_or(Value::Null);

                        if col.is_primary {
                            let low_encoded = Self::value_to_primary_row_id(&low_val);
                            let high_encoded = Self::value_to_primary_row_id(&high_val);

                            if let (Some(low_key), Some(high_key)) = (low_encoded, high_encoded) {
                                let mut kv = Vec::new();
                                for prefix in self.routed_data_prefixes_for_table(table_name) {
                                    let mut start = prefix.as_bytes().to_vec();
                                    start.extend_from_slice(low_key.as_bytes());
                                    let mut end = prefix.as_bytes().to_vec();
                                    end.extend_from_slice(high_key.as_bytes());
                                    end.push(0);
                                    let remaining =
                                        limit.map(|limit| limit.saturating_sub(kv.len()));
                                    if remaining == Some(0) {
                                        break;
                                    }
                                    let mut scanned =
                                        txn.scan_range(&start, &end, remaining).await?;
                                    kv.append(&mut scanned);
                                    if limit.is_some_and(|limit| kv.len() >= limit) {
                                        break;
                                    }
                                }
                                let mut row_ids = HashSet::with_capacity(kv.len());
                                for (k, _) in kv {
                                    let row_id =
                                        self.legacy_row_id_from_routed_data_key(table_name, &k)?;
                                    row_ids.insert(row_id.to_string());
                                }
                                return Ok(Some(IndexScanPlan {
                                    row_ids,
                                    ordered_row_ids: None,
                                    exact: true,
                                    ordered_topk_counted: false,
                                    covered: None,
                                }));
                            }
                        }
                        if delimited_row_ids_are_unambiguous
                            && col.is_indexed
                            && col.index_type == IndexType::BTree
                            && !col.is_primary
                        {
                            if let (Some(low_key), Some(high_key)) = (
                                self.secondary_index_range_value_key(&low_val),
                                self.secondary_index_range_value_key(&high_val),
                            ) {
                                let lower = SecondaryIndexRangeBound {
                                    value_key: low_key,
                                    inclusive: true,
                                };
                                let upper = SecondaryIndexRangeBound {
                                    value_key: high_key,
                                    inclusive: true,
                                };
                                let order_direction = if self.shard_router.is_none() {
                                    self.secondary_index_order_direction(order_by, schema, col_idx)
                                } else {
                                    None
                                };
                                let order_direction = match order_direction {
                                    Some(false) if !txn.supports_bounded_scan_range_reverse() => {
                                        None
                                    }
                                    direction => direction,
                                };
                                let scan_limit = if order_direction.is_some() {
                                    ordered_limit.or(limit)
                                } else {
                                    limit
                                };
                                if single_column_index_includes.is_none() {
                                    single_column_index_includes = Some(
                                        self.load_single_column_index_includes_for_table(
                                            table_name, schema, txn,
                                        )
                                        .await?,
                                    );
                                }
                                let include_indices = single_column_index_includes
                                    .as_ref()
                                    .and_then(|includes| includes.get(&col_idx))
                                    .map(Vec::as_slice)
                                    .unwrap_or(&[]);
                                return Ok(Some(
                                    self.scan_secondary_index_range(
                                        table_name,
                                        &storage_col_name,
                                        schema,
                                        col_idx,
                                        pk_index,
                                        txn,
                                        Some(lower),
                                        Some(upper),
                                        scan_limit,
                                        order_direction,
                                        include_indices,
                                    )
                                    .await?,
                                ));
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
                        .try_index_scan(left, table_name, schema, txn, params, None, None, None)
                        .await?;
                    let right_res = self
                        .try_index_scan(right, table_name, schema, txn, params, None, None, None)
                        .await?;

                    match (left_res, right_res) {
                        (Some(mut l), Some(mut r)) => {
                            // AND: intersect both index results for tighter filtering
                            let row_ids = if l.row_ids.len() <= r.row_ids.len() {
                                l.row_ids.retain(|row_id| r.row_ids.contains(row_id));
                                l.row_ids
                            } else {
                                r.row_ids.retain(|row_id| l.row_ids.contains(row_id));
                                r.row_ids
                            };

                            return Ok(Some(IndexScanPlan {
                                row_ids,
                                ordered_row_ids: None,
                                exact: l.exact && r.exact,
                                ordered_topk_counted: false,
                                covered: None,
                            }));
                        }
                        (Some(s), None) | (None, Some(s)) => {
                            return Ok(Some(IndexScanPlan {
                                row_ids: s.row_ids,
                                ordered_row_ids: None,
                                exact: false,
                                ordered_topk_counted: false,
                                covered: None,
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
                        .try_index_scan(left, table_name, schema, txn, params, None, None, None)
                        .await?;
                    let right_res = self
                        .try_index_scan(right, table_name, schema, txn, params, None, None, None)
                        .await?;

                    // OR: both sides must have index results to be useful
                    if let (Some(mut l), Some(r)) = (left_res, right_res) {
                        l.row_ids.extend(r.row_ids);
                        return Ok(Some(IndexScanPlan {
                            row_ids: l.row_ids,
                            ordered_row_ids: None,
                            exact: l.exact && r.exact,
                            ordered_topk_counted: false,
                            covered: None,
                        }));
                    }
                }
                Expr::Nested(inner) => {
                    return self
                        .try_index_scan(
                            inner,
                            table_name,
                            schema,
                            txn,
                            params,
                            limit,
                            order_by,
                            ordered_limit,
                        )
                        .await;
                }
                _ => {}
            }
            Ok(None)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Executor;
    use crate::common::{encoding::encode_i64_comparable, Value};

    #[test]
    fn data_key_for_row_id_preallocates_exact_key() {
        let key = Executor::data_key_for_row_id("orders", "00042");

        assert_eq!(key, "data:orders:00042");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn data_key_upper_bound_appends_nul_with_capacity() {
        let key = Executor::data_key_upper_bound_for_row_id("orders", "00042");

        assert_eq!(key, "data:orders:00042\0");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn secondary_index_prefix_for_value_preallocates_exact_prefix() {
        let prefix = Executor::secondary_index_prefix_for_value("orders", "status", "open");

        assert_eq!(prefix, "index:orders:status:open:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn secondary_index_prefix_for_value_start_preallocates_exact_prefix() {
        let prefix = Executor::secondary_index_prefix_for_value_start("orders", "status", "op");

        assert_eq!(prefix, "index:orders:status:op");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn primary_key_row_from_parts_matches_type_case_without_uppercase_allocation() {
        let integer_row = Executor::primary_key_row_from_parts(
            2,
            Some(0),
            Some("iNt4"),
            &encode_i64_comparable(7),
        );
        assert_eq!(integer_row, vec![Value::Integer(7), Value::Null]);

        let date_row = Executor::primary_key_row_from_parts(
            2,
            Some(1),
            Some("dAtE"),
            &encode_i64_comparable(31),
        );
        assert_eq!(date_row, vec![Value::Null, Value::Date(31)]);

        let timestamp_row = Executor::primary_key_row_from_parts(
            1,
            Some(0),
            Some("timeStamp(6)"),
            &encode_i64_comparable(123_456),
        );
        assert_eq!(timestamp_row, vec![Value::Timestamp(123_456)]);
    }

    #[test]
    fn secondary_index_order_type_supported_matches_order_preserving_types() {
        for data_type in [
            "BOOLEAN",
            "bool",
            "DATE32",
            "TIMESTAMPTZ",
            "TIMESTAMP WITH TIME ZONE",
            "DATETIME(6)",
            "INTERVAL DAY",
        ] {
            assert!(
                Executor::secondary_index_order_type_supported(data_type),
                "{data_type}"
            );
        }

        for data_type in [
            "TEXT",
            "VARCHAR(32)",
            "DECIMAL",
            "NUMERIC(10,2)",
            "FLOAT",
            "DOUBLE PRECISION",
            "TIMESTAMPZ",
            "DATETIMEX",
            "INTERVALX DAY",
        ] {
            assert!(
                !Executor::secondary_index_order_type_supported(data_type),
                "{data_type}"
            );
        }
    }

    #[test]
    fn secondary_index_value_from_key_decodes_boolean_and_temporal_aliases() {
        assert_eq!(
            Executor::secondary_index_value_from_key("false", "BOOLEAN"),
            Some(Value::Boolean(false))
        );
        assert_eq!(
            Executor::secondary_index_value_from_key("true", "BOOL"),
            Some(Value::Boolean(true))
        );
        assert_eq!(
            Executor::secondary_index_value_from_key(&encode_i64_comparable(31), "DATE32"),
            Some(Value::Date(31))
        );
        assert_eq!(
            Executor::secondary_index_value_from_key(
                &encode_i64_comparable(123_456),
                "TIMESTAMPTZ"
            ),
            Some(Value::Timestamp(123_456))
        );
        assert_eq!(
            Executor::secondary_index_value_from_key(
                &encode_i64_comparable(86_400_000_000),
                "INTERVAL DAY"
            ),
            Some(Value::Interval(86_400_000_000))
        );
        assert_eq!(
            Executor::secondary_index_value_from_key("dec:10", "DECIMAL"),
            None
        );
    }
}
