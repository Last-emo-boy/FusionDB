use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, OrderByKind,
    Value as SqlValue,
};
use std::collections::HashSet;

use super::Executor;

pub(super) const SMALL_INDEX_FETCH_THRESHOLD: usize = 64;

pub(crate) struct IndexScanPlan {
    pub(crate) row_ids: HashSet<String>,
    pub(crate) ordered_row_ids: Option<Vec<String>>,
    pub(crate) exact: bool,
}

impl Executor {
    fn data_key_for_row_id(table_name: &str, row_id: &str) -> String {
        let mut key = String::with_capacity("data:".len() + table_name.len() + 1 + row_id.len());
        key.push_str("data:");
        key.push_str(table_name);
        key.push(':');
        key.push_str(row_id);
        key
    }

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

    pub(super) fn primary_key_row_from_id(
        schema: &TableSchema,
        pk_index: Option<usize>,
        row_id: &str,
    ) -> Vec<Value> {
        let pk_type_upper = pk_index
            .and_then(|pk_idx| schema.columns.get(pk_idx))
            .map(|column| column.data_type.to_ascii_uppercase());
        Self::primary_key_row_from_parts(
            schema.columns.len(),
            pk_index,
            pk_type_upper.as_deref(),
            row_id,
        )
    }

    pub(super) fn primary_key_row_from_parts(
        schema_width: usize,
        pk_index: Option<usize>,
        pk_type_upper: Option<&str>,
        row_id: &str,
    ) -> Vec<Value> {
        let mut row = vec![Value::Null; schema_width];
        let (Some(pk_idx), Some(upper)) = (pk_index, pk_type_upper) else {
            return row;
        };
        if pk_idx >= schema_width {
            return row;
        }

        row[pk_idx] = if crate::execution::Executor::is_integer_type_name(upper) {
            crate::common::encoding::decode_i64_comparable(row_id)
                .map(Value::Integer)
                .unwrap_or_else(|| Value::String(row_id.to_string()))
        } else if upper == "DATE" {
            crate::common::encoding::decode_i64_comparable(row_id)
                .map(|days| Value::Date(days as i32))
                .unwrap_or_else(|| Value::String(row_id.to_string()))
        } else if upper.starts_with("TIMESTAMP") || upper == "DATETIME" {
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
        let data_key = Self::data_key_for_row_id(table_name, row_id);
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
                            let val = Self::coerce_value_to_column_type(
                                val,
                                &schema.columns[col_idx].data_type,
                            )
                            .unwrap_or(Value::Null);
                            if let Some(val_str) = self.value_to_index_string(&val) {
                                let primary_order_asc =
                                    self.order_by_primary_key_direction(order_by, schema);
                                let scan_limit = if primary_order_asc == Some(true) {
                                    ordered_limit.or(limit)
                                } else if primary_order_asc == Some(false) {
                                    None
                                } else {
                                    limit
                                };
                                let index_prefix = format!(
                                    "index:{}:{}:{}:",
                                    table_name, storage_col_name, val_str
                                );
                                let index_entries =
                                    txn.scan_prefix(index_prefix.as_bytes(), scan_limit).await?;

                                let mut row_ids = HashSet::with_capacity(index_entries.len());
                                let mut ordered_row_ids = primary_order_asc
                                    .map(|_| Vec::with_capacity(index_entries.len()));
                                for (k, _) in index_entries {
                                    if let Some(row_id) = Self::row_id_from_key(&k) {
                                        let row_id = row_id.to_string();
                                        if row_ids.insert(row_id.clone()) {
                                            if let Some(ordered) = &mut ordered_row_ids {
                                                ordered.push(row_id);
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
                                            let index_prefix = Self::fts_token_prefix_for_token(
                                                table_name, &col_name, &token,
                                            );
                                            let index_entries = txn
                                                .scan_prefix(index_prefix.as_bytes(), None)
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
                            let mut all_row_ids = HashSet::with_capacity(list.len());
                            for item in list {
                                let val = self
                                    .evaluate_value(item, &[], schema, params)
                                    .unwrap_or(Value::Null);
                                let val = Self::coerce_value_to_column_type(val, &col.data_type)
                                    .unwrap_or(Value::Null);

                                if col.is_primary {
                                    let val_str = Self::value_to_primary_row_id(&val);
                                    if let Some(s) = val_str {
                                        let key = Self::data_key_for_row_id(table_name, &s);
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
                                    all_row_ids.reserve(kv.len());
                                    for (k, _) in kv {
                                        if let Some(row_id) = Self::row_id_from_key(&k) {
                                            all_row_ids.insert(row_id.to_string());
                                        }
                                    }
                                }
                            }
                            return Ok(Some(IndexScanPlan {
                                row_ids: all_row_ids,
                                ordered_row_ids: None,
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
                                    let all_row_ids = if col.is_primary {
                                        let key_prefix =
                                            Self::data_key_for_row_id(table_name, &prefix);
                                        let kv =
                                            txn.scan_prefix(key_prefix.as_bytes(), limit).await?;
                                        let mut row_ids = HashSet::with_capacity(kv.len());
                                        for (k, _) in kv {
                                            if let Some(row_id) = Self::row_id_from_key(&k) {
                                                row_ids.insert(row_id.to_string());
                                            }
                                        }
                                        row_ids
                                    } else {
                                        let index_prefix = format!(
                                            "index:{}:{}:{}",
                                            table_name, storage_col_name, prefix
                                        );
                                        let kv =
                                            txn.scan_prefix(index_prefix.as_bytes(), limit).await?;
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
                                            let mut set = HashSet::with_capacity(row_keys.len());
                                            for s in row_keys {
                                                set.insert(s);
                                            }
                                            return Ok(Some(IndexScanPlan {
                                                row_ids: set,
                                                ordered_row_ids: None,
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
                        let low_val = Self::coerce_value_to_column_type(low_val, &col.data_type)
                            .unwrap_or(Value::Null);
                        let high_val = Self::coerce_value_to_column_type(high_val, &col.data_type)
                            .unwrap_or(Value::Null);

                        if col.is_primary {
                            let low_encoded = Self::value_to_primary_row_id(&low_val);
                            let high_encoded = Self::value_to_primary_row_id(&high_val);

                            if let (Some(low_key), Some(high_key)) = (low_encoded, high_encoded) {
                                let start = Self::data_key_for_row_id(table_name, &low_key);
                                let end =
                                    Self::data_key_upper_bound_for_row_id(table_name, &high_key);
                                let kv = txn
                                    .scan_range(start.as_bytes(), end.as_bytes(), limit)
                                    .await?;
                                let mut row_ids = HashSet::with_capacity(kv.len());
                                for (k, _) in kv {
                                    if let Some(row_id) = Self::row_id_from_key(&k) {
                                        row_ids.insert(row_id.to_string());
                                    }
                                }
                                return Ok(Some(IndexScanPlan {
                                    row_ids,
                                    ordered_row_ids: None,
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
                            }));
                        }
                        (Some(s), None) | (None, Some(s)) => {
                            return Ok(Some(IndexScanPlan {
                                row_ids: s.row_ids,
                                ordered_row_ids: None,
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
}
