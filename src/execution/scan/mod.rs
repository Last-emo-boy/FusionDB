mod index_plan;
mod join;
mod predicate;

use index_plan::SMALL_INDEX_FETCH_THRESHOLD;
pub(crate) use index_plan::{CoveredIndexRows, IndexScanPlan};

use crate::catalog::Column;
use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::{
    self, ScanVisitor, SqlBlockZoneMapComparisonOp, SqlBlockZoneMapPredicateKind,
    SqlBlockZoneMapPredicateTerm, SqlBlockZoneMapPruningPlan, StorageScanOptions, Transaction,
};
use futures::stream::StreamExt;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName,
    ObjectNamePart, OrderByKind, TableFactor, TableFunctionArgs,
};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use super::analyze::TableStats;
use super::stats::StatsEstimator;
use super::Executor;

const STATS_INDEX_PROBE_LIMIT_MAX: usize = 65_536;

struct ScanPredicateTerm {
    value_slot: usize,
    kind: ScanPredicateTermKind,
}

enum ScanPredicateTermKind {
    Compare { op: BinaryOperator, value: Value },
    InList { values: Vec<Value> },
    LikePrefix { prefix: String },
    LikePattern { pattern: String, negated: bool },
}

struct ScanPredicatePlan {
    terms: Vec<ScanPredicateTerm>,
    column_indices: Vec<usize>,
}

struct PrimaryKeyRangeScanPlan {
    lower_row_id: Option<String>,
    upper_row_id: Option<String>,
}

impl ScanPredicateTerm {
    fn matches(&self, value: &Value) -> bool {
        match &self.kind {
            ScanPredicateTermKind::Compare { op, value: rhs } => {
                if matches!(value, Value::Null) || matches!(rhs, Value::Null) {
                    return false;
                }

                match op {
                    BinaryOperator::Eq => value == rhs,
                    BinaryOperator::NotEq => value != rhs,
                    BinaryOperator::Gt => value.compare(rhs) == Ordering::Greater,
                    BinaryOperator::Lt => value.compare(rhs) == Ordering::Less,
                    BinaryOperator::GtEq => value.compare(rhs) != Ordering::Less,
                    BinaryOperator::LtEq => value.compare(rhs) != Ordering::Greater,
                    _ => false,
                }
            }
            ScanPredicateTermKind::InList { values } => {
                if matches!(value, Value::Null) {
                    return false;
                }
                values
                    .iter()
                    .any(|candidate| value.compare(candidate) == Ordering::Equal)
            }
            ScanPredicateTermKind::LikePrefix { prefix } => match value {
                Value::String(text) => text.starts_with(prefix),
                _ => false,
            },
            ScanPredicateTermKind::LikePattern { pattern, negated } => match value {
                Value::String(text) => {
                    let matched = Executor::like_match(text, pattern);
                    if *negated {
                        !matched
                    } else {
                        matched
                    }
                }
                _ => false,
            },
        }
    }
}

impl ScanPredicatePlan {
    fn scratch_values(&self) -> Vec<Value> {
        Vec::with_capacity(self.column_indices.len())
    }

    fn decode_values(&self, data: &[u8], values: &mut Vec<Value>) -> Result<()> {
        values.clear();
        values.reserve(self.column_indices.len());
        for &column_index in &self.column_indices {
            values.push(
                crate::common::encoding::RowDecoder::decode_column(data, column_index)
                    .map_err(|e| {
                        FusionError::Execution(format!("Data deserialization error: {}", e))
                    })?
                    .unwrap_or(Value::Null),
            );
        }
        Ok(())
    }

    fn matches_values(&self, values: &[Value]) -> bool {
        for term in &self.terms {
            let Some(value) = values.get(term.value_slot) else {
                return false;
            };
            if !term.matches(value) {
                return false;
            }
        }
        true
    }
}

fn scan_object_name_eq_ascii(name: &ObjectName, expected: &str) -> bool {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)] => ident.value.eq_ignore_ascii_case(expected),
        [ObjectNamePart::Function(function)] => function.name.value.eq_ignore_ascii_case(expected),
        _ => false,
    }
}

impl Executor {
    pub(crate) fn generate_subscripts_schema(relation: &TableFactor) -> Option<TableSchema> {
        let TableFactor::Table {
            name, alias, args, ..
        } = relation
        else {
            return None;
        };
        if !scan_object_name_eq_ascii(name, "generate_subscripts") || args.is_none() {
            return None;
        }

        let column_name = alias
            .as_ref()
            .and_then(|alias| alias.columns.first())
            .map(|column| column.name.value.clone())
            .or_else(|| alias.as_ref().map(|alias| alias.name.value.clone()))
            .unwrap_or_else(|| "generate_subscripts".to_string());
        Some(TableSchema::new(
            alias
                .as_ref()
                .map(|alias| alias.name.value.clone())
                .unwrap_or_else(|| "generate_subscripts".to_string()),
            vec![Column {
                name: column_name,
                data_type: "INTEGER".to_string(),
                is_primary: false,
                is_indexed: false,
                index_type: IndexType::None,
                default_value: None,
                is_nullable: false,
                is_unique: false,
                check_expr: None,
            }],
        ))
    }

    pub(crate) fn generate_subscripts_rows_for_value(value: Value, dim: i64) -> Vec<Vec<Value>> {
        let Value::Array(values) = value else {
            return Vec::new();
        };
        if dim <= 1 {
            let mut rows = Vec::with_capacity(values.len());
            for index in 1..=values.len() {
                rows.push(vec![Value::Integer(index as i64)]);
            }
            return rows;
        }

        let max_len = values
            .iter()
            .filter_map(|value| match value {
                Value::Array(values) => Some(values.len()),
                _ => None,
            })
            .max()
            .unwrap_or(0);
        let mut rows = Vec::with_capacity(max_len);
        for index in 1..=max_len {
            rows.push(vec![Value::Integer(index as i64)]);
        }
        rows
    }

    pub(crate) fn table_function_args_exprs(args: &TableFunctionArgs) -> Option<Vec<&Expr>> {
        let mut exprs = Vec::with_capacity(args.args.len());
        for arg in &args.args {
            let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
                return None;
            };
            exprs.push(expr);
        }
        Some(exprs)
    }

    pub(crate) fn evaluate_generate_subscripts(
        &self,
        relation: &TableFactor,
        row: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Option<Result<(TableSchema, Vec<Vec<Value>>)>> {
        let TableFactor::Table { args, .. } = relation else {
            return None;
        };
        let output_schema = Self::generate_subscripts_schema(relation)?;
        let args = args.as_ref()?;
        let exprs = match Self::table_function_args_exprs(args) {
            Some(exprs) if exprs.len() == 2 => exprs,
            _ => {
                return Some(Err(FusionError::Execution(
                    "generate_subscripts expects two arguments".to_string(),
                )))
            }
        };
        let value = match self.evaluate_value(exprs[0], row, schema, params) {
            Ok(value) => value,
            Err(err) => return Some(Err(err)),
        };
        let dim = match self.evaluate_value(exprs[1], row, schema, params) {
            Ok(Value::Integer(value)) => value,
            Ok(_) => {
                return Some(Err(FusionError::Execution(
                    "generate_subscripts dimension must be integer".to_string(),
                )))
            }
            Err(err) => return Some(Err(err)),
        };

        Some(Ok((
            output_schema,
            Self::generate_subscripts_rows_for_value(value, dim),
        )))
    }

    fn materialized_query_schema(table_name: String, columns: Vec<String>) -> TableSchema {
        let mut schema_columns = Vec::with_capacity(columns.len());
        for name in columns {
            schema_columns.push(Column {
                name,
                data_type: "UNKNOWN".to_string(),
                is_primary: false,
                is_indexed: false,
                index_type: IndexType::None,
                default_value: None,
                is_nullable: true,
                is_unique: false,
                check_expr: None,
            });
        }
        TableSchema::new(table_name, schema_columns)
    }

    #[cfg(test)]
    fn scan_data_key_for_row_id(table_name: &str, row_id: &str) -> String {
        let mut key = String::with_capacity("data:".len() + table_name.len() + 1 + row_id.len());
        key.push_str("data:");
        key.push_str(table_name);
        key.push(':');
        key.push_str(row_id);
        key
    }

    #[cfg(test)]
    fn scan_data_prefix_for_table(table_name: &str) -> String {
        let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
        prefix.push_str("data:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix
    }

    fn scan_schema_key_for_table(table_name: &str) -> String {
        let mut key = String::with_capacity("schema:".len() + table_name.len());
        key.push_str("schema:");
        key.push_str(table_name);
        key
    }

    fn scan_view_key_for_table(table_name: &str) -> String {
        let mut key = String::with_capacity("view:".len() + table_name.len());
        key.push_str("view:");
        key.push_str(table_name);
        key
    }

    fn scan_view_wrapped_query_sql(view_sql: &str) -> String {
        let mut sql =
            String::with_capacity("SELECT * FROM (".len() + view_sql.len() + ") AS _v".len());
        sql.push_str("SELECT * FROM (");
        sql.push_str(view_sql);
        sql.push_str(") AS _v");
        sql
    }

    fn projection_indices_for_scan(
        projection: &Option<Vec<String>>,
        schema: &TableSchema,
    ) -> Option<Vec<usize>> {
        let cols = projection.as_ref()?;
        if cols.is_empty() {
            return Some(Vec::new());
        }

        if cols.len().saturating_mul(schema.columns.len()) <= 32 {
            let mut indices = Vec::with_capacity(cols.len());
            for name in cols {
                if let Some(idx) = Self::projection_index_for_name(name, schema) {
                    indices.push(idx);
                }
            }
            return if indices.is_empty() {
                None
            } else {
                Some(indices)
            };
        }

        let mut column_indices = HashMap::with_capacity(schema.columns.len());
        for (idx, column) in schema.columns.iter().enumerate() {
            column_indices
                .entry(column.name.to_ascii_lowercase())
                .or_insert(idx);
        }

        let mut indices = Vec::with_capacity(cols.len());
        for name in cols {
            if let Some(idx) = column_indices.get(&name.to_ascii_lowercase()) {
                indices.push(*idx);
            } else if let Some(idx) = Self::projection_index_for_name(name, schema) {
                indices.push(idx);
            }
        }

        if indices.is_empty() {
            None
        } else {
            Some(indices)
        }
    }

    fn covered_index_rows_satisfy_projection(
        covered: Option<&CoveredIndexRows>,
        projection_indices: Option<&[usize]>,
    ) -> bool {
        let (Some(covered), Some(indices)) = (covered, projection_indices) else {
            return false;
        };
        indices
            .iter()
            .all(|index| covered.column_indices.contains(index))
    }

    fn projection_index_for_name(name: &str, schema: &TableSchema) -> Option<usize> {
        if let Some((idx, _)) = schema
            .columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.name.eq_ignore_ascii_case(name))
        {
            return Some(idx);
        }

        let fallback_name = name.rsplit('.').next().unwrap_or(name);
        let suffix = format!(".{}", fallback_name);
        let suffix_lower = suffix.to_ascii_lowercase();
        let mut matches = schema.columns.iter().enumerate().filter_map(|(idx, col)| {
            if col.name.eq_ignore_ascii_case(fallback_name)
                || col.name.eq_ignore_ascii_case(name)
                || col.name.ends_with(&suffix)
                || col.name.to_ascii_lowercase().ends_with(&suffix_lower)
            {
                Some(idx)
            } else {
                None
            }
        });

        let first = matches.next()?;
        if matches.next().is_none() {
            Some(first)
        } else {
            None
        }
    }

    async fn scan_derived_table(
        &self,
        subquery: &sqlparser::ast::Query,
        alias: &Option<sqlparser::ast::TableAlias>,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        let result = Box::pin(self.handle_query(subquery, txn, params)).await?;
        if let super::QueryResult::Select { columns, rows } = result {
            let table_name = alias
                .as_ref()
                .map(|alias| alias.name.value.clone())
                .unwrap_or_else(|| "derived".to_string());
            return Ok((Self::materialized_query_schema(table_name, columns), rows));
        }

        Err(FusionError::Execution(
            "Derived table subquery must return rows".to_string(),
        ))
    }

    pub(crate) async fn scan_table_base(
        &self,
        relation: &TableFactor,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        match relation {
            TableFactor::Table { name, .. } => {
                if let Some(result) = self.evaluate_generate_subscripts(
                    relation,
                    &[],
                    &TableSchema::new("generate_subscripts_input".to_string(), vec![]),
                    params,
                ) {
                    return result;
                }

                let table_name = name.to_string();
                let schema_key = Self::scan_schema_key_for_table(&table_name);

                // Try table first
                if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
                    let schema: TableSchema = bincode::deserialize(&schema_bytes).map_err(|e| {
                        FusionError::Execution(format!("Schema deserialization error: {}", e))
                    })?;

                    let kv_pairs = self
                        .scan_routed_data_prefixes_for_table(&table_name, txn, None)
                        .await?;
                    let mut rows = Vec::with_capacity(kv_pairs.len());
                    for (k, v) in kv_pairs {
                        let row: Vec<Value> = if let Ok(key_str) = std::str::from_utf8(&k) {
                            if let Some(row) = self.row_cache.get(key_str) {
                                monitor::inc_row_cache_hit();
                                row
                            } else {
                                let row = crate::common::encoding::RowDecoder::decode(&v).map_err(
                                    |e| {
                                        FusionError::Execution(format!(
                                            "Data deserialization error: {}",
                                            e
                                        ))
                                    },
                                )?;
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
                let view_key = Self::scan_view_key_for_table(&table_name);
                if let Some(view_bytes) = txn.get(view_key.as_bytes()).await? {
                    let view_sql = String::from_utf8(view_bytes)
                        .map_err(|e| FusionError::Execution(format!("View decode error: {}", e)))?;
                    let wrapped_sql = Self::scan_view_wrapped_query_sql(&view_sql);
                    let stmts = crate::parser::parse_sql(&wrapped_sql)?;
                    if let Some(sqlparser::ast::Statement::Query(query)) = stmts.into_iter().next()
                    {
                        let result = Box::pin(self.handle_query(&query, txn, params)).await?;
                        if let super::QueryResult::Select { columns, rows } = result {
                            let schema = Self::materialized_query_schema(table_name, columns);
                            return Ok((schema, rows));
                        }
                    }
                }

                Err(FusionError::Execution(format!(
                    "Table {} not found",
                    table_name
                )))
            }
            TableFactor::Derived {
                subquery,
                alias,
                lateral,
            } => {
                if *lateral {
                    return Err(FusionError::Execution(
                        "Unsupported lateral derived table".to_string(),
                    ));
                }
                self.scan_derived_table(subquery, alias, txn, params).await
            }
            _ => Err(FusionError::Execution(
                "Unsupported table factor".to_string(),
            )),
        }
    }

    async fn stats_guided_index_probe_limit(
        &self,
        table_name: &str,
        selection: &Expr,
        schema: &TableSchema,
        txn: &mut dyn Transaction,
        limit: Option<usize>,
        order_by: Option<&sqlparser::ast::OrderBy>,
    ) -> Result<Option<usize>> {
        let baseline_cap = Self::index_candidate_cap(limit, order_by);
        let Some(stats) = self.load_table_stats(table_name, txn).await? else {
            return Ok(None);
        };
        let Some(estimated_rows) =
            self.scan_estimate_indexable_predicate_rows(selection, schema, &stats)
        else {
            return Ok(None);
        };

        if estimated_rows <= baseline_cap {
            return Ok(None);
        }

        let table_rows = stats.row_count.max(1);
        let index_cost = (table_rows as f64).log2().max(1.0) + estimated_rows as f64;
        if index_cost < table_rows as f64 {
            Ok(Some(
                estimated_rows
                    .min(STATS_INDEX_PROBE_LIMIT_MAX)
                    .max(baseline_cap),
            ))
        } else {
            Ok(None)
        }
    }

    fn scan_estimate_indexable_predicate_rows(
        &self,
        selection: &Expr,
        schema: &TableSchema,
        stats: &TableStats,
    ) -> Option<usize> {
        Self::collect_conjunctive_predicates(selection)
            .iter()
            .filter_map(|predicate| self.scan_estimate_single_index_rows(predicate, schema, stats))
            .min()
    }

    fn scan_estimate_single_index_rows(
        &self,
        expr: &Expr,
        schema: &TableSchema,
        stats: &TableStats,
    ) -> Option<usize> {
        let estimator = StatsEstimator::new(schema, stats);
        match expr {
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
                let column_idx = self.scan_column_constant_index(left, right, schema)?;
                let column = schema.columns.get(column_idx)?;
                if !Self::scan_column_can_use_btree_index(column) {
                    return None;
                }
                estimator.equality_rows(column_idx)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } if !*negated => {
                let column_idx = self.resolve_schema_column_index(expr, schema)?;
                let column = schema.columns.get(column_idx)?;
                if !Self::scan_column_can_use_btree_index(column) {
                    return None;
                }
                estimator.in_list_rows(column_idx, list.len())
            }
            Expr::Nested(inner) => self.scan_estimate_single_index_rows(inner, schema, stats),
            _ => None,
        }
    }

    fn scan_column_constant_index(
        &self,
        left: &Expr,
        right: &Expr,
        schema: &TableSchema,
    ) -> Option<usize> {
        let left_idx = self.resolve_schema_column_index(left, schema);
        let right_idx = self.resolve_schema_column_index(right, schema);

        if left_idx.is_some() && right_idx.is_none() && !self.expr_has_column_reference(right) {
            left_idx
        } else if right_idx.is_some() && left_idx.is_none() && !self.expr_has_column_reference(left)
        {
            right_idx
        } else {
            None
        }
    }

    fn scan_column_can_use_btree_index(column: &Column) -> bool {
        column.is_primary || (column.is_indexed && column.index_type == IndexType::BTree)
    }

    fn primary_key_range_scan_plan(
        &self,
        selection: &Expr,
        schema: &TableSchema,
        pk_idx: usize,
        params: &[Value],
    ) -> Result<Option<PrimaryKeyRangeScanPlan>> {
        let predicates = Self::collect_conjunctive_predicates(selection);
        if predicates.is_empty() {
            return Ok(None);
        }

        let mut lower_row_id: Option<String> = None;
        let mut upper_row_id: Option<String> = None;
        for predicate in predicates {
            let Expr::BinaryOp { left, op, right } = predicate else {
                return Ok(None);
            };
            let Some((range_op, value_expr)) =
                self.primary_key_range_value_expr(&left, &op, &right, schema, pk_idx)
            else {
                return Ok(None);
            };

            let value = match self.evaluate_value(value_expr, &[], schema, params) {
                Ok(value) => value,
                Err(_) => return Ok(None),
            };
            let value =
                match Self::coerce_value_to_column_type(value, &schema.columns[pk_idx].data_type) {
                    Ok(value) => value,
                    Err(_) => return Ok(None),
                };
            let Some(mut row_id) = Self::value_to_primary_row_id(&value) else {
                return Ok(None);
            };

            match range_op {
                BinaryOperator::Gt => {
                    row_id.push('\0');
                    if lower_row_id
                        .as_ref()
                        .map_or(true, |current| row_id.as_str() > current.as_str())
                    {
                        lower_row_id = Some(row_id);
                    }
                }
                BinaryOperator::GtEq => {
                    if lower_row_id
                        .as_ref()
                        .map_or(true, |current| row_id.as_str() > current.as_str())
                    {
                        lower_row_id = Some(row_id);
                    }
                }
                BinaryOperator::Lt => {
                    if upper_row_id
                        .as_ref()
                        .map_or(true, |current| row_id.as_str() < current.as_str())
                    {
                        upper_row_id = Some(row_id);
                    }
                }
                BinaryOperator::LtEq => {
                    row_id.push('\0');
                    if upper_row_id
                        .as_ref()
                        .map_or(true, |current| row_id.as_str() < current.as_str())
                    {
                        upper_row_id = Some(row_id);
                    }
                }
                _ => return Ok(None),
            }
        }

        Ok(Some(PrimaryKeyRangeScanPlan {
            lower_row_id,
            upper_row_id,
        }))
    }

    fn scan_predicate_value_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Value(_) => true,
            Expr::Nested(inner) => Self::scan_predicate_value_expr(inner),
            Expr::UnaryOp { expr, .. } => Self::scan_predicate_value_expr(expr),
            Expr::BinaryOp { left, right, .. } => {
                Self::scan_predicate_value_expr(left) && Self::scan_predicate_value_expr(right)
            }
            _ => false,
        }
    }

    fn scan_predicate_value_slot(column_indices: &mut Vec<usize>, column_index: usize) -> usize {
        match column_indices.iter().position(|&idx| idx == column_index) {
            Some(slot) => slot,
            None => {
                column_indices.push(column_index);
                column_indices.len() - 1
            }
        }
    }

    fn scan_predicate_eq_value(
        &self,
        predicate: &Expr,
        schema: &TableSchema,
        params: &[Value],
    ) -> Option<(usize, Value)> {
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = predicate
        else {
            return None;
        };

        if let Some(column_index) = self.resolve_schema_column_index(left, schema) {
            if !Self::scan_predicate_value_expr(right) {
                return None;
            }
            let value = self.evaluate_value(right, &[], schema, params).ok()?;
            let value =
                Self::coerce_value_to_column_type(value, &schema.columns[column_index].data_type)
                    .ok()?;
            return Some((column_index, value));
        }

        if let Some(column_index) = self.resolve_schema_column_index(right, schema) {
            if !Self::scan_predicate_value_expr(left) {
                return None;
            }
            let value = self.evaluate_value(left, &[], schema, params).ok()?;
            let value =
                Self::coerce_value_to_column_type(value, &schema.columns[column_index].data_type)
                    .ok()?;
            return Some((column_index, value));
        }

        None
    }

    fn scan_predicate_or_eq_term(
        &self,
        predicate: &Expr,
        schema: &TableSchema,
        params: &[Value],
        column_indices: &mut Vec<usize>,
    ) -> Option<ScanPredicateTerm> {
        let disjuncts = Self::collect_disjunctive_predicates(predicate);
        if disjuncts.len() < 2 {
            return None;
        }

        let mut column_index = None;
        let mut values = Vec::with_capacity(disjuncts.len());
        for disjunct in disjuncts {
            let (candidate_column_index, value) =
                self.scan_predicate_eq_value(&disjunct, schema, params)?;
            if column_index
                .replace(candidate_column_index)
                .is_some_and(|idx| idx != candidate_column_index)
            {
                return None;
            }
            if !matches!(value, Value::Null) {
                values.push(value);
            }
        }

        let column_index = column_index?;
        let value_slot = Self::scan_predicate_value_slot(column_indices, column_index);
        Some(ScanPredicateTerm {
            value_slot,
            kind: ScanPredicateTermKind::InList { values },
        })
    }

    fn scan_predicate_between_terms(
        &self,
        predicate: &Expr,
        schema: &TableSchema,
        params: &[Value],
        column_indices: &mut Vec<usize>,
    ) -> Option<[ScanPredicateTerm; 2]> {
        let Expr::Between {
            expr,
            negated,
            low,
            high,
        } = predicate
        else {
            return None;
        };
        if *negated {
            return None;
        }

        let column_index = self.resolve_schema_column_index(expr, schema)?;
        if !Self::scan_predicate_value_expr(low) || !Self::scan_predicate_value_expr(high) {
            return None;
        }

        let low_value = self.evaluate_value(low, &[], schema, params).ok()?;
        let low_value =
            Self::coerce_value_to_column_type(low_value, &schema.columns[column_index].data_type)
                .ok()?;
        let high_value = self.evaluate_value(high, &[], schema, params).ok()?;
        let high_value =
            Self::coerce_value_to_column_type(high_value, &schema.columns[column_index].data_type)
                .ok()?;

        let value_slot = Self::scan_predicate_value_slot(column_indices, column_index);
        Some([
            ScanPredicateTerm {
                value_slot,
                kind: ScanPredicateTermKind::Compare {
                    op: BinaryOperator::GtEq,
                    value: low_value,
                },
            },
            ScanPredicateTerm {
                value_slot,
                kind: ScanPredicateTermKind::Compare {
                    op: BinaryOperator::LtEq,
                    value: high_value,
                },
            },
        ])
    }

    pub(crate) fn scan_predicate_like_prefix(pattern: &str) -> Option<String> {
        let prefix = pattern.strip_suffix('%')?.trim_end_matches('%').to_string();
        if prefix.is_empty() || prefix.chars().any(|ch| matches!(ch, '%' | '_' | '?')) {
            return None;
        }
        Some(prefix)
    }

    fn scan_predicate_like_term(
        &self,
        predicate: &Expr,
        schema: &TableSchema,
        params: &[Value],
        column_indices: &mut Vec<usize>,
    ) -> Option<ScanPredicateTerm> {
        let Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } = predicate
        else {
            return None;
        };
        if *any || escape_char.is_some() {
            return None;
        }

        let column_index = self.resolve_schema_column_index(expr, schema)?;
        if !Self::scan_predicate_value_expr(pattern) {
            return None;
        }

        let pattern_value = self.evaluate_value(pattern, &[], schema, params).ok()?;
        let Value::String(pattern) = pattern_value else {
            return None;
        };
        let value_slot = Self::scan_predicate_value_slot(column_indices, column_index);
        if !*negated {
            if let Some(prefix) = Self::scan_predicate_like_prefix(&pattern) {
                return Some(ScanPredicateTerm {
                    value_slot,
                    kind: ScanPredicateTermKind::LikePrefix { prefix },
                });
            }
        }

        Some(ScanPredicateTerm {
            value_slot,
            kind: ScanPredicateTermKind::LikePattern {
                pattern,
                negated: *negated,
            },
        })
    }

    fn sql_block_zone_map_column_term(
        schema: &TableSchema,
        column_index: usize,
        kind: SqlBlockZoneMapPredicateKind,
    ) -> Option<SqlBlockZoneMapPredicateTerm> {
        let column = schema.columns.get(column_index)?;
        let type_tag = storage::sql_block_zone_map_type_tag(&column.data_type)?;
        Some(SqlBlockZoneMapPredicateTerm {
            column_index: u32::try_from(column_index).ok()?,
            column_name: column.name.clone(),
            type_tag,
            value_encoding_version: storage::SQL_BLOCK_ZONE_MAP_VALUE_ENCODING_VERSION,
            kind,
        })
    }

    fn sql_block_zone_map_scalar_for_column(
        &self,
        expr: &Expr,
        schema: &TableSchema,
        params: &[Value],
        column_index: usize,
    ) -> Option<i64> {
        if !Self::scan_predicate_value_expr(expr) {
            return None;
        }
        let column = schema.columns.get(column_index)?;
        let type_tag = storage::sql_block_zone_map_type_tag(&column.data_type)?;
        let value = self.evaluate_value(expr, &[], schema, params).ok()?;
        let value = Self::coerce_value_to_column_type(value, &column.data_type).ok()?;
        storage::sql_block_zone_map_scalar(&value, type_tag)?
    }

    fn sql_block_zone_map_compare_op(op: &BinaryOperator) -> Option<SqlBlockZoneMapComparisonOp> {
        match op {
            BinaryOperator::Eq => Some(SqlBlockZoneMapComparisonOp::Eq),
            BinaryOperator::Lt => Some(SqlBlockZoneMapComparisonOp::Lt),
            BinaryOperator::LtEq => Some(SqlBlockZoneMapComparisonOp::LtEq),
            BinaryOperator::Gt => Some(SqlBlockZoneMapComparisonOp::Gt),
            BinaryOperator::GtEq => Some(SqlBlockZoneMapComparisonOp::GtEq),
            _ => None,
        }
    }

    fn sql_block_zone_map_reverse_compare_op(
        op: &BinaryOperator,
    ) -> Option<SqlBlockZoneMapComparisonOp> {
        match op {
            BinaryOperator::Eq => Some(SqlBlockZoneMapComparisonOp::Eq),
            BinaryOperator::Lt => Some(SqlBlockZoneMapComparisonOp::Gt),
            BinaryOperator::LtEq => Some(SqlBlockZoneMapComparisonOp::GtEq),
            BinaryOperator::Gt => Some(SqlBlockZoneMapComparisonOp::Lt),
            BinaryOperator::GtEq => Some(SqlBlockZoneMapComparisonOp::LtEq),
            _ => None,
        }
    }

    fn sql_block_zone_map_between_terms(
        &self,
        predicate: &Expr,
        schema: &TableSchema,
        params: &[Value],
    ) -> Option<[SqlBlockZoneMapPredicateTerm; 2]> {
        let Expr::Between {
            expr,
            negated,
            low,
            high,
        } = predicate
        else {
            return None;
        };
        if *negated {
            return None;
        }

        let column_index = self.resolve_schema_column_index(expr, schema)?;
        let low_scalar =
            self.sql_block_zone_map_scalar_for_column(low, schema, params, column_index)?;
        let high_scalar =
            self.sql_block_zone_map_scalar_for_column(high, schema, params, column_index)?;

        Some([
            Self::sql_block_zone_map_column_term(
                schema,
                column_index,
                SqlBlockZoneMapPredicateKind::Compare {
                    op: SqlBlockZoneMapComparisonOp::GtEq,
                    scalar: low_scalar,
                },
            )?,
            Self::sql_block_zone_map_column_term(
                schema,
                column_index,
                SqlBlockZoneMapPredicateKind::Compare {
                    op: SqlBlockZoneMapComparisonOp::LtEq,
                    scalar: high_scalar,
                },
            )?,
        ])
    }

    fn sql_block_zone_map_in_list_term(
        &self,
        predicate: &Expr,
        schema: &TableSchema,
        params: &[Value],
    ) -> Option<SqlBlockZoneMapPredicateTerm> {
        let Expr::InList {
            expr,
            list,
            negated,
        } = predicate
        else {
            return None;
        };
        if *negated || list.is_empty() {
            return None;
        }

        let column_index = self.resolve_schema_column_index(expr, schema)?;
        let mut scalars = Vec::with_capacity(list.len());
        for item in list {
            scalars.push(self.sql_block_zone_map_scalar_for_column(
                item,
                schema,
                params,
                column_index,
            )?);
        }
        scalars.sort_unstable();
        scalars.dedup();
        if scalars.is_empty() {
            return None;
        }

        Self::sql_block_zone_map_column_term(
            schema,
            column_index,
            SqlBlockZoneMapPredicateKind::InList { scalars },
        )
    }

    fn sql_block_zone_map_binary_term(
        &self,
        predicate: &Expr,
        schema: &TableSchema,
        params: &[Value],
    ) -> Option<SqlBlockZoneMapPredicateTerm> {
        let Expr::BinaryOp { left, op, right } = predicate else {
            return None;
        };

        if let Some(column_index) = self.resolve_schema_column_index(left, schema) {
            let scalar =
                self.sql_block_zone_map_scalar_for_column(right, schema, params, column_index)?;
            return Self::sql_block_zone_map_column_term(
                schema,
                column_index,
                SqlBlockZoneMapPredicateKind::Compare {
                    op: Self::sql_block_zone_map_compare_op(op)?,
                    scalar,
                },
            );
        }

        if let Some(column_index) = self.resolve_schema_column_index(right, schema) {
            let scalar =
                self.sql_block_zone_map_scalar_for_column(left, schema, params, column_index)?;
            return Self::sql_block_zone_map_column_term(
                schema,
                column_index,
                SqlBlockZoneMapPredicateKind::Compare {
                    op: Self::sql_block_zone_map_reverse_compare_op(op)?,
                    scalar,
                },
            );
        }

        None
    }

    fn sql_block_zone_map_pruning_plan(
        &self,
        table_name: &str,
        selection: &Expr,
        schema: &TableSchema,
        params: &[Value],
    ) -> Option<Arc<SqlBlockZoneMapPruningPlan>> {
        let predicates = Self::collect_conjunctive_predicates(selection);
        let mut terms = Vec::with_capacity(predicates.len());

        for predicate in predicates {
            if let Some(term) = self.sql_block_zone_map_in_list_term(&predicate, schema, params) {
                terms.push(term);
                continue;
            }
            if let Some([low_term, high_term]) =
                self.sql_block_zone_map_between_terms(&predicate, schema, params)
            {
                terms.push(low_term);
                terms.push(high_term);
                continue;
            }
            terms.push(self.sql_block_zone_map_binary_term(&predicate, schema, params)?);
        }

        if terms.is_empty() {
            None
        } else {
            Some(Arc::new(SqlBlockZoneMapPruningPlan {
                table_name: table_name.to_string(),
                schema_fingerprint: storage::sql_block_zone_map_schema_fingerprint(schema),
                terms,
            }))
        }
    }

    fn sql_block_zone_map_scan_options(
        &self,
        table_name: &str,
        selection: Option<&Expr>,
        schema: &TableSchema,
        params: &[Value],
        options: StorageScanOptions,
    ) -> StorageScanOptions {
        if !self.sql_block_zone_map_pruning_enabled() {
            return options;
        }
        let Some(selection) = selection else {
            return options;
        };
        match self.sql_block_zone_map_pruning_plan(table_name, selection, schema, params) {
            Some(plan) => options.with_sql_block_zone_map_pruning_plan(plan),
            None => options,
        }
    }

    fn scan_predicate_plan(
        &self,
        selection: &Expr,
        schema: &TableSchema,
        params: &[Value],
    ) -> Option<ScanPredicatePlan> {
        let predicates = Self::collect_conjunctive_predicates(selection);
        let mut terms = Vec::with_capacity(predicates.len());
        let mut column_indices = Vec::with_capacity(predicates.len());

        for predicate in predicates {
            if let Expr::InList {
                expr,
                list,
                negated,
            } = predicate
            {
                if negated {
                    return None;
                }
                let column_index = self.resolve_schema_column_index(&expr, schema)?;
                let mut values = Vec::with_capacity(list.len());
                for item in list {
                    if !Self::scan_predicate_value_expr(&item) {
                        return None;
                    }
                    let value = self.evaluate_value(&item, &[], schema, params).ok()?;
                    let value = Self::coerce_value_to_column_type(
                        value,
                        &schema.columns[column_index].data_type,
                    )
                    .ok()?;
                    if !matches!(value, Value::Null) {
                        values.push(value);
                    }
                }
                let value_slot = Self::scan_predicate_value_slot(&mut column_indices, column_index);
                terms.push(ScanPredicateTerm {
                    value_slot,
                    kind: ScanPredicateTermKind::InList { values },
                });
                continue;
            }

            if let Some([low_term, high_term]) =
                self.scan_predicate_between_terms(&predicate, schema, params, &mut column_indices)
            {
                terms.push(low_term);
                terms.push(high_term);
                continue;
            }

            if let Some(term) =
                self.scan_predicate_like_term(&predicate, schema, params, &mut column_indices)
            {
                terms.push(term);
                continue;
            }

            if let Some(term) =
                self.scan_predicate_or_eq_term(&predicate, schema, params, &mut column_indices)
            {
                terms.push(term);
                continue;
            }

            let Expr::BinaryOp { left, op, right } = predicate else {
                return None;
            };

            let supported_op = matches!(
                op,
                BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Gt
                    | BinaryOperator::Lt
                    | BinaryOperator::GtEq
                    | BinaryOperator::LtEq
            );
            if !supported_op {
                return None;
            }

            if let Some(column_index) = self.resolve_schema_column_index(&left, schema) {
                if !Self::scan_predicate_value_expr(&right) {
                    return None;
                }
                let value = self.evaluate_value(&right, &[], schema, params).ok()?;
                let value = Self::coerce_value_to_column_type(
                    value,
                    &schema.columns[column_index].data_type,
                )
                .ok()?;
                let value_slot = Self::scan_predicate_value_slot(&mut column_indices, column_index);
                terms.push(ScanPredicateTerm {
                    value_slot,
                    kind: ScanPredicateTermKind::Compare { op, value },
                });
                continue;
            }

            if let Some(column_index) = self.resolve_schema_column_index(&right, schema) {
                if !Self::scan_predicate_value_expr(&left) {
                    return None;
                }
                let value = self.evaluate_value(&left, &[], schema, params).ok()?;
                let value = Self::coerce_value_to_column_type(
                    value,
                    &schema.columns[column_index].data_type,
                )
                .ok()?;
                let op = match op {
                    BinaryOperator::Eq => BinaryOperator::Eq,
                    BinaryOperator::NotEq => BinaryOperator::NotEq,
                    BinaryOperator::Gt => BinaryOperator::Lt,
                    BinaryOperator::Lt => BinaryOperator::Gt,
                    BinaryOperator::GtEq => BinaryOperator::LtEq,
                    BinaryOperator::LtEq => BinaryOperator::GtEq,
                    _ => return None,
                };
                let value_slot = Self::scan_predicate_value_slot(&mut column_indices, column_index);
                terms.push(ScanPredicateTerm {
                    value_slot,
                    kind: ScanPredicateTermKind::Compare { op, value },
                });
                continue;
            }

            return None;
        }

        if terms.is_empty() {
            None
        } else {
            Some(ScanPredicatePlan {
                terms,
                column_indices,
            })
        }
    }

    fn decode_predicate_first_filtered_row(
        &self,
        selection: &Expr,
        schema: &TableSchema,
        params: &[Value],
        projection_indices: Option<&[usize]>,
        key_only_scan: bool,
        zero_column_projection: bool,
        pk_index: Option<usize>,
        predicate_plan: &ScanPredicatePlan,
        predicate_values: &mut Vec<Value>,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<Value>>> {
        if let Ok(key_str) = std::str::from_utf8(key) {
            if let Some(row) = self.row_cache.get(key_str) {
                monitor::inc_row_cache_hit();
                return if self.evaluate_expr(selection, &row, schema, params)? {
                    Ok(Some(row))
                } else {
                    Ok(None)
                };
            }
        }

        predicate_plan.decode_values(value, predicate_values)?;
        if !predicate_plan.matches_values(predicate_values) {
            return Ok(None);
        }

        let row = if key_only_scan {
            match Self::row_id_from_key(key) {
                Some(pk_str) => Self::primary_key_row_from_id(schema, pk_index, pk_str),
                None => return Ok(None),
            }
        } else if zero_column_projection {
            Vec::new()
        } else {
            match std::str::from_utf8(key) {
                Ok(key_str) => {
                    if projection_indices.is_none() {
                        Self::decode_row_for_projection(value, None)
                            .map(|row| {
                                self.row_cache.insert(key_str.to_string(), row.clone());
                                row
                            })
                            .map_err(|e| {
                                FusionError::Execution(format!("Data deserialization error: {}", e))
                            })?
                    } else {
                        Self::decode_row_for_projection(value, projection_indices).map_err(|e| {
                            FusionError::Execution(format!("Data deserialization error: {}", e))
                        })?
                    }
                }
                Err(_) => {
                    Self::decode_row_for_projection(value, projection_indices).map_err(|e| {
                        FusionError::Execution(format!("Data deserialization error: {}", e))
                    })?
                }
            }
        };

        Ok(Some(row))
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
        ordered_limit: Option<usize>,
        streaming_order_limit: Option<usize>,
    ) -> Result<(TableSchema, Vec<Vec<Value>>, bool)> {
        match table {
            TableFactor::Derived {
                subquery,
                alias,
                lateral,
            } => {
                if *lateral {
                    return Err(FusionError::Execution(
                        "Unsupported lateral derived table".to_string(),
                    ));
                }
                let (schema, rows) = self
                    .scan_derived_table(subquery, alias, txn, params)
                    .await?;
                let rows = if let Some(selection) = selection {
                    self.filter_rows_with_expr(rows, &schema, selection, params)?
                } else {
                    rows
                };
                Ok((schema, rows, false))
            }
            TableFactor::Table { name, .. } => {
                if let Some(result) = self.evaluate_generate_subscripts(
                    table,
                    &[],
                    &TableSchema::new("generate_subscripts_input".to_string(), vec![]),
                    params,
                ) {
                    let (schema, rows) = result?;
                    let rows = if let Some(selection) = selection {
                        self.filter_rows_with_expr(rows, &schema, selection, params)?
                    } else {
                        rows
                    };
                    return Ok((schema, rows, false));
                }

                let table_name = name.to_string();
                let schema_key = Self::scan_schema_key_for_table(&table_name);

                // Check for view — if no table schema exists, try view expansion
                let schema_bytes_opt = txn.get(schema_key.as_bytes()).await?;
                if schema_bytes_opt.is_none() {
                    let view_key = Self::scan_view_key_for_table(&table_name);
                    if let Some(view_bytes) = txn.get(view_key.as_bytes()).await? {
                        let view_sql = String::from_utf8(view_bytes).map_err(|e| {
                            FusionError::Execution(format!("View decode error: {}", e))
                        })?;
                        let stmts = crate::parser::parse_sql(&view_sql)?;
                        if let Some(sqlparser::ast::Statement::Query(query)) =
                            stmts.into_iter().next()
                        {
                            let result = Box::pin(self.handle_query(&query, txn, params)).await?;
                            if let super::QueryResult::Select { columns, rows } = result {
                                let schema = Self::materialized_query_schema(table_name, columns);
                                return Ok((schema, rows, false));
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

                let projection_indices = Self::projection_indices_for_scan(projection, &schema);
                let zero_column_projection = projection_indices
                    .as_ref()
                    .is_some_and(|indices| indices.is_empty());

                let mut pk_index = None;
                for (i, col) in schema.columns.iter().enumerate() {
                    if col.is_primary {
                        pk_index = Some(i);
                        break;
                    }
                }
                let schema_order_by = if ordered_limit.is_some() {
                    order_by
                } else {
                    None
                };
                // Determine if we can do Key-Only Scan (Projection Pushdown Optimization)
                let mut key_only_scan = false;

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
                            let mut cols = HashSet::with_capacity(schema.columns.len());
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
                            let order_by_is_pk_only = if let Some(ob) = schema_order_by {
                                let mut cols = HashSet::with_capacity(schema.columns.len());
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
                let effective_limit = if let Some(ob) = schema_order_by {
                    let mut is_pk_asc = false;
                    if let sqlparser::ast::OrderByKind::Expressions(exprs) = &ob.kind {
                        if exprs.len() == 1 {
                            let expr = &exprs[0];
                            if expr.options.asc.unwrap_or(true) {
                                if self
                                    .resolve_schema_column_index(&expr.expr, &schema)
                                    .is_some_and(|idx| schema.columns[idx].is_primary)
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

                let row_capacity = effective_limit.or(limit).unwrap_or(0).min(4096);
                let mut rows = Vec::with_capacity(row_capacity);
                let mut index_used = false;
                let mut selection_fully_applied = selection.is_none();
                let mut rows_satisfy_order_by = false;

                // Optimization: Vector Search (HNSW)
                if let Some(order_by) = order_by {
                    if let Some(l) = limit {
                        if l > 0 {
                            if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind
                            {
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
                                        if scan_object_name_eq_ascii(&func.name, "VECTOR_DISTANCE")
                                        {
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
                                                                Self::column_name_from_expr(
                                                                    col_expr,
                                                                )
                                                            {
                                                                vector_search_args = Some((
                                                                    col_name,
                                                                    val_expr.clone(),
                                                                ));
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    if let Some((col_name, query_expr)) = vector_search_args {
                                        if let Ok(idx) =
                                            self.resolve_column_index(&col_name, &schema)
                                        {
                                            if schema.columns[idx].index_type == IndexType::HNSW {
                                                let storage_col_name =
                                                    schema.columns[idx].name.clone();
                                                let query_val = self.evaluate_value(
                                                    &query_expr,
                                                    &[],
                                                    &schema,
                                                    params,
                                                )?;
                                                if let Value::Vector(query_vec) = query_val {
                                                    let idx_name = Self::hnsw_index_name_for_column(
                                                        &table_name,
                                                        &storage_col_name,
                                                    );
                                                    let search_results = self
                                                        .vector_index
                                                        .search(&idx_name, &query_vec, l)?;

                                                    for (id, _dist) in search_results {
                                                        let key = self.routed_data_key_for_row_id(
                                                            &table_name,
                                                            &id,
                                                        );
                                                        if let Some(row) = self.row_cache.get(&key)
                                                        {
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
                            let val = if let Some(pk_idx) = pk_index {
                                Self::coerce_value_to_column_type(
                                    val,
                                    &schema.columns[pk_idx].data_type,
                                )
                                .unwrap_or(Value::Null)
                            } else {
                                val
                            };
                            let row_id = match val {
                                Value::Integer(i) => {
                                    Some(crate::common::encoding::encode_i64_comparable(i))
                                }
                                Value::String(s) => Some(s),
                                Value::Date(days) => Some(
                                    crate::common::encoding::encode_i64_comparable(days as i64),
                                ),
                                Value::Timestamp(micros) => {
                                    Some(crate::common::encoding::encode_i64_comparable(micros))
                                }
                                _ => None,
                            };

                            if let Some(id) = row_id {
                                let key = self.routed_data_key_for_row_id(&table_name, &id);

                                if key_only_scan {
                                    if txn.get(key.as_bytes()).await?.is_some() {
                                        let row =
                                            Self::primary_key_row_from_id(&schema, pk_index, &id);
                                        return Ok((schema, vec![row], false));
                                    }
                                    return Ok((schema, vec![], false));
                                }

                                if let Some(row) = self.row_cache.get(&key) {
                                    monitor::inc_row_cache_hit();
                                    return Ok((schema, vec![row], false));
                                }

                                if let Some(v) = txn.get(key.as_bytes()).await? {
                                    monitor::inc_row_read();
                                    let lookup_projection_indices =
                                        projection_indices.as_ref().map(|indices| {
                                            if let Some(pk_idx) = pk_index {
                                                let mut lookup_indices =
                                                    Vec::with_capacity(indices.len());
                                                for &idx in indices {
                                                    if idx != pk_idx {
                                                        lookup_indices.push(idx);
                                                    }
                                                }
                                                lookup_indices
                                            } else {
                                                indices.clone()
                                            }
                                        });
                                    let row = Self::decode_row_for_projection(
                                        &v,
                                        lookup_projection_indices.as_deref(),
                                    )
                                    .map_err(|e| {
                                        FusionError::Execution(format!(
                                            "Data deserialization error: {}",
                                            e
                                        ))
                                    })?;
                                    let mut row = row;
                                    if let Some(pk_idx) = pk_index {
                                        if row
                                            .get(pk_idx)
                                            .is_some_and(|value| value == &Value::Null)
                                        {
                                            if let Some(value) = Self::primary_key_row_from_id(
                                                &schema, pk_index, &id,
                                            )
                                            .get(pk_idx)
                                            {
                                                row[pk_idx] = value.clone();
                                            }
                                        }
                                    }

                                    if projection_indices.is_none() {
                                        self.row_cache.insert(key, row.clone());
                                    }

                                    return Ok((schema, vec![row], false));
                                }
                                return Ok((schema, vec![], false));
                            }
                        }
                    }
                }

                // Optimization: Primary Key Range Scan
                if let Some(sel) = selection {
                    if !index_used {
                        if let Some(range_plan) = pk_index
                            .map(|pk_idx| {
                                self.primary_key_range_scan_plan(sel, &schema, pk_idx, params)
                            })
                            .transpose()?
                            .flatten()
                        {
                            index_used = true;
                            selection_fully_applied = true;
                            let mut kv_pairs = Vec::new();
                            for table_prefix in self.routed_data_prefixes_for_table(&table_name) {
                                let mut start = table_prefix.as_bytes().to_vec();
                                if let Some(lower) = range_plan.lower_row_id.as_ref() {
                                    start.extend_from_slice(lower.as_bytes());
                                }

                                let mut end = table_prefix.as_bytes().to_vec();
                                if let Some(upper) = range_plan.upper_row_id.as_ref() {
                                    end.extend_from_slice(upper.as_bytes());
                                } else {
                                    end.push(0xFF);
                                }

                                if start >= end {
                                    continue;
                                }

                                let remaining =
                                    limit.map(|limit| limit.saturating_sub(kv_pairs.len()));
                                if remaining == Some(0) {
                                    break;
                                }
                                let scan_options = if remaining.is_none() {
                                    self.bulk_scan_options()
                                } else {
                                    StorageScanOptions::fill_cache()
                                };
                                let scan_options = self.sql_block_zone_map_scan_options(
                                    &table_name,
                                    selection.as_ref(),
                                    &schema,
                                    params,
                                    scan_options,
                                );
                                let mut scanned = txn
                                    .scan_range_with_options(&start, &end, remaining, scan_options)
                                    .await?;
                                kv_pairs.append(&mut scanned);
                                if limit.is_some_and(|limit| kv_pairs.len() >= limit) {
                                    break;
                                }
                            }

                            for (k, v) in kv_pairs {
                                let row = if key_only_scan {
                                    if let Some(pk_str) = Self::row_id_from_key(&k) {
                                        Self::primary_key_row_from_id(&schema, pk_index, pk_str)
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
                                            self.row_cache.insert(key_str.to_string(), row.clone());
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

                if !index_used && selection.is_none() {
                    if let Some(index_plan) = self
                        .try_ordered_secondary_index_scan(
                            &table_name,
                            &schema,
                            txn,
                            schema_order_by,
                            streaming_order_limit,
                        )
                        .await?
                    {
                        let IndexScanPlan {
                            row_ids: _,
                            ordered_row_ids,
                            exact,
                            ordered_topk_counted,
                            covered,
                        } = index_plan;
                        let covered_index_rows = if exact
                            && Self::covered_index_rows_satisfy_projection(
                                covered.as_ref(),
                                projection_indices.as_deref(),
                            ) {
                            covered
                        } else {
                            None
                        };
                        let row_ids_vec = ordered_row_ids.unwrap_or_default();
                        let row_fetch_limit = ordered_limit.or(limit);

                        index_used = true;
                        selection_fully_applied = true;
                        rows_satisfy_order_by = true;

                        for row_id in row_ids_vec {
                            let data_key = self.routed_data_key_for_row_id(&table_name, &row_id);

                            let row = if let Some(row) = covered_index_rows
                                .as_ref()
                                .and_then(|covered| covered.rows.get(&row_id))
                            {
                                if ordered_topk_counted {
                                    monitor::inc_index_ordered_topk_index_only_row();
                                }
                                row.clone()
                            } else if let Some(row) = self.row_cache.get(&data_key) {
                                if ordered_topk_counted {
                                    monitor::inc_index_ordered_topk_base_row_fetch();
                                }
                                monitor::inc_row_cache_hit();
                                row
                            } else if let Some(data_bytes) = txn.get(data_key.as_bytes()).await? {
                                if ordered_topk_counted {
                                    monitor::inc_index_ordered_topk_base_row_fetch();
                                }
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

                            rows.push(row);
                            if let Some(l) = row_fetch_limit {
                                if rows.len() >= l {
                                    break;
                                }
                            }
                        }
                    }
                }

                // Try Index Scan
                if !index_used {
                    if let Some(sel) = selection {
                        let index_candidate_cap = self
                            .stats_guided_index_probe_limit(
                                &table_name,
                                sel,
                                &schema,
                                txn,
                                limit,
                                schema_order_by,
                            )
                            .await?
                            .unwrap_or_else(|| Self::index_candidate_cap(limit, schema_order_by));
                        let index_probe_limit = index_candidate_cap.saturating_add(1);
                        if let Some(index_plan) = self
                            .try_index_scan(
                                sel,
                                &table_name,
                                &schema,
                                txn,
                                params,
                                Some(index_probe_limit),
                                schema_order_by,
                                ordered_limit,
                            )
                            .await?
                        {
                            let IndexScanPlan {
                                row_ids,
                                ordered_row_ids,
                                exact,
                                ordered_topk_counted,
                                covered,
                            } = index_plan;
                            let ordered_index_rows = ordered_row_ids.is_some();
                            let mut covered_index_rows = if exact
                                && Self::covered_index_rows_satisfy_projection(
                                    covered.as_ref(),
                                    projection_indices.as_deref(),
                                ) {
                                covered
                            } else {
                                None
                            };
                            let mut row_ids_vec = if let Some(ordered) = ordered_row_ids {
                                ordered
                            } else {
                                let mut row_ids_vec = Vec::with_capacity(row_ids.len());
                                for row_id in row_ids {
                                    row_ids_vec.push(row_id);
                                }
                                row_ids_vec.sort_unstable();
                                row_ids_vec
                            };

                            if order_by.is_none() && exact {
                                if let Some(l) = limit {
                                    if row_ids_vec.len() > l {
                                        row_ids_vec.truncate(l);
                                    }
                                }
                            } else if ordered_index_rows && exact {
                                if let Some(l) = ordered_limit.or(limit) {
                                    if row_ids_vec.len() > l {
                                        row_ids_vec.truncate(l);
                                    }
                                }
                            }

                            if row_ids_vec.is_empty() && exact {
                                index_used = true;
                                selection_fully_applied = true;
                            } else if Self::should_use_index_plan(
                                row_ids_vec.len(),
                                index_candidate_cap,
                            ) {
                                index_used = true;
                                selection_fully_applied = exact;
                                rows_satisfy_order_by = ordered_index_rows;
                                let row_fetch_limit = if ordered_index_rows {
                                    ordered_limit.or(limit)
                                } else {
                                    limit
                                };

                                if ordered_index_rows
                                    || row_ids_vec.len() <= SMALL_INDEX_FETCH_THRESHOLD
                                {
                                    for row_id in row_ids_vec {
                                        let data_key =
                                            self.routed_data_key_for_row_id(&table_name, &row_id);

                                        let row = if let Some(row) = covered_index_rows
                                            .as_ref()
                                            .and_then(|covered| covered.rows.get(&row_id))
                                        {
                                            if ordered_topk_counted {
                                                monitor::inc_index_ordered_topk_index_only_row();
                                            }
                                            row.clone()
                                        } else if let Some(row) = self.row_cache.get(&data_key) {
                                            if ordered_topk_counted {
                                                monitor::inc_index_ordered_topk_base_row_fetch();
                                            }
                                            monitor::inc_row_cache_hit();
                                            row
                                        } else if key_only_scan {
                                            if ordered_topk_counted {
                                                monitor::inc_index_ordered_topk_index_only_row();
                                            }
                                            Self::primary_key_row_from_id(
                                                &schema, pk_index, &row_id,
                                            )
                                        } else if let Some(data_bytes) =
                                            txn.get(data_key.as_bytes()).await?
                                        {
                                            if ordered_topk_counted {
                                                monitor::inc_index_ordered_topk_base_row_fetch();
                                            }
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

                                        if !exact
                                            && !self.evaluate_expr(sel, &row, &schema, params)?
                                        {
                                            continue;
                                        }

                                        rows.push(row);
                                        if let Some(l) = row_fetch_limit {
                                            if rows.len() >= l {
                                                break;
                                            }
                                        }
                                    }
                                } else {
                                    let txn_ref = &*txn;

                                    let table_name_for_stream =
                                        Arc::<str>::from(table_name.as_str());
                                    let projection_indices_for_stream =
                                        projection_indices.clone().map(Arc::<[usize]>::from);
                                    let covered_rows_for_stream =
                                        covered_index_rows.take().map(Arc::new);
                                    let schema_width = schema.columns.len();
                                    let pk_type_upper_for_stream = if key_only_scan {
                                        pk_index.and_then(|idx| {
                                            schema.columns.get(idx).map(|column| {
                                                Arc::<str>::from(
                                                    column
                                                        .data_type
                                                        .to_ascii_uppercase()
                                                        .into_boxed_str(),
                                                )
                                            })
                                        })
                                    } else {
                                        None
                                    };
                                    let executor_for_stream = self;

                                    let fetch_stream = futures::stream::iter(row_ids_vec)
                                        .map(|row_id| {
                                            let table_name = table_name_for_stream.clone();
                                            let projection_indices =
                                                projection_indices_for_stream.clone();
                                            let covered_rows = covered_rows_for_stream.clone();
                                            let pk_type_upper = pk_type_upper_for_stream.clone();
                                            let executor = executor_for_stream;

                                            async move {
                                                let data_key = executor.routed_data_key_for_row_id(
                                                    table_name.as_ref(),
                                                    &row_id,
                                                );

                                                if let Some(row) = covered_rows
                                                    .as_ref()
                                                    .and_then(|covered| covered.rows.get(&row_id))
                                                {
                                                    return Ok::<_, FusionError>(Some((
                                                        row_id,
                                                        row.clone(),
                                                        false,
                                                        false,
                                                        false,
                                                    )));
                                                }

                                                if let Some(row) = executor.row_cache.get(&data_key)
                                                {
                                                    return Ok::<_, FusionError>(Some((
                                                        row_id, row, true, false, false,
                                                    )));
                                                }

                                                if key_only_scan {
                                                    let r = Self::primary_key_row_from_parts(
                                                        schema_width,
                                                        pk_index,
                                                        pk_type_upper.as_deref(),
                                                        &row_id,
                                                    );
                                                    return Ok(Some((
                                                        row_id, r, false, false, false,
                                                    )));
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
                                                    let data_key = self.routed_data_key_for_row_id(
                                                        &table_name,
                                                        &row_id,
                                                    );
                                                    self.row_cache.insert(data_key, row.clone());
                                                }
                                            } else if from_cache {
                                                monitor::inc_row_cache_hit();
                                            }

                                            if !exact
                                                && !self
                                                    .evaluate_expr(sel, &row, &schema, params)?
                                            {
                                                continue;
                                            }

                                            rows.push(row);
                                            if let Some(l) = row_fetch_limit {
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
                    if let (Some(order_by), Some(window)) = (order_by, streaming_order_limit) {
                        if window == 0 {
                            return Ok((schema, Vec::new(), true));
                        }
                        if limit.is_none()
                            && OrderTopKScanVisitor::supports_order_by(order_by, &schema, self)
                        {
                            let mut visitor = OrderTopKScanVisitor {
                                executor: self,
                                selection: selection.as_ref(),
                                order_by,
                                schema: &schema,
                                params,
                                projection_indices: projection_indices.as_deref(),
                                key_only_scan,
                                zero_column_projection,
                                pk_index,
                                window,
                                sequence: 0,
                                entries: BinaryHeap::with_capacity(window.saturating_add(1)),
                                error: None,
                            };
                            self.scan_routed_data_prefixes_for_each_with_options(
                                &table_name,
                                txn,
                                None,
                                &mut visitor,
                                self.sql_block_zone_map_scan_options(
                                    &table_name,
                                    selection.as_ref(),
                                    &schema,
                                    params,
                                    self.bulk_scan_options(),
                                ),
                            )
                            .await?;
                            let OrderTopKScanVisitor { error, entries, .. } = visitor;
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok((
                                schema,
                                OrderTopKScanVisitor::into_sorted_rows(entries),
                                true,
                            ));
                        }
                    }

                    // BENCHPROD-444: when a LIMIT is pushed into a filtered scan, stream the
                    // routed prefixes through a visitor so the storage layer stops reading once
                    // enough matches are found, instead of materializing every key/value pair.
                    if let (Some(sel), Some(limit_value)) = (selection.as_ref(), limit) {
                        if limit_value == 0 {
                            return Ok((schema, Vec::new(), rows_satisfy_order_by));
                        }

                        let predicate_plan = self.scan_predicate_plan(sel, &schema, params);
                        let predicate_values = predicate_plan
                            .as_ref()
                            .map_or_else(Vec::new, ScanPredicatePlan::scratch_values);
                        let mut visitor = FilteredScanVisitor {
                            executor: self,
                            selection: sel,
                            schema: &schema,
                            params,
                            projection_indices: projection_indices.as_deref(),
                            key_only_scan,
                            zero_column_projection,
                            pk_index,
                            predicate_plan,
                            predicate_values,
                            limit: Some(limit_value),
                            rows: Vec::with_capacity(limit_value.min(4096)),
                            error: None,
                        };
                        // limit=None: the driver's limit counts visited pairs, not matched rows;
                        // the visitor stops itself once it has collected `limit_value` matches.
                        self.scan_routed_data_prefixes_for_each_with_options(
                            &table_name,
                            txn,
                            None,
                            &mut visitor,
                            self.sql_block_zone_map_scan_options(
                                &table_name,
                                Some(sel),
                                &schema,
                                params,
                                self.bulk_scan_options(),
                            ),
                        )
                        .await?;
                        let FilteredScanVisitor {
                            rows: streamed,
                            error,
                            ..
                        } = visitor;
                        if let Some(err) = error {
                            return Err(err);
                        }
                        return Ok((schema, streamed, rows_satisfy_order_by));
                    }

                    let scan_limit = if selection.is_none() {
                        effective_limit
                    } else {
                        None
                    };

                    let scan_options = if scan_limit.is_none() {
                        self.bulk_scan_options()
                    } else {
                        StorageScanOptions::fill_cache()
                    };
                    let scan_options = self.sql_block_zone_map_scan_options(
                        &table_name,
                        selection.as_ref(),
                        &schema,
                        params,
                        scan_options,
                    );
                    let kv_pairs = self
                        .scan_routed_data_prefixes_for_table_with_options(
                            &table_name,
                            txn,
                            scan_limit,
                            scan_options,
                        )
                        .await?;

                    if let (Some(sel), None) = (selection.as_ref(), limit) {
                        if let Some(predicate_plan) = self.scan_predicate_plan(sel, &schema, params)
                        {
                            if kv_pairs.len() > 1000 {
                                use rayon::iter::IntoParallelIterator;
                                use rayon::iter::ParallelIterator;

                                let projection_indices = projection_indices.as_deref();
                                let filtered: Vec<Result<Option<Vec<Value>>>> = kv_pairs
                                    .into_par_iter()
                                    .map(|(k, v)| {
                                        let mut predicate_values = predicate_plan.scratch_values();
                                        self.decode_predicate_first_filtered_row(
                                            sel,
                                            &schema,
                                            params,
                                            projection_indices,
                                            key_only_scan,
                                            zero_column_projection,
                                            pk_index,
                                            &predicate_plan,
                                            &mut predicate_values,
                                            &k,
                                            &v,
                                        )
                                    })
                                    .collect();

                                rows = Vec::with_capacity(filtered.len().min(4096));
                                for item in filtered {
                                    if let Some(row) = item? {
                                        rows.push(row);
                                    }
                                }
                            } else {
                                let mut predicate_values = predicate_plan.scratch_values();
                                for (k, v) in kv_pairs {
                                    if let Some(row) = self.decode_predicate_first_filtered_row(
                                        sel,
                                        &schema,
                                        params,
                                        projection_indices.as_deref(),
                                        key_only_scan,
                                        zero_column_projection,
                                        pk_index,
                                        &predicate_plan,
                                        &mut predicate_values,
                                        &k,
                                        &v,
                                    )? {
                                        rows.push(row);
                                    }
                                }
                            }

                            return Ok((schema, rows, rows_satisfy_order_by));
                        }
                    }

                    // BENCHPROD-440: For a large unindexed full scan WITH a selection and
                    // no pushed limit (so the serial early-break isn't in play), decode and
                    // filter rows in parallel via rayon. `par_iter().filter_map().collect()`
                    // preserves input order, so this yields the same rows in the same order
                    // as the serial path below. Skip the key-only / zero-column projections,
                    // which take cheaper decode paths, and only engage above a threshold.
                    let parallel_full_scan = selection.is_some()
                        && limit.is_none()
                        && !key_only_scan
                        && !zero_column_projection
                        && kv_pairs.len() > 1000;

                    if parallel_full_scan {
                        use rayon::iter::IntoParallelIterator;
                        use rayon::iter::ParallelIterator;

                        let sel = selection.as_ref().unwrap();
                        let projection_indices = projection_indices.as_deref();
                        let filtered: Vec<Result<Option<Vec<Value>>>> = kv_pairs
                            .into_par_iter()
                            .map(|(k, v)| {
                                let row = match std::str::from_utf8(&k) {
                                    Ok(key_str) => {
                                        if let Some(row) = self.row_cache.get(key_str) {
                                            monitor::inc_row_cache_hit();
                                            Ok(row)
                                        } else if projection_indices.is_none() {
                                            Self::decode_row_for_projection(&v, None)
                                                .map(|row| {
                                                    self.row_cache
                                                        .insert(key_str.to_string(), row.clone());
                                                    row
                                                })
                                                .map_err(|e| {
                                                    FusionError::Execution(format!(
                                                        "Data deserialization error: {}",
                                                        e
                                                    ))
                                                })
                                        } else {
                                            Self::decode_row_for_projection(&v, projection_indices)
                                                .map_err(|e| {
                                                    FusionError::Execution(format!(
                                                        "Data deserialization error: {}",
                                                        e
                                                    ))
                                                })
                                        }
                                    }
                                    Err(_) => {
                                        Self::decode_row_for_projection(&v, projection_indices)
                                            .map_err(|e| {
                                                FusionError::Execution(format!(
                                                    "Data deserialization error: {}",
                                                    e
                                                ))
                                            })
                                    }
                                }?;

                                if self.evaluate_expr(sel, &row, &schema, params)? {
                                    Ok(Some(row))
                                } else {
                                    Ok(None)
                                }
                            })
                            .collect();

                        rows = Vec::with_capacity(filtered.len().min(4096));
                        for item in filtered {
                            if let Some(row) = item? {
                                rows.push(row);
                            }
                        }

                        return Ok((schema, rows, rows_satisfy_order_by));
                    }

                    for (k, v) in kv_pairs {
                        let row_res = if key_only_scan {
                            if let Some(pk_str) = Self::row_id_from_key(&k) {
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
                                Err(_) => Self::decode_row_for_projection(
                                    &v,
                                    projection_indices.as_deref(),
                                )
                                .ok(),
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

                Ok((schema, rows, rows_satisfy_order_by))
            }
            _ => Err(FusionError::Execution(
                "Unsupported table factor".to_string(),
            )),
        }
    }
}

struct OrderTopKValue {
    value: Value,
    asc: bool,
}

struct OrderTopKEntry {
    order_values: Vec<OrderTopKValue>,
    sequence: usize,
    row: Vec<Value>,
}

impl PartialEq for OrderTopKEntry {
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence && self.cmp(other) == Ordering::Equal
    }
}

impl Eq for OrderTopKEntry {}

impl PartialOrd for OrderTopKEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderTopKEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        for (left, right) in self.order_values.iter().zip(other.order_values.iter()) {
            let ordering = left.value.compare(&right.value);
            let ordering = if left.asc {
                ordering
            } else {
                ordering.reverse()
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }

        self.order_values
            .len()
            .cmp(&other.order_values.len())
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

struct OrderTopKScanVisitor<'a> {
    executor: &'a Executor,
    selection: Option<&'a Expr>,
    order_by: &'a sqlparser::ast::OrderBy,
    schema: &'a TableSchema,
    params: &'a [Value],
    projection_indices: Option<&'a [usize]>,
    key_only_scan: bool,
    zero_column_projection: bool,
    pk_index: Option<usize>,
    window: usize,
    sequence: usize,
    entries: BinaryHeap<OrderTopKEntry>,
    error: Option<FusionError>,
}

impl OrderTopKScanVisitor<'_> {
    fn supports_order_by(
        order_by: &sqlparser::ast::OrderBy,
        schema: &TableSchema,
        executor: &Executor,
    ) -> bool {
        let OrderByKind::Expressions(exprs) = &order_by.kind else {
            return false;
        };
        !exprs.is_empty()
            && exprs.iter().all(|order_expr| {
                Executor::order_limit_column_name(&order_expr.expr)
                    .and_then(|name| executor.resolve_column_index(&name, schema).ok())
                    .is_some()
            })
    }

    fn order_values_for_row(&self, row: &[Value]) -> Vec<OrderTopKValue> {
        let OrderByKind::Expressions(exprs) = &self.order_by.kind else {
            return Vec::new();
        };

        let mut values = Vec::with_capacity(exprs.len());
        for order_expr in exprs {
            values.push(OrderTopKValue {
                value: self
                    .executor
                    .evaluate_value(&order_expr.expr, row, self.schema, self.params)
                    .unwrap_or(Value::Null),
                asc: order_expr.options.asc.unwrap_or(true),
            });
        }
        values
    }

    fn push_row(&mut self, row: Vec<Value>) {
        let entry = OrderTopKEntry {
            order_values: self.order_values_for_row(&row),
            sequence: self.sequence,
            row,
        };
        self.sequence = self.sequence.saturating_add(1);

        if self.entries.len() < self.window {
            self.entries.push(entry);
            return;
        }

        if let Some(mut worst) = self.entries.peek_mut() {
            if entry.cmp(&*worst) == Ordering::Less {
                *worst = entry;
            }
        }
    }

    fn into_sorted_rows(entries: BinaryHeap<OrderTopKEntry>) -> Vec<Vec<Value>> {
        let mut entries = entries.into_vec();
        entries.sort_by(|left, right| left.cmp(right));
        let mut rows = Vec::with_capacity(entries.len());
        for entry in entries {
            rows.push(entry.row);
        }
        rows
    }
}

impl ScanVisitor for OrderTopKScanVisitor<'_> {
    fn visit(&mut self, key: &[u8], value: &[u8]) -> bool {
        let row = if self.key_only_scan {
            match Executor::row_id_from_key(key) {
                Some(pk_str) => {
                    Executor::primary_key_row_from_id(self.schema, self.pk_index, pk_str)
                }
                None => return true,
            }
        } else if self.zero_column_projection {
            Vec::new()
        } else {
            let decoded = match std::str::from_utf8(key) {
                Ok(key_str) => {
                    if let Some(row) = self.executor.row_cache.get(key_str) {
                        monitor::inc_row_cache_hit();
                        Some(row)
                    } else if self.projection_indices.is_none() {
                        Executor::decode_row_for_projection(value, None)
                            .ok()
                            .map(|row| {
                                self.executor
                                    .row_cache
                                    .insert(key_str.to_string(), row.clone());
                                row
                            })
                    } else {
                        Executor::decode_row_for_projection(value, self.projection_indices).ok()
                    }
                }
                Err(_) => Executor::decode_row_for_projection(value, self.projection_indices).ok(),
            };
            match decoded {
                Some(row) => row,
                None => return true,
            }
        };

        if let Some(selection) = self.selection {
            match self
                .executor
                .evaluate_expr(selection, &row, self.schema, self.params)
            {
                Ok(true) => {}
                Ok(false) => return true,
                Err(e) => {
                    self.error = Some(e);
                    return false;
                }
            }
        }

        self.push_row(row);
        true
    }
}

/// Streams a filtered full-table scan through a storage visitor. Decode logic mirrors the serial
/// full-scan loop in `scan_single_table` so the streamed result is row-for-row identical; the
/// difference is that the storage layer does not materialize the full key/value set first.
struct FilteredScanVisitor<'a> {
    executor: &'a Executor,
    selection: &'a Expr,
    schema: &'a TableSchema,
    params: &'a [Value],
    projection_indices: Option<&'a [usize]>,
    key_only_scan: bool,
    zero_column_projection: bool,
    pk_index: Option<usize>,
    predicate_plan: Option<ScanPredicatePlan>,
    predicate_values: Vec<Value>,
    limit: Option<usize>,
    rows: Vec<Vec<Value>>,
    error: Option<FusionError>,
}

impl FilteredScanVisitor<'_> {
    fn decode_output_row(&self, key: &[u8], value: &[u8]) -> Option<Vec<Value>> {
        if self.key_only_scan {
            match Executor::row_id_from_key(key) {
                Some(pk_str) => Some(Executor::primary_key_row_from_id(
                    self.schema,
                    self.pk_index,
                    pk_str,
                )),
                None => None,
            }
        } else if self.zero_column_projection {
            Some(Vec::new())
        } else {
            match std::str::from_utf8(key) {
                Ok(key_str) => {
                    if let Some(row) = self.executor.row_cache.get(key_str) {
                        monitor::inc_row_cache_hit();
                        Some(row)
                    } else if self.projection_indices.is_none() {
                        Executor::decode_row_for_projection(value, None)
                            .ok()
                            .map(|row| {
                                self.executor
                                    .row_cache
                                    .insert(key_str.to_string(), row.clone());
                                row
                            })
                    } else {
                        Executor::decode_row_for_projection(value, self.projection_indices).ok()
                    }
                }
                Err(_) => Executor::decode_row_for_projection(value, self.projection_indices).ok(),
            }
        }
    }

    fn push_matched_row(&mut self, row: Vec<Value>) -> bool {
        self.rows.push(row);
        self.limit.map_or(true, |limit| self.rows.len() < limit)
    }
}

impl ScanVisitor for FilteredScanVisitor<'_> {
    fn visit(&mut self, key: &[u8], value: &[u8]) -> bool {
        if let Some(predicate_plan) = self.predicate_plan.as_ref() {
            return match self.executor.decode_predicate_first_filtered_row(
                self.selection,
                self.schema,
                self.params,
                self.projection_indices,
                self.key_only_scan,
                self.zero_column_projection,
                self.pk_index,
                predicate_plan,
                &mut self.predicate_values,
                key,
                value,
            ) {
                Ok(Some(row)) => self.push_matched_row(row),
                Ok(None) => true,
                Err(e) => {
                    self.error = Some(e);
                    false
                }
            };
        }

        let Some(row) = self.decode_output_row(key, value) else {
            return true;
        };

        match self
            .executor
            .evaluate_expr(self.selection, &row, self.schema, self.params)
        {
            Ok(true) => self.push_matched_row(row),
            Ok(false) => true,
            Err(e) => {
                self.error = Some(e);
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{scan_object_name_eq_ascii, Executor};
    use crate::catalog::{Column, IndexType, TableSchema};
    use crate::execution::SQL_BLOCK_ZONE_MAP_PRUNING_ENABLED;
    use crate::parser::parse_sql;
    use crate::storage::{
        SqlBlockZoneMapComparisonOp, SqlBlockZoneMapPredicateKind, StorageScanOptions,
        SQL_BLOCK_ZONE_MAP_TYPE_BOOLEAN, SQL_BLOCK_ZONE_MAP_TYPE_INTEGER,
    };
    use sqlparser::ast::{
        Expr, Ident, ObjectName, ObjectNamePart, ObjectNamePartFunction, SetExpr, Statement,
    };
    use std::sync::Arc;

    fn zone_map_scan_schema() -> TableSchema {
        TableSchema {
            name: "metrics".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    is_primary: true,
                    is_indexed: true,
                    index_type: IndexType::BTree,
                    default_value: None,
                    is_nullable: false,
                    is_unique: true,
                    check_expr: None,
                },
                Column {
                    name: "bucket".to_string(),
                    data_type: "BIGINT".to_string(),
                    is_primary: false,
                    is_indexed: false,
                    index_type: IndexType::None,
                    default_value: None,
                    is_nullable: false,
                    is_unique: false,
                    check_expr: None,
                },
                Column {
                    name: "active".to_string(),
                    data_type: "BOOLEAN".to_string(),
                    is_primary: false,
                    is_indexed: false,
                    index_type: IndexType::None,
                    default_value: None,
                    is_nullable: false,
                    is_unique: false,
                    check_expr: None,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "TEXT".to_string(),
                    is_primary: false,
                    is_indexed: false,
                    index_type: IndexType::None,
                    default_value: None,
                    is_nullable: false,
                    is_unique: false,
                    check_expr: None,
                },
                Column {
                    name: "score".to_string(),
                    data_type: "DOUBLE".to_string(),
                    is_primary: false,
                    is_indexed: false,
                    index_type: IndexType::None,
                    default_value: None,
                    is_nullable: false,
                    is_unique: false,
                    check_expr: None,
                },
            ],
        }
    }

    fn scan_test_executor() -> Executor {
        let wal_path = format!(
            "test_sql_block_zone_map_scan_plan_{}.wal",
            uuid::Uuid::new_v4()
        );
        let storage: Arc<dyn crate::storage::Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage);
        let _ = std::fs::remove_file(wal_path);
        executor
    }

    fn select_selection(sql: &str) -> Expr {
        let statements = parse_sql(sql).unwrap();
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("expected one SELECT statement");
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected SELECT body");
        };
        select.selection.clone().expect("selection")
    }

    #[test]
    fn scan_data_key_for_row_id_preallocates_exact_key() {
        let key = Executor::scan_data_key_for_row_id("lineitem", "00000042");

        assert_eq!(key, "data:lineitem:00000042");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn scan_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = Executor::scan_data_prefix_for_table("lineitem");

        assert_eq!(prefix, "data:lineitem:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn scan_schema_key_for_table_preallocates_exact_key() {
        let key = Executor::scan_schema_key_for_table("lineitem");

        assert_eq!(key, "schema:lineitem");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn scan_view_key_for_table_preallocates_exact_key() {
        let key = Executor::scan_view_key_for_table("revenue0");

        assert_eq!(key, "view:revenue0");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn scan_view_wrapped_query_sql_preallocates_exact_sql() {
        let sql =
            Executor::scan_view_wrapped_query_sql("SELECT id, name FROM vt WHERE score >= 80");

        assert_eq!(
            sql,
            "SELECT * FROM (SELECT id, name FROM vt WHERE score >= 80) AS _v"
        );
        assert!(sql.capacity() >= sql.len());
    }

    #[test]
    fn sql_block_zone_map_plan_accepts_safe_compare_between_and_in_terms() {
        let executor = scan_test_executor();
        let schema = zone_map_scan_schema();
        let selection = select_selection(
            "SELECT * FROM metrics \
             WHERE id = 7 AND active = true AND bucket BETWEEN 10 AND 20 AND id IN (1, 2, 2)",
        );

        let plan = executor
            .sql_block_zone_map_pruning_plan("metrics", &selection, &schema, &[])
            .expect("zone-map plan");

        assert_eq!(plan.table_name, "metrics");
        assert_eq!(plan.terms.len(), 5);
        assert!(plan.schema_fingerprint != 0);

        let id_eq = &plan.terms[0];
        assert_eq!(id_eq.column_index, 0);
        assert_eq!(id_eq.column_name, "id");
        assert_eq!(id_eq.type_tag, SQL_BLOCK_ZONE_MAP_TYPE_INTEGER);
        assert!(matches!(
            id_eq.kind,
            SqlBlockZoneMapPredicateKind::Compare {
                op: SqlBlockZoneMapComparisonOp::Eq,
                scalar: 7
            }
        ));

        let active_eq = &plan.terms[1];
        assert_eq!(active_eq.column_index, 2);
        assert_eq!(active_eq.type_tag, SQL_BLOCK_ZONE_MAP_TYPE_BOOLEAN);
        assert!(matches!(
            active_eq.kind,
            SqlBlockZoneMapPredicateKind::Compare {
                op: SqlBlockZoneMapComparisonOp::Eq,
                scalar: 1
            }
        ));

        assert!(matches!(
            plan.terms[2].kind,
            SqlBlockZoneMapPredicateKind::Compare {
                op: SqlBlockZoneMapComparisonOp::GtEq,
                scalar: 10
            }
        ));
        assert!(matches!(
            plan.terms[3].kind,
            SqlBlockZoneMapPredicateKind::Compare {
                op: SqlBlockZoneMapComparisonOp::LtEq,
                scalar: 20
            }
        ));
        assert!(matches!(
            &plan.terms[4].kind,
            SqlBlockZoneMapPredicateKind::InList { scalars } if scalars == &vec![1, 2]
        ));
    }

    #[test]
    fn sql_block_zone_map_plan_rejects_unsafe_predicates_and_types() {
        let executor = scan_test_executor();
        let schema = zone_map_scan_schema();

        for sql in [
            "SELECT * FROM metrics WHERE id <> 7",
            "SELECT * FROM metrics WHERE name LIKE 'a%'",
            "SELECT * FROM metrics WHERE id = 1 OR id = 2",
            "SELECT * FROM metrics WHERE id NOT IN (1, 2)",
            "SELECT * FROM metrics WHERE id = NULL",
            "SELECT * FROM metrics WHERE name = 'west'",
            "SELECT * FROM metrics WHERE score > 1.5",
        ] {
            let selection = select_selection(sql);
            assert!(
                executor
                    .sql_block_zone_map_pruning_plan("metrics", &selection, &schema, &[])
                    .is_none(),
                "expected no zone-map plan for {sql}"
            );
        }
    }

    #[test]
    fn sql_block_zone_map_scan_options_attach_plan_without_changing_fill_cache() {
        let executor = scan_test_executor();
        let schema = zone_map_scan_schema();
        let selection = select_selection("SELECT * FROM metrics WHERE bucket >= 10");

        let options = executor.sql_block_zone_map_scan_options(
            "metrics",
            Some(&selection),
            &schema,
            &[],
            StorageScanOptions::no_fill_cache(),
        );

        assert!(!options.fill_cache);
        assert!(options.sql_block_zone_map_pruning);
        assert!(options.sql_block_zone_map_pruning_enabled());
        assert!(options.sql_block_zone_map_pruning_plan.is_some());
    }

    #[tokio::test]
    async fn sql_block_zone_map_scan_options_respect_scoped_disable() {
        let executor = scan_test_executor();
        let schema = zone_map_scan_schema();
        let selection = select_selection("SELECT * FROM metrics WHERE bucket >= 10");

        let options = SQL_BLOCK_ZONE_MAP_PRUNING_ENABLED
            .scope(false, async {
                executor.sql_block_zone_map_scan_options(
                    "metrics",
                    Some(&selection),
                    &schema,
                    &[],
                    StorageScanOptions::no_fill_cache(),
                )
            })
            .await;

        assert!(!options.fill_cache);
        assert!(!options.sql_block_zone_map_pruning);
        assert!(!options.sql_block_zone_map_pruning_enabled());
        assert!(options.sql_block_zone_map_pruning_plan.is_none());
    }

    #[test]
    fn scan_object_name_eq_ascii_matches_single_part_names_without_display_string() {
        let name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            "Generate_SubScripts",
        ))]);
        let qualified = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("pg_catalog")),
            ObjectNamePart::Identifier(Ident::new("generate_subscripts")),
        ]);
        let function_part = ObjectName(vec![ObjectNamePart::Function(ObjectNamePartFunction {
            name: Ident::new("VECTOR_DISTANCE"),
            args: Vec::new(),
        })]);

        assert!(scan_object_name_eq_ascii(&name, "generate_subscripts"));
        assert!(scan_object_name_eq_ascii(&function_part, "vector_distance"));
        assert!(!scan_object_name_eq_ascii(&name, "generate_series"));
        assert!(!scan_object_name_eq_ascii(
            &qualified,
            "generate_subscripts"
        ));
    }
}
