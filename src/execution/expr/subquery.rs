use crate::catalog::TableSchema;
use crate::common::{Result, Value};
use crate::storage::Transaction;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName,
    ObjectNamePart, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins, UnaryOperator,
    Value as SqlValue,
};
use std::collections::{HashMap, HashSet};
use std::fmt::Write;

use super::super::{Executor, QueryResult};
use super::SqlTruth;

struct SubqueryLocalScope {
    schema: crate::catalog::TableSchema,
}

#[derive(Clone)]
struct ExistsMembershipPlan {
    table_factor: TableFactor,
    column_name: String,
    probe_expr: Expr,
}

struct ExistsMembershipCache {
    plan: ExistsMembershipPlan,
    values: HashSet<Value>,
}

#[derive(Clone)]
enum ExistsJoinSide {
    Left,
    Right,
}

#[derive(Clone)]
struct ExistsJoinMembershipPlan {
    left_table_factor: TableFactor,
    right_table_factor: TableFactor,
    left_join_column: String,
    right_join_column: String,
    filter_side: ExistsJoinSide,
    filter_column: String,
    filter_expr: Expr,
    probe_side: ExistsJoinSide,
    probe_column: String,
    probe_expr: Expr,
}

struct ExistsJoinMembershipCache {
    plan: ExistsJoinMembershipPlan,
    values: HashSet<Value>,
}

struct InSubqueryMembershipCache {
    values: HashSet<Value>,
    contains_null: bool,
}

enum ExistsCache {
    Single(ExistsMembershipCache),
    Join(ExistsJoinMembershipCache),
    InSubquery(InSubqueryMembershipCache),
}

fn subquery_schema_key_for_table(table_name: &str) -> String {
    let mut key = String::with_capacity("schema:".len() + table_name.len());
    key.push_str("schema:");
    key.push_str(table_name);
    key
}

fn subquery_derived_cache_name_for_alias(alias_name: &str) -> String {
    let mut name = String::with_capacity("derived:".len() + alias_name.len());
    name.push_str("derived:");
    name.push_str(alias_name);
    name
}

fn subquery_object_name_display_capacity(name: &ObjectName) -> usize {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => {
                ident.value.len() + usize::from(ident.quote_style.is_some()) * 2
            }
            ObjectNamePart::Function(function) => function.name.value.len() + 2,
        })
        .sum::<usize>()
        + name.0.len().saturating_sub(1)
}

fn subquery_table_cache_name_for_factor(name: &ObjectName, alias_name: &str) -> String {
    let mut cache_name = String::with_capacity(
        "table:".len() + subquery_object_name_display_capacity(name) + 1 + alias_name.len(),
    );
    cache_name.push_str("table:");
    write!(&mut cache_name, "{name}").expect("writing to String cannot fail");
    cache_name.push(':');
    cache_name.push_str(alias_name);
    cache_name
}

fn subquery_push_ascii_lowercase(output: &mut String, value: &str) {
    let mut chunk_start = 0;
    for (index, byte) in value.bytes().enumerate() {
        if byte.is_ascii_uppercase() {
            if chunk_start < index {
                output.push_str(&value[chunk_start..index]);
            }
            output.push((byte + b'a' - b'A') as char);
            chunk_start = index + 1;
        }
    }
    if chunk_start < value.len() {
        output.push_str(&value[chunk_start..]);
    }
}

fn subquery_exists_membership_cache_key(
    table_factor_cache_name: &str,
    column_name: &str,
) -> String {
    let mut key = String::with_capacity(table_factor_cache_name.len() + 1 + column_name.len());
    key.push_str(table_factor_cache_name);
    key.push('|');
    subquery_push_ascii_lowercase(&mut key, column_name);
    key
}

fn subquery_exists_join_membership_cache_key(
    left_table_factor_cache_name: &str,
    right_table_factor_cache_name: &str,
    left_join_column: &str,
    right_join_column: &str,
    filter_column: &str,
    filter_expr: &Expr,
    probe_column: &str,
) -> String {
    let mut key = String::with_capacity(
        left_table_factor_cache_name.len()
            + right_table_factor_cache_name.len()
            + left_join_column.len()
            + right_join_column.len()
            + filter_column.len()
            + probe_column.len()
            + 6,
    );
    key.push_str(left_table_factor_cache_name);
    key.push('|');
    key.push_str(right_table_factor_cache_name);
    key.push('|');
    subquery_push_ascii_lowercase(&mut key, left_join_column);
    key.push('|');
    subquery_push_ascii_lowercase(&mut key, right_join_column);
    key.push('|');
    subquery_push_ascii_lowercase(&mut key, filter_column);
    key.push('|');
    write!(&mut key, "{filter_expr}").expect("writing to String cannot fail");
    key.push('|');
    subquery_push_ascii_lowercase(&mut key, probe_column);
    key
}

fn subquery_in_membership_cache_key(
    probe_expr: &Expr,
    subquery: &Query,
    column_type: &str,
) -> String {
    let mut key = String::with_capacity(column_type.len() + 2);
    key.push_str("in|");
    key.push_str(column_type);
    key.push('|');
    write!(&mut key, "{probe_expr}|{subquery}").expect("writing to String cannot fail");
    key
}

impl Executor {
    pub(crate) async fn materialize_subqueries(
        &self,
        expr: &Expr,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Expr> {
        self.materialize_subqueries_with_outer(expr, None, None, txn, params)
            .await
    }

    pub(crate) async fn materialize_subqueries_with_outer(
        &self,
        expr: &Expr,
        outer_row: Option<&[Value]>,
        outer_schema: Option<&crate::catalog::TableSchema>,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Expr> {
        match expr {
            Expr::InSubquery {
                expr: inner_expr,
                subquery,
                negated,
            } => {
                let mut bound_subquery = subquery.clone();
                if let (Some(row), Some(schema)) = (outer_row, outer_schema) {
                    Box::pin(self.bind_outer_references_in_query(
                        &mut bound_subquery,
                        row,
                        schema,
                        txn,
                        params,
                    ))
                    .await?;
                }
                let result = Box::pin(self.handle_query(&bound_subquery, txn, params)).await?;
                let values = match result {
                    QueryResult::Select { rows, .. } => {
                        let mut values = Vec::with_capacity(rows.len());
                        for row in rows {
                            if let Some(value) = row.into_iter().next() {
                                values.push(self.fusion_value_to_sql_expr(&value));
                            }
                        }
                        values
                    }
                    _ => vec![],
                };
                Ok(Expr::InList {
                    expr: inner_expr.clone(),
                    list: values,
                    negated: *negated,
                })
            }
            Expr::Subquery(subquery) => {
                let mut bound_subquery = subquery.clone();
                if let (Some(row), Some(schema)) = (outer_row, outer_schema) {
                    Box::pin(self.bind_outer_references_in_query(
                        &mut bound_subquery,
                        row,
                        schema,
                        txn,
                        params,
                    ))
                    .await?;
                }
                let result = Box::pin(self.handle_query(&bound_subquery, txn, params)).await?;
                match result {
                    QueryResult::Select { rows, .. } => {
                        if let Some(row) = rows.into_iter().next() {
                            if let Some(val) = row.into_iter().next() {
                                Ok(self.fusion_value_to_sql_expr(&val))
                            } else {
                                Ok(Expr::Value(sqlparser::ast::ValueWithSpan {
                                    value: SqlValue::Null,
                                    span: sqlparser::tokenizer::Span::empty(),
                                }))
                            }
                        } else {
                            Ok(Expr::Value(sqlparser::ast::ValueWithSpan {
                                value: SqlValue::Null,
                                span: sqlparser::tokenizer::Span::empty(),
                            }))
                        }
                    }
                    _ => Ok(Expr::Value(sqlparser::ast::ValueWithSpan {
                        value: SqlValue::Null,
                        span: sqlparser::tokenizer::Span::empty(),
                    })),
                }
            }
            Expr::Exists { .. } => Ok(expr.clone()),
            Expr::BinaryOp { left, op, right } => {
                let new_left = Box::pin(self.materialize_subqueries_with_outer(
                    left,
                    outer_row,
                    outer_schema,
                    txn,
                    params,
                ))
                .await?;
                let new_right = Box::pin(self.materialize_subqueries_with_outer(
                    right,
                    outer_row,
                    outer_schema,
                    txn,
                    params,
                ))
                .await?;
                Ok(Expr::BinaryOp {
                    left: Box::new(new_left),
                    op: op.clone(),
                    right: Box::new(new_right),
                })
            }
            Expr::Nested(inner) => {
                let new_inner = Box::pin(self.materialize_subqueries_with_outer(
                    inner,
                    outer_row,
                    outer_schema,
                    txn,
                    params,
                ))
                .await?;
                Ok(Expr::Nested(Box::new(new_inner)))
            }
            Expr::UnaryOp { op, expr: inner } => {
                let new_inner = Box::pin(self.materialize_subqueries_with_outer(
                    inner,
                    outer_row,
                    outer_schema,
                    txn,
                    params,
                ))
                .await?;
                Ok(Expr::UnaryOp {
                    op: op.clone(),
                    expr: Box::new(new_inner),
                })
            }
            // All other expressions pass through unchanged
            other => Ok(other.clone()),
        }
    }

    /// Convert a FusionDB Value to a sqlparser Expr for subquery materialization.
    fn fusion_value_to_sql_expr(&self, val: &Value) -> Expr {
        let span = sqlparser::tokenizer::Span::empty();
        match val {
            Value::Integer(n) => Expr::Value(sqlparser::ast::ValueWithSpan {
                value: SqlValue::Number(n.to_string(), false),
                span,
            }),
            Value::Float(f) => Expr::Value(sqlparser::ast::ValueWithSpan {
                value: SqlValue::Number(f.to_string(), false),
                span,
            }),
            Value::String(s) => Expr::Value(sqlparser::ast::ValueWithSpan {
                value: SqlValue::SingleQuotedString(s.clone()),
                span,
            }),
            Value::Boolean(b) => Expr::Value(sqlparser::ast::ValueWithSpan {
                value: SqlValue::Boolean(*b),
                span,
            }),
            Value::Date(days) => Expr::TypedString(sqlparser::ast::TypedString {
                data_type: sqlparser::ast::DataType::Date,
                value: sqlparser::ast::ValueWithSpan {
                    value: SqlValue::SingleQuotedString(Value::format_date(*days)),
                    span,
                },
                uses_odbc_syntax: false,
            }),
            Value::Timestamp(micros) => Expr::TypedString(sqlparser::ast::TypedString {
                data_type: sqlparser::ast::DataType::Timestamp(
                    None,
                    sqlparser::ast::TimezoneInfo::None,
                ),
                value: sqlparser::ast::ValueWithSpan {
                    value: SqlValue::SingleQuotedString(Value::format_timestamp(*micros)),
                    span,
                },
                uses_odbc_syntax: false,
            }),
            Value::Array(values) => {
                let mut elems = Vec::with_capacity(values.len());
                for value in values {
                    elems.push(self.fusion_value_to_sql_expr(value));
                }
                Expr::Array(sqlparser::ast::Array {
                    elem: elems,
                    named: true,
                })
            }
            Value::Null => Expr::Value(sqlparser::ast::ValueWithSpan {
                value: SqlValue::Null,
                span,
            }),
            _ => Expr::Value(sqlparser::ast::ValueWithSpan {
                value: SqlValue::Null,
                span,
            }),
        }
    }

    /// Returns true if the expression contains any subqueries.
    pub(crate) fn contains_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::InSubquery { .. } | Expr::Subquery(_) | Expr::Exists { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::contains_subquery(left) || Self::contains_subquery(right)
            }
            Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => {
                Self::contains_subquery(inner)
            }
            _ => false,
        }
    }

    pub(crate) fn split_deferred_subquery_selection(expr: &Expr) -> (Option<Expr>, Option<Expr>) {
        let predicates = Self::collect_conjunctive_predicates(expr);
        let predicate_count = predicates.len();
        let mut eager = Vec::with_capacity(predicate_count);
        let mut deferred = Vec::with_capacity(predicate_count);

        for predicate in predicates {
            if Self::contains_deferred_subquery(&predicate) {
                deferred.push(predicate);
            } else {
                eager.push(predicate);
            }
        }

        (
            Self::combine_subquery_predicates(eager),
            Self::combine_subquery_predicates(deferred),
        )
    }

    fn contains_deferred_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Exists { .. } => true,
            Expr::InSubquery {
                expr: probe_expr,
                subquery,
                ..
            } => Self::in_subquery_allows_deferred(probe_expr, subquery),
            Expr::BinaryOp { left, right, .. } => {
                Self::contains_deferred_subquery(left) || Self::contains_deferred_subquery(right)
            }
            Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => {
                Self::contains_deferred_subquery(inner)
            }
            _ => false,
        }
    }

    fn deferred_subquery_cache_capacity(expr: &Expr) -> usize {
        match expr {
            Expr::Exists { .. } | Expr::InSubquery { .. } => 1,
            Expr::BinaryOp { left, right, .. } => Self::deferred_subquery_cache_capacity(left)
                .saturating_add(Self::deferred_subquery_cache_capacity(right)),
            Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => {
                Self::deferred_subquery_cache_capacity(inner)
            }
            _ => 0,
        }
    }

    fn in_subquery_probe_allows_deferred(expr: &Expr) -> bool {
        match expr {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => true,
            Expr::Nested(inner) | Expr::Cast { expr: inner, .. } => {
                Self::in_subquery_probe_allows_deferred(inner)
            }
            _ => false,
        }
    }

    fn in_subquery_allows_deferred(probe_expr: &Expr, subquery: &Query) -> bool {
        Self::in_subquery_probe_allows_deferred(probe_expr)
            && Self::in_subquery_query_shape_allows_deferred(subquery)
    }

    fn in_subquery_query_shape_allows_deferred(query: &Query) -> bool {
        if query.with.is_some() {
            return false;
        }

        match query.body.as_ref() {
            SetExpr::Select(select) => Self::in_subquery_select_shape_allows_deferred(select),
            SetExpr::Query(inner) => Self::in_subquery_query_shape_allows_deferred(inner),
            _ => false,
        }
    }

    fn in_subquery_select_shape_allows_deferred(select: &Select) -> bool {
        matches!(
            select.group_by,
            sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty()
        ) && select.from.iter().all(|table| table.joins.is_empty())
    }

    fn combine_subquery_predicates(predicates: Vec<Expr>) -> Option<Expr> {
        let mut iter = predicates.into_iter();
        let first = iter.next()?;
        Some(iter.fold(first, |acc, expr| Expr::BinaryOp {
            left: Box::new(acc),
            op: BinaryOperator::And,
            right: Box::new(expr),
        }))
    }

    pub(crate) async fn filter_rows_with_subqueries(
        &self,
        rows: Vec<Vec<Value>>,
        schema: &crate::catalog::TableSchema,
        expr: &Expr,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Vec<Vec<Value>>> {
        let mut membership_caches =
            HashMap::with_capacity(Self::deferred_subquery_cache_capacity(expr));
        let mut filtered = Vec::with_capacity(rows.len());
        for row in rows {
            if Box::pin(self.evaluate_expr_with_subqueries(
                expr,
                &row,
                schema,
                txn,
                params,
                &mut membership_caches,
            ))
            .await?
            .is_true()
            {
                filtered.push(row);
            }
        }
        Ok(filtered)
    }

    async fn evaluate_expr_with_subqueries(
        &self,
        expr: &Expr,
        row: &[Value],
        schema: &crate::catalog::TableSchema,
        txn: &mut dyn Transaction,
        params: &[Value],
        membership_caches: &mut HashMap<String, ExistsCache>,
    ) -> Result<SqlTruth> {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    let left_truth = Box::pin(self.evaluate_expr_with_subqueries(
                        left,
                        row,
                        schema,
                        txn,
                        params,
                        membership_caches,
                    ))
                    .await?;
                    if matches!(left_truth, SqlTruth::False) {
                        return Ok(SqlTruth::False);
                    }
                    Box::pin(self.evaluate_expr_with_subqueries(
                        right,
                        row,
                        schema,
                        txn,
                        params,
                        membership_caches,
                    ))
                    .await
                    .map(|right_truth| left_truth.and(right_truth))
                }
                BinaryOperator::Or => {
                    let left_truth = Box::pin(self.evaluate_expr_with_subqueries(
                        left,
                        row,
                        schema,
                        txn,
                        params,
                        membership_caches,
                    ))
                    .await?;
                    if matches!(left_truth, SqlTruth::True) {
                        return Ok(SqlTruth::True);
                    }
                    Box::pin(self.evaluate_expr_with_subqueries(
                        right,
                        row,
                        schema,
                        txn,
                        params,
                        membership_caches,
                    ))
                    .await
                    .map(|right_truth| left_truth.or(right_truth))
                }
                _ => self.evaluate_expr_truth(expr, row, schema, params),
            },
            Expr::Nested(inner) => {
                Box::pin(self.evaluate_expr_with_subqueries(
                    inner,
                    row,
                    schema,
                    txn,
                    params,
                    membership_caches,
                ))
                .await
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: inner,
            } => Ok(Box::pin(self.evaluate_expr_with_subqueries(
                inner,
                row,
                schema,
                txn,
                params,
                membership_caches,
            ))
            .await?
            .not()),
            Expr::InSubquery {
                expr: probe_expr,
                subquery,
                negated,
            } => {
                Box::pin(self.evaluate_in_subquery_membership(
                    probe_expr,
                    subquery,
                    *negated,
                    row,
                    schema,
                    txn,
                    params,
                    membership_caches,
                ))
                .await
            }
            Expr::Exists { subquery, negated } => {
                let has_rows = if let Some(plan) = self.exists_membership_plan(subquery) {
                    self.evaluate_exists_membership(
                        &plan,
                        row,
                        schema,
                        txn,
                        params,
                        membership_caches,
                    )
                    .await?
                } else if let Some(plan) =
                    Box::pin(self.exists_join_membership_plan(subquery, txn)).await?
                {
                    self.evaluate_exists_join_membership(
                        &plan,
                        row,
                        schema,
                        txn,
                        params,
                        membership_caches,
                    )
                    .await?
                } else {
                    self.evaluate_exists_subquery(subquery, row, schema, txn, params)
                        .await?
                };
                Ok(SqlTruth::from_bool(if *negated {
                    !has_rows
                } else {
                    has_rows
                }))
            }
            _ => self.evaluate_expr_truth(expr, row, schema, params),
        }
    }

    async fn evaluate_in_subquery_membership(
        &self,
        probe_expr: &Expr,
        subquery: &Query,
        negated: bool,
        outer_row: &[Value],
        outer_schema: &crate::catalog::TableSchema,
        txn: &mut dyn Transaction,
        params: &[Value],
        membership_caches: &mut HashMap<String, ExistsCache>,
    ) -> Result<SqlTruth> {
        let Some(column_type) = self
            .comparison_column_type(probe_expr, outer_schema)
            .map(str::to_string)
        else {
            return self
                .evaluate_in_subquery_via_materialization(
                    probe_expr,
                    subquery,
                    negated,
                    outer_row,
                    outer_schema,
                    txn,
                    params,
                )
                .await;
        };

        if !matches!(
            Box::pin(self.query_has_outer_references(subquery, txn, params)).await,
            Ok(false)
        ) {
            return self
                .evaluate_in_subquery_via_materialization(
                    probe_expr,
                    subquery,
                    negated,
                    outer_row,
                    outer_schema,
                    txn,
                    params,
                )
                .await;
        }

        let key = subquery_in_membership_cache_key(probe_expr, subquery, &column_type);
        if !membership_caches.contains_key(&key) {
            let (values, contains_null) = self
                .materialize_in_subquery_membership(subquery, &column_type, txn, params)
                .await?;
            membership_caches.insert(
                key.clone(),
                ExistsCache::InSubquery(InSubqueryMembershipCache {
                    values,
                    contains_null,
                }),
            );
        }

        let Some(ExistsCache::InSubquery(cache)) = membership_caches.get(&key) else {
            return self
                .evaluate_in_subquery_via_materialization(
                    probe_expr,
                    subquery,
                    negated,
                    outer_row,
                    outer_schema,
                    txn,
                    params,
                )
                .await;
        };

        self.evaluate_cached_in_subquery_membership(
            probe_expr,
            negated,
            cache,
            &column_type,
            outer_row,
            outer_schema,
            params,
        )
    }

    async fn materialize_in_subquery_membership(
        &self,
        subquery: &Query,
        column_type: &str,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<(HashSet<Value>, bool)> {
        let result = Box::pin(self.handle_query(subquery, txn, params)).await?;
        let QueryResult::Select { rows, .. } = result else {
            return Ok((HashSet::new(), false));
        };

        let mut values = HashSet::with_capacity(rows.len());
        let mut contains_null = false;
        for row in rows {
            let Some(value) = row.into_iter().next() else {
                continue;
            };
            let value = Self::coerce_value_to_column_type(value, column_type)?;
            if matches!(value, Value::Null) {
                contains_null = true;
            } else {
                values.insert(value);
            }
        }

        Ok((values, contains_null))
    }

    fn evaluate_cached_in_subquery_membership(
        &self,
        probe_expr: &Expr,
        negated: bool,
        cache: &InSubqueryMembershipCache,
        column_type: &str,
        outer_row: &[Value],
        outer_schema: &crate::catalog::TableSchema,
        params: &[Value],
    ) -> Result<SqlTruth> {
        if cache.values.is_empty() && !cache.contains_null {
            return Ok(SqlTruth::from_bool(negated));
        }

        let probe_value = self.evaluate_value(probe_expr, outer_row, outer_schema, params)?;
        let probe_value = Self::coerce_value_to_column_type(probe_value, column_type)?;
        if matches!(probe_value, Value::Null) {
            return Ok(SqlTruth::Unknown);
        }

        let found = cache.values.contains(&probe_value);
        if negated {
            Ok(if found {
                SqlTruth::False
            } else if cache.contains_null {
                SqlTruth::Unknown
            } else {
                SqlTruth::True
            })
        } else {
            Ok(if found {
                SqlTruth::True
            } else if cache.contains_null {
                SqlTruth::Unknown
            } else {
                SqlTruth::False
            })
        }
    }

    async fn evaluate_in_subquery_via_materialization(
        &self,
        probe_expr: &Expr,
        subquery: &Query,
        negated: bool,
        outer_row: &[Value],
        outer_schema: &crate::catalog::TableSchema,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<SqlTruth> {
        let expr = Expr::InSubquery {
            expr: Box::new(probe_expr.clone()),
            subquery: Box::new(subquery.clone()),
            negated,
        };
        let materialized = Box::pin(self.materialize_subqueries_with_outer(
            &expr,
            Some(outer_row),
            Some(outer_schema),
            txn,
            params,
        ))
        .await?;
        self.evaluate_expr_truth(&materialized, outer_row, outer_schema, params)
    }

    async fn evaluate_exists_membership(
        &self,
        plan: &ExistsMembershipPlan,
        outer_row: &[Value],
        outer_schema: &crate::catalog::TableSchema,
        txn: &mut dyn Transaction,
        params: &[Value],
        membership_caches: &mut HashMap<String, ExistsCache>,
    ) -> Result<bool> {
        let key = self.exists_membership_cache_key(plan);
        if !membership_caches.contains_key(&key) {
            let (local_schema, local_rows) = self
                .scan_table_base(&plan.table_factor, txn, params)
                .await?;
            let column_index = self.resolve_column_index(&plan.column_name, &local_schema)?;
            let mut values = HashSet::with_capacity(local_rows.len());
            for row in local_rows {
                if let Some(value) = row.get(column_index).cloned() {
                    values.insert(value);
                }
            }
            membership_caches.insert(
                key.clone(),
                ExistsCache::Single(ExistsMembershipCache {
                    plan: plan.clone(),
                    values,
                }),
            );
        }

        let Some(ExistsCache::Single(cache)) = membership_caches.get(&key) else {
            return Ok(false);
        };
        let probe_value =
            self.evaluate_value(&cache.plan.probe_expr, outer_row, outer_schema, params)?;
        Ok(cache.values.contains(&probe_value))
    }

    fn exists_membership_cache_key(&self, plan: &ExistsMembershipPlan) -> String {
        let table_factor_cache_name = Self::table_factor_cache_name(&plan.table_factor);
        subquery_exists_membership_cache_key(&table_factor_cache_name, &plan.column_name)
    }

    async fn evaluate_exists_join_membership(
        &self,
        plan: &ExistsJoinMembershipPlan,
        outer_row: &[Value],
        outer_schema: &crate::catalog::TableSchema,
        txn: &mut dyn Transaction,
        params: &[Value],
        membership_caches: &mut HashMap<String, ExistsCache>,
    ) -> Result<bool> {
        let key = self.exists_join_membership_cache_key(plan);
        if !membership_caches.contains_key(&key) {
            let (left_schema, left_rows) = self
                .scan_table_base(&plan.left_table_factor, txn, params)
                .await?;
            let (right_schema, right_rows) = self
                .scan_table_base(&plan.right_table_factor, txn, params)
                .await?;

            let left_join_index =
                self.resolve_column_index(&plan.left_join_column, &left_schema)?;
            let right_join_index =
                self.resolve_column_index(&plan.right_join_column, &right_schema)?;
            let filter_schema = match plan.filter_side {
                ExistsJoinSide::Left => &left_schema,
                ExistsJoinSide::Right => &right_schema,
            };
            let filter_index = self.resolve_column_index(&plan.filter_column, filter_schema)?;
            let probe_schema = match plan.probe_side {
                ExistsJoinSide::Left => &left_schema,
                ExistsJoinSide::Right => &right_schema,
            };
            let probe_index = self.resolve_column_index(&plan.probe_column, probe_schema)?;
            let filter_value = self.evaluate_value(
                &plan.filter_expr,
                &[],
                &crate::catalog::TableSchema::new("".to_string(), vec![]),
                params,
            )?;

            let mut matching_left_keys = HashSet::with_capacity(match plan.filter_side {
                ExistsJoinSide::Left => left_rows.len(),
                ExistsJoinSide::Right => 0,
            });
            let mut matching_right_keys = HashSet::with_capacity(match plan.filter_side {
                ExistsJoinSide::Left => 0,
                ExistsJoinSide::Right => right_rows.len(),
            });
            match plan.filter_side {
                ExistsJoinSide::Left => {
                    for row in &left_rows {
                        if row.get(filter_index) == Some(&filter_value) {
                            if let Some(value) = row.get(left_join_index).cloned() {
                                matching_left_keys.insert(value);
                            }
                        }
                    }
                }
                ExistsJoinSide::Right => {
                    for row in &right_rows {
                        if row.get(filter_index) == Some(&filter_value) {
                            if let Some(value) = row.get(right_join_index).cloned() {
                                matching_right_keys.insert(value);
                            }
                        }
                    }
                }
            }

            let mut values = HashSet::with_capacity(match plan.probe_side {
                ExistsJoinSide::Left => left_rows.len(),
                ExistsJoinSide::Right => right_rows.len(),
            });
            match plan.probe_side {
                ExistsJoinSide::Left => {
                    for row in left_rows {
                        let Some(join_value) = row.get(left_join_index) else {
                            continue;
                        };
                        let joined = match plan.filter_side {
                            ExistsJoinSide::Left => matching_left_keys.contains(join_value),
                            ExistsJoinSide::Right => matching_right_keys.contains(join_value),
                        };
                        if joined {
                            if let Some(value) = row.get(probe_index).cloned() {
                                values.insert(value);
                            }
                        }
                    }
                }
                ExistsJoinSide::Right => {
                    for row in right_rows {
                        let Some(join_value) = row.get(right_join_index) else {
                            continue;
                        };
                        let joined = match plan.filter_side {
                            ExistsJoinSide::Left => matching_left_keys.contains(join_value),
                            ExistsJoinSide::Right => matching_right_keys.contains(join_value),
                        };
                        if joined {
                            if let Some(value) = row.get(probe_index).cloned() {
                                values.insert(value);
                            }
                        }
                    }
                }
            };

            membership_caches.insert(
                key.clone(),
                ExistsCache::Join(ExistsJoinMembershipCache {
                    plan: plan.clone(),
                    values,
                }),
            );
        }

        let Some(ExistsCache::Join(cache)) = membership_caches.get(&key) else {
            return Ok(false);
        };
        let probe_value =
            self.evaluate_value(&cache.plan.probe_expr, outer_row, outer_schema, params)?;
        Ok(cache.values.contains(&probe_value))
    }

    fn exists_join_membership_cache_key(&self, plan: &ExistsJoinMembershipPlan) -> String {
        let left_table_factor_cache_name = Self::table_factor_cache_name(&plan.left_table_factor);
        let right_table_factor_cache_name = Self::table_factor_cache_name(&plan.right_table_factor);
        subquery_exists_join_membership_cache_key(
            &left_table_factor_cache_name,
            &right_table_factor_cache_name,
            &plan.left_join_column,
            &plan.right_join_column,
            &plan.filter_column,
            &plan.filter_expr,
            &plan.probe_column,
        )
    }

    fn table_factor_cache_name(factor: &TableFactor) -> String {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let alias_name = alias
                    .as_ref()
                    .map(|alias| alias.name.value.as_str())
                    .unwrap_or("");
                subquery_table_cache_name_for_factor(name, alias_name)
            }
            TableFactor::Derived { alias, .. } => {
                let alias_name = alias
                    .as_ref()
                    .map(|alias| alias.name.value.as_str())
                    .unwrap_or("");
                subquery_derived_cache_name_for_alias(alias_name)
            }
            _ => "unsupported".to_string(),
        }
    }

    fn exists_membership_plan(&self, subquery: &Query) -> Option<ExistsMembershipPlan> {
        if subquery.limit_clause.is_some() || subquery.fetch.is_some() {
            return None;
        }
        let select = match subquery.body.as_ref() {
            SetExpr::Select(select) => select,
            SetExpr::Query(inner) => match inner.body.as_ref() {
                SetExpr::Select(select) => {
                    if inner.limit_clause.is_some() || inner.fetch.is_some() {
                        return None;
                    }
                    select
                }
                _ => return None,
            },
            _ => return None,
        };
        if select.having.is_some()
            || select.distinct.is_some()
            || !matches!(
                select.group_by,
                sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty()
            )
        {
            return None;
        }
        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return None;
        }
        let table_factor = select.from[0].relation.clone();
        let relation_names = self.relation_names(&table_factor);
        let Some(selection) = select.selection.as_ref() else {
            return None;
        };
        let (left, right) = match selection {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => (left.as_ref(), right.as_ref()),
            Expr::Nested(inner) => {
                return self.exists_membership_plan(&Query {
                    body: Box::new(SetExpr::Select(Box::new(Select {
                        selection: Some((**inner).clone()),
                        ..select.as_ref().clone()
                    }))),
                    order_by: None,
                    limit_clause: None,
                    with: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
                });
            }
            _ => return None,
        };

        if let Some(column_name) = Self::local_membership_column(left, &relation_names) {
            return Some(ExistsMembershipPlan {
                table_factor,
                column_name,
                probe_expr: right.clone(),
            });
        }
        if let Some(column_name) = Self::local_membership_column(right, &relation_names) {
            return Some(ExistsMembershipPlan {
                table_factor,
                column_name,
                probe_expr: left.clone(),
            });
        }
        None
    }

    async fn exists_join_membership_plan(
        &self,
        subquery: &Query,
        txn: &mut dyn Transaction,
    ) -> Result<Option<ExistsJoinMembershipPlan>> {
        if subquery.limit_clause.is_some() || subquery.fetch.is_some() {
            return Ok(None);
        }
        let select = match subquery.body.as_ref() {
            SetExpr::Select(select) => select,
            SetExpr::Query(inner) => match inner.body.as_ref() {
                SetExpr::Select(select) => {
                    if inner.limit_clause.is_some() || inner.fetch.is_some() {
                        return Ok(None);
                    }
                    select
                }
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
        if select.having.is_some()
            || select.distinct.is_some()
            || !matches!(
                select.group_by,
                sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty()
            )
        {
            return Ok(None);
        }
        if !select.projection.iter().all(|item| {
            matches!(
                item,
                sqlparser::ast::SelectItem::Wildcard(_)
                    | sqlparser::ast::SelectItem::UnnamedExpr(_)
            )
        }) {
            return Ok(None);
        }
        if select.from.len() != 2 || !select.from.iter().all(|from| from.joins.is_empty()) {
            return Ok(None);
        }

        let left_table_factor = select.from[0].relation.clone();
        let right_table_factor = select.from[1].relation.clone();
        let Some(left_schema) = self
            .table_factor_schema_if_simple_table(&left_table_factor, txn)
            .await?
        else {
            return Ok(None);
        };
        let Some(right_schema) = self
            .table_factor_schema_if_simple_table(&right_table_factor, txn)
            .await?
        else {
            return Ok(None);
        };
        let Some(selection) = select.selection.as_ref() else {
            return Ok(None);
        };

        let predicates = Self::collect_conjunctive_predicates(selection);
        let mut join_columns: Option<(String, String)> = None;
        let mut filter: Option<(ExistsJoinSide, String, Expr)> = None;
        let mut probe: Option<(ExistsJoinSide, String, Expr)> = None;

        for predicate in predicates {
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = &predicate
            else {
                return Ok(None);
            };

            let left_ref = self.exists_join_local_ref(left, &left_schema, &right_schema);
            let right_ref = self.exists_join_local_ref(right, &left_schema, &right_schema);

            match (left_ref, right_ref) {
                (
                    Some((ExistsJoinSide::Left, left_col)),
                    Some((ExistsJoinSide::Right, right_col)),
                )
                | (
                    Some((ExistsJoinSide::Right, right_col)),
                    Some((ExistsJoinSide::Left, left_col)),
                ) => {
                    if join_columns.replace((left_col, right_col)).is_some() {
                        return Ok(None);
                    }
                }
                (Some((side, col)), None) => {
                    if self.exists_join_expr_is_outer_probe(right, &left_schema, &right_schema) {
                        if probe.replace((side, col, right.as_ref().clone())).is_some() {
                            return Ok(None);
                        }
                    } else if filter
                        .replace((side, col, right.as_ref().clone()))
                        .is_some()
                    {
                        return Ok(None);
                    }
                }
                (None, Some((side, col))) => {
                    if self.exists_join_expr_is_outer_probe(left, &left_schema, &right_schema) {
                        if probe.replace((side, col, left.as_ref().clone())).is_some() {
                            return Ok(None);
                        }
                    } else if filter.replace((side, col, left.as_ref().clone())).is_some() {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            }
        }

        let Some((left_join_column, right_join_column)) = join_columns else {
            return Ok(None);
        };
        let Some((filter_side, filter_column, filter_expr)) = filter else {
            return Ok(None);
        };
        let Some((probe_side, probe_column, probe_expr)) = probe else {
            return Ok(None);
        };

        Ok(Some(ExistsJoinMembershipPlan {
            left_table_factor,
            right_table_factor,
            left_join_column,
            right_join_column,
            filter_side,
            filter_column,
            filter_expr,
            probe_side,
            probe_column,
            probe_expr,
        }))
    }

    fn exists_join_local_ref(
        &self,
        expr: &Expr,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Option<(ExistsJoinSide, String)> {
        let col_name = match expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(idents) => Self::compound_identifier_name(idents),
            _ => return None,
        };

        let left = self.resolve_column_index(&col_name, left_schema).ok();
        let right = self.resolve_column_index(&col_name, right_schema).ok();
        match (left, right) {
            (Some(index), None) => Some((
                ExistsJoinSide::Left,
                left_schema.columns[index].name.clone(),
            )),
            (None, Some(index)) => Some((
                ExistsJoinSide::Right,
                right_schema.columns[index].name.clone(),
            )),
            _ => None,
        }
    }

    fn exists_join_expr_is_outer_probe(
        &self,
        expr: &Expr,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> bool {
        let mut columns = HashSet::with_capacity(
            left_schema
                .columns
                .len()
                .saturating_add(right_schema.columns.len()),
        );
        self.extract_columns_from_expr(expr, &mut columns);
        if columns.is_empty() {
            return false;
        }

        columns.into_iter().any(|column| {
            self.resolve_column_index(&column, left_schema).is_err()
                && self.resolve_column_index(&column, right_schema).is_err()
        })
    }

    async fn table_factor_schema_if_simple_table(
        &self,
        factor: &TableFactor,
        txn: &mut dyn Transaction,
    ) -> Result<Option<TableSchema>> {
        let TableFactor::Table { name, .. } = factor else {
            return Ok(None);
        };
        let table_name = name.to_string();
        let schema_key = subquery_schema_key_for_table(&table_name);
        let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? else {
            return Ok(None);
        };
        bincode::deserialize::<TableSchema>(&schema_bytes)
            .map(Some)
            .map_err(|e| {
                crate::common::FusionError::Execution(format!(
                    "Schema deserialization error: {}",
                    e
                ))
            })
    }

    fn local_membership_column(expr: &Expr, relation_names: &HashSet<String>) -> Option<String> {
        match expr {
            Expr::Identifier(_) => None,
            Expr::CompoundIdentifier(idents) => {
                let qualifier = Self::subquery_compound_identifier_prefix(idents);
                if !relation_names
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&qualifier))
                {
                    return None;
                }
                Some(Self::compound_identifier_name(idents))
            }
            _ => None,
        }
    }

    async fn query_has_outer_references(
        &self,
        query: &Query,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<bool> {
        if query.with.is_some() {
            return Ok(true);
        }

        match query.body.as_ref() {
            SetExpr::Select(select) => {
                Box::pin(self.select_has_outer_references(select, txn, params)).await
            }
            SetExpr::Query(inner) => {
                Box::pin(self.query_has_outer_references(inner, txn, params)).await
            }
            _ => Ok(true),
        }
    }

    async fn select_has_outer_references(
        &self,
        select: &Select,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<bool> {
        if select.from.iter().any(|table| !table.joins.is_empty()) {
            return Ok(true);
        }

        let local_scope = self.subquery_local_scope(&select.from, txn, params).await?;
        if let Some(selection) = &select.selection {
            if self.expr_has_outer_reference(selection, &local_scope) {
                return Ok(true);
            }
        }
        if let Some(having) = &select.having {
            if self.expr_has_outer_reference(having, &local_scope) {
                return Ok(true);
            }
        }
        if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
            for expr in exprs {
                if self.expr_has_outer_reference(expr, &local_scope) {
                    return Ok(true);
                }
            }
        } else {
            return Ok(true);
        }

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if self.expr_has_outer_reference(expr, &local_scope) {
                        return Ok(true);
                    }
                }
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
            }
        }

        Ok(false)
    }

    fn expr_has_outer_reference(&self, expr: &Expr, local_scope: &SubqueryLocalScope) -> bool {
        match expr {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                !self.expr_is_local_to_subquery(expr, local_scope)
            }
            Expr::Value(_) | Expr::TypedString(_) => false,
            Expr::BinaryOp { left, right, .. } => {
                self.expr_has_outer_reference(left, local_scope)
                    || self.expr_has_outer_reference(right, local_scope)
            }
            Expr::Nested(inner)
            | Expr::UnaryOp { expr: inner, .. }
            | Expr::Cast { expr: inner, .. }
            | Expr::Extract { expr: inner, .. }
            | Expr::IsNull(inner)
            | Expr::IsNotNull(inner)
            | Expr::IsTrue(inner)
            | Expr::IsFalse(inner) => self.expr_has_outer_reference(inner, local_scope),
            Expr::Array(array) => array
                .elem
                .iter()
                .any(|elem| self.expr_has_outer_reference(elem, local_scope)),
            Expr::Function(func) => match &func.args {
                FunctionArguments::List(args) => args.args.iter().any(|arg| match arg {
                    FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => match arg {
                        FunctionArgExpr::Expr(expr) => {
                            self.expr_has_outer_reference(expr, local_scope)
                        }
                        _ => false,
                    },
                    FunctionArg::ExprNamed { name, arg, .. } => {
                        self.expr_has_outer_reference(name, local_scope)
                            || match arg {
                                FunctionArgExpr::Expr(expr) => {
                                    self.expr_has_outer_reference(expr, local_scope)
                                }
                                _ => false,
                            }
                    }
                }),
                _ => true,
            },
            Expr::Between {
                expr, low, high, ..
            } => {
                self.expr_has_outer_reference(expr, local_scope)
                    || self.expr_has_outer_reference(low, local_scope)
                    || self.expr_has_outer_reference(high, local_scope)
            }
            Expr::InList { expr, list, .. } => {
                self.expr_has_outer_reference(expr, local_scope)
                    || list
                        .iter()
                        .any(|item| self.expr_has_outer_reference(item, local_scope))
            }
            Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
                self.expr_has_outer_reference(expr, local_scope)
                    || self.expr_has_outer_reference(pattern, local_scope)
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                operand
                    .as_ref()
                    .is_some_and(|expr| self.expr_has_outer_reference(expr, local_scope))
                    || conditions.iter().any(|case_when| {
                        self.expr_has_outer_reference(&case_when.condition, local_scope)
                            || self.expr_has_outer_reference(&case_when.result, local_scope)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|expr| self.expr_has_outer_reference(expr, local_scope))
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                self.expr_has_outer_reference(expr, local_scope)
                    || substring_from
                        .as_ref()
                        .is_some_and(|expr| self.expr_has_outer_reference(expr, local_scope))
                    || substring_for
                        .as_ref()
                        .is_some_and(|expr| self.expr_has_outer_reference(expr, local_scope))
            }
            Expr::Trim {
                expr,
                trim_what,
                trim_characters,
                ..
            } => {
                self.expr_has_outer_reference(expr, local_scope)
                    || trim_what
                        .as_ref()
                        .is_some_and(|expr| self.expr_has_outer_reference(expr, local_scope))
                    || trim_characters.as_ref().is_some_and(|exprs| {
                        exprs
                            .iter()
                            .any(|expr| self.expr_has_outer_reference(expr, local_scope))
                    })
            }
            Expr::Position { expr, r#in } => {
                self.expr_has_outer_reference(expr, local_scope)
                    || self.expr_has_outer_reference(r#in, local_scope)
            }
            Expr::CompoundFieldAccess { root, access_chain } => {
                self.expr_has_outer_reference(root, local_scope)
                    || access_chain.iter().any(|access| {
                        if let sqlparser::ast::AccessExpr::Subscript(
                            sqlparser::ast::Subscript::Index { index },
                        ) = access
                        {
                            self.expr_has_outer_reference(index, local_scope)
                        } else {
                            false
                        }
                    })
            }
            Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
                self.expr_has_outer_reference(left, local_scope)
                    || self.expr_has_outer_reference(right, local_scope)
            }
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => true,
            _ => true,
        }
    }

    async fn evaluate_exists_subquery(
        &self,
        subquery: &Query,
        outer_row: &[Value],
        outer_schema: &crate::catalog::TableSchema,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<bool> {
        let mut bound_subquery = subquery.clone();
        Box::pin(self.bind_outer_references_in_query(
            &mut bound_subquery,
            outer_row,
            outer_schema,
            txn,
            params,
        ))
        .await?;

        let result = Box::pin(self.handle_query(&bound_subquery, txn, params)).await?;
        Ok(match result {
            QueryResult::Select { rows, .. } => !rows.is_empty(),
            _ => false,
        })
    }

    async fn bind_outer_references_in_query(
        &self,
        query: &mut Query,
        outer_row: &[Value],
        outer_schema: &crate::catalog::TableSchema,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<()> {
        match query.body.as_mut() {
            SetExpr::Select(select) => {
                self.bind_outer_references_in_select(select, outer_row, outer_schema, txn, params)
                    .await
            }
            SetExpr::Query(inner) => {
                Box::pin(self.bind_outer_references_in_query(
                    inner,
                    outer_row,
                    outer_schema,
                    txn,
                    params,
                ))
                .await
            }
            _ => Ok(()),
        }
    }

    async fn bind_outer_references_in_select(
        &self,
        select: &mut Box<Select>,
        outer_row: &[Value],
        outer_schema: &crate::catalog::TableSchema,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<()> {
        let local_scope = self.subquery_local_scope(&select.from, txn, params).await?;
        if let Some(selection) = &select.selection {
            select.selection = Some(self.bind_outer_references_in_expr(
                selection,
                outer_row,
                outer_schema,
                &local_scope,
                params,
            )?);
        }
        if let Some(having) = &select.having {
            select.having = Some(self.bind_outer_references_in_expr(
                having,
                outer_row,
                outer_schema,
                &local_scope,
                params,
            )?);
        }
        for item in &mut select.projection {
            self.bind_outer_references_in_select_item(
                item,
                outer_row,
                outer_schema,
                &local_scope,
                params,
            )?;
        }
        Ok(())
    }

    async fn subquery_local_scope(
        &self,
        from: &[TableWithJoins],
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<SubqueryLocalScope> {
        let relation_capacity = from
            .iter()
            .map(|table| 1usize.saturating_add(table.joins.len()))
            .sum();
        let mut columns = Vec::with_capacity(relation_capacity);

        for table in from {
            self.append_local_scope_for_factor(&table.relation, &mut columns, txn, params)
                .await?;
            for join in &table.joins {
                self.append_local_scope_for_factor(&join.relation, &mut columns, txn, params)
                    .await?;
            }
        }

        Ok(SubqueryLocalScope {
            schema: crate::catalog::TableSchema::new("__subquery_scope".to_string(), columns),
        })
    }

    async fn append_local_scope_for_factor(
        &self,
        factor: &TableFactor,
        columns: &mut Vec<crate::catalog::Column>,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<()> {
        let schema = match factor {
            TableFactor::Table { name, .. } => {
                let table_name = name.to_string();
                let schema_key = subquery_schema_key_for_table(&table_name);
                let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? else {
                    return Ok(());
                };
                bincode::deserialize::<crate::catalog::TableSchema>(&schema_bytes).map_err(|e| {
                    crate::common::FusionError::Execution(format!(
                        "Schema deserialization error: {}",
                        e
                    ))
                })?
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let result = Box::pin(self.handle_query(subquery, txn, params)).await?;
                let QueryResult::Select {
                    columns: output_columns,
                    ..
                } = result
                else {
                    return Ok(());
                };
                let table_name = alias
                    .as_ref()
                    .map(|alias| alias.name.value.clone())
                    .unwrap_or_else(|| "derived".to_string());
                crate::catalog::TableSchema::new(
                    table_name,
                    output_columns
                        .into_iter()
                        .map(|name| crate::catalog::Column {
                            name,
                            data_type: "UNKNOWN".to_string(),
                            is_primary: false,
                            is_indexed: false,
                            index_type: crate::catalog::IndexType::None,
                            default_value: None,
                            is_nullable: true,
                            is_unique: false,
                            check_expr: None,
                        })
                        .collect(),
                )
            }
            _ => return Ok(()),
        };

        let mut prefixed_schema = schema.clone();
        self.prefix_schema_columns(&mut prefixed_schema, factor)?;

        for column in schema.columns {
            if !columns
                .iter()
                .any(|existing| existing.name.eq_ignore_ascii_case(&column.name))
            {
                columns.push(column);
            }
        }
        for column in prefixed_schema.columns {
            if !columns
                .iter()
                .any(|existing| existing.name.eq_ignore_ascii_case(&column.name))
            {
                columns.push(column);
            }
        }

        Ok(())
    }

    fn bind_outer_references_in_select_item(
        &self,
        item: &mut SelectItem,
        outer_row: &[Value],
        outer_schema: &crate::catalog::TableSchema,
        local_scope: &SubqueryLocalScope,
        params: &[Value],
    ) -> Result<()> {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                *expr = self.bind_outer_references_in_expr(
                    expr,
                    outer_row,
                    outer_schema,
                    local_scope,
                    params,
                )?;
            }
            SelectItem::ExprWithAlias { expr, .. } => {
                *expr = self.bind_outer_references_in_expr(
                    expr,
                    outer_row,
                    outer_schema,
                    local_scope,
                    params,
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    fn bind_outer_references_in_expr(
        &self,
        expr: &Expr,
        outer_row: &[Value],
        outer_schema: &crate::catalog::TableSchema,
        local_scope: &SubqueryLocalScope,
        params: &[Value],
    ) -> Result<Expr> {
        match expr {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                if self.expr_is_local_to_subquery(expr, local_scope) {
                    return Ok(expr.clone());
                }

                if let Some(value) = self.outer_column_value(expr, outer_row, outer_schema) {
                    Ok(self.fusion_value_to_sql_expr(&value))
                } else {
                    Ok(expr.clone())
                }
            }
            Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(self.bind_outer_references_in_expr(
                    left,
                    outer_row,
                    outer_schema,
                    local_scope,
                    params,
                )?),
                op: op.clone(),
                right: Box::new(self.bind_outer_references_in_expr(
                    right,
                    outer_row,
                    outer_schema,
                    local_scope,
                    params,
                )?),
            }),
            Expr::Nested(inner) => Ok(Expr::Nested(Box::new(self.bind_outer_references_in_expr(
                inner,
                outer_row,
                outer_schema,
                local_scope,
                params,
            )?))),
            Expr::UnaryOp { op, expr: inner } => Ok(Expr::UnaryOp {
                op: op.clone(),
                expr: Box::new(self.bind_outer_references_in_expr(
                    inner,
                    outer_row,
                    outer_schema,
                    local_scope,
                    params,
                )?),
            }),
            Expr::Value(value) => {
                if let SqlValue::Placeholder(p) = &value.value {
                    let idx = Self::placeholder_index(p);
                    if idx > 0 && idx <= params.len() {
                        return Ok(self.fusion_value_to_sql_expr(&params[idx - 1]));
                    }
                }
                Ok(expr.clone())
            }
            _ => Ok(expr.clone()),
        }
    }

    fn expr_is_local_to_subquery(&self, expr: &Expr, local_scope: &SubqueryLocalScope) -> bool {
        match expr {
            Expr::Identifier(ident) => self
                .resolve_column_index(&ident.value, &local_scope.schema)
                .is_ok(),
            Expr::CompoundIdentifier(idents) => {
                let col_name = Self::compound_identifier_name(idents);
                local_scope
                    .schema
                    .columns
                    .iter()
                    .any(|column| column.name.eq_ignore_ascii_case(&col_name))
            }
            _ => false,
        }
    }

    fn outer_column_value(
        &self,
        expr: &Expr,
        outer_row: &[Value],
        outer_schema: &crate::catalog::TableSchema,
    ) -> Option<Value> {
        let col_name = match expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(idents) => Self::compound_identifier_name(idents),
            _ => return None,
        };

        self.resolve_column_index(&col_name, outer_schema)
            .ok()
            .and_then(|index| outer_row.get(index).cloned())
    }

    fn subquery_compound_identifier_prefix(idents: &[sqlparser::ast::Ident]) -> String {
        let prefix_len = idents.len().saturating_sub(1);
        let capacity = idents
            .iter()
            .take(prefix_len)
            .map(|ident| ident.value.len())
            .sum::<usize>()
            + prefix_len.saturating_sub(1);
        let mut prefix = String::with_capacity(capacity);

        for (index, ident) in idents.iter().take(prefix_len).enumerate() {
            if index > 0 {
                prefix.push('.');
            }
            prefix.push_str(&ident.value);
        }

        prefix
    }
}

#[cfg(test)]
mod tests {
    use super::{
        subquery_derived_cache_name_for_alias, subquery_exists_join_membership_cache_key,
        subquery_exists_membership_cache_key, subquery_push_ascii_lowercase,
        subquery_schema_key_for_table, subquery_table_cache_name_for_factor,
    };
    use sqlparser::ast::{Expr, Ident, ObjectName, ObjectNamePart};

    #[test]
    fn subquery_schema_key_for_table_preallocates_exact_key() {
        let key = subquery_schema_key_for_table("orders");

        assert_eq!(key, "schema:orders");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn subquery_derived_cache_name_for_alias_preallocates_exact_name() {
        let name = subquery_derived_cache_name_for_alias("candidate_orders");

        assert_eq!(name, "derived:candidate_orders");
        assert!(name.capacity() >= name.len());
    }

    #[test]
    fn subquery_table_cache_name_for_factor_preallocates_exact_name() {
        let table_name = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("public")),
            ObjectNamePart::Identifier(Ident::new("orders")),
        ]);
        let name = subquery_table_cache_name_for_factor(&table_name, "o");

        assert_eq!(name, "table:public.orders:o");
        assert!(name.capacity() >= name.len());
    }

    #[test]
    fn subquery_push_ascii_lowercase_matches_to_ascii_lowercase() {
        let value = "CustomerID_Ä_2024";
        let mut output = String::from("prefix:");

        subquery_push_ascii_lowercase(&mut output, value);

        assert_eq!(output, format!("prefix:{}", value.to_ascii_lowercase()));
    }

    #[test]
    fn subquery_exists_membership_cache_key_preserves_legacy_key() {
        let table_cache_name = "table:public.orders:o";
        let column_name = "CustomerID_Ä";
        let key = subquery_exists_membership_cache_key(table_cache_name, column_name);

        assert_eq!(
            key,
            format!("{}|{}", table_cache_name, column_name.to_ascii_lowercase())
        );
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn subquery_exists_join_membership_cache_key_preserves_legacy_key() {
        let filter_expr = Expr::Identifier(Ident::new("OrderStatus"));
        let key = subquery_exists_join_membership_cache_key(
            "table:public.orders:o",
            "table:public.customers:c",
            "CustomerID",
            "ID",
            "Status",
            &filter_expr,
            "ProbeID",
        );

        assert_eq!(
            key,
            format!(
                "{}|{}|{}|{}|{}|{}|{}",
                "table:public.orders:o",
                "table:public.customers:c",
                "CustomerID".to_ascii_lowercase(),
                "ID".to_ascii_lowercase(),
                "Status".to_ascii_lowercase(),
                filter_expr,
                "ProbeID".to_ascii_lowercase()
            )
        );
        assert!(key.capacity() >= key.len());
    }
}
