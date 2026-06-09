use crate::catalog::TableSchema;
use crate::common::{Result, Value};
use crate::storage::Transaction;
use sqlparser::ast::{
    BinaryOperator, Expr, Query, Select, SetExpr, TableFactor, TableWithJoins, UnaryOperator,
    Value as SqlValue,
};
use std::collections::{HashMap, HashSet};

use super::super::{Executor, QueryResult};

struct SubqueryLocalScope {
    relation_names: HashSet<String>,
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

enum ExistsCache {
    Single(ExistsMembershipCache),
    Join(ExistsJoinMembershipCache),
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
            Expr::Exists { .. } => 1,
            Expr::BinaryOp { left, right, .. } => Self::deferred_subquery_cache_capacity(left)
                .saturating_add(Self::deferred_subquery_cache_capacity(right)),
            Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => {
                Self::deferred_subquery_cache_capacity(inner)
            }
            _ => 0,
        }
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
    ) -> Result<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    if !Box::pin(self.evaluate_expr_with_subqueries(
                        left,
                        row,
                        schema,
                        txn,
                        params,
                        membership_caches,
                    ))
                    .await?
                    {
                        return Ok(false);
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
                }
                BinaryOperator::Or => {
                    if Box::pin(self.evaluate_expr_with_subqueries(
                        left,
                        row,
                        schema,
                        txn,
                        params,
                        membership_caches,
                    ))
                    .await?
                    {
                        return Ok(true);
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
                }
                _ => self.evaluate_expr(expr, row, schema, params),
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
            } => Ok(!Box::pin(self.evaluate_expr_with_subqueries(
                inner,
                row,
                schema,
                txn,
                params,
                membership_caches,
            ))
            .await?),
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
                Ok(if *negated { !has_rows } else { has_rows })
            }
            _ => self.evaluate_expr(expr, row, schema, params),
        }
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
        format!(
            "{}|{}",
            Self::table_factor_cache_name(&plan.table_factor),
            plan.column_name.to_ascii_lowercase()
        )
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
        format!(
            "{}|{}|{}|{}|{}|{}|{}",
            Self::table_factor_cache_name(&plan.left_table_factor),
            Self::table_factor_cache_name(&plan.right_table_factor),
            plan.left_join_column.to_ascii_lowercase(),
            plan.right_join_column.to_ascii_lowercase(),
            plan.filter_column.to_ascii_lowercase(),
            plan.filter_expr,
            plan.probe_column.to_ascii_lowercase()
        )
    }

    fn table_factor_cache_name(factor: &TableFactor) -> String {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let alias_name = alias
                    .as_ref()
                    .map(|alias| alias.name.value.as_str())
                    .unwrap_or("");
                format!("table:{}:{}", name, alias_name)
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
        let select = match subquery.body.as_ref() {
            SetExpr::Select(select) => select,
            SetExpr::Query(inner) => match inner.body.as_ref() {
                SetExpr::Select(select) => select,
                _ => return None,
            },
            _ => return None,
        };
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
        let select = match subquery.body.as_ref() {
            SetExpr::Select(select) => select,
            SetExpr::Query(inner) => match inner.body.as_ref() {
                SetExpr::Select(select) => select,
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
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
        let mut relation_names = HashSet::with_capacity(relation_capacity);
        let mut columns = Vec::with_capacity(relation_capacity);

        for table in from {
            self.append_local_scope_for_factor(
                &table.relation,
                &mut relation_names,
                &mut columns,
                txn,
                params,
            )
            .await?;
            for join in &table.joins {
                self.append_local_scope_for_factor(
                    &join.relation,
                    &mut relation_names,
                    &mut columns,
                    txn,
                    params,
                )
                .await?;
            }
        }

        Ok(SubqueryLocalScope {
            relation_names,
            schema: crate::catalog::TableSchema::new("__subquery_scope".to_string(), columns),
        })
    }

    async fn append_local_scope_for_factor(
        &self,
        factor: &TableFactor,
        relation_names: &mut HashSet<String>,
        columns: &mut Vec<crate::catalog::Column>,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<()> {
        relation_names.extend(self.relation_names(factor));

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
                let qualifier = Self::subquery_compound_identifier_prefix(idents);
                if local_scope
                    .relation_names
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&qualifier))
                {
                    return true;
                }
                local_scope
                    .schema
                    .columns
                    .iter()
                    .any(|column| column.name.eq_ignore_ascii_case(&col_name))
                    || self
                        .resolve_column_index(&col_name, &local_scope.schema)
                        .is_ok()
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
    use super::{subquery_derived_cache_name_for_alias, subquery_schema_key_for_table};

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
}
