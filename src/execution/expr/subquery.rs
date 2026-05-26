use crate::common::{Result, Value};
use crate::storage::Transaction;
use sqlparser::ast::{Expr, Value as SqlValue};

use super::super::{Executor, QueryResult};

impl Executor {
    pub(crate) async fn materialize_subqueries(
        &self,
        expr: &Expr,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Expr> {
        match expr {
            Expr::InSubquery {
                expr: inner_expr,
                subquery,
                negated,
            } => {
                let result = Box::pin(self.handle_query(subquery, txn, params)).await?;
                let values = match result {
                    QueryResult::Select { rows, .. } => rows
                        .into_iter()
                        .filter_map(|row| row.into_iter().next())
                        .map(|v| self.fusion_value_to_sql_expr(&v))
                        .collect(),
                    _ => vec![],
                };
                Ok(Expr::InList {
                    expr: inner_expr.clone(),
                    list: values,
                    negated: *negated,
                })
            }
            Expr::Subquery(subquery) => {
                let result = Box::pin(self.handle_query(subquery, txn, params)).await?;
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
            Expr::Exists { subquery, negated } => {
                let result = Box::pin(self.handle_query(subquery, txn, params)).await?;
                let has_rows = match result {
                    QueryResult::Select { rows, .. } => !rows.is_empty(),
                    _ => false,
                };
                let bool_val = if *negated { !has_rows } else { has_rows };
                Ok(Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: SqlValue::Boolean(bool_val),
                    span: sqlparser::tokenizer::Span::empty(),
                }))
            }
            Expr::BinaryOp { left, op, right } => {
                let new_left = Box::pin(self.materialize_subqueries(left, txn, params)).await?;
                let new_right = Box::pin(self.materialize_subqueries(right, txn, params)).await?;
                Ok(Expr::BinaryOp {
                    left: Box::new(new_left),
                    op: op.clone(),
                    right: Box::new(new_right),
                })
            }
            Expr::Nested(inner) => {
                let new_inner = Box::pin(self.materialize_subqueries(inner, txn, params)).await?;
                Ok(Expr::Nested(Box::new(new_inner)))
            }
            Expr::UnaryOp { op, expr: inner } => {
                let new_inner = Box::pin(self.materialize_subqueries(inner, txn, params)).await?;
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
}
