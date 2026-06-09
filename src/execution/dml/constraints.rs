use crate::common::{Result, Value};
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};

use super::super::Executor;

const CHECK_KEYWORD: &[u8] = b"CHECK";

fn starts_with_check_keyword_ascii(value: &str) -> bool {
    match value.as_bytes().get(..CHECK_KEYWORD.len()) {
        Some(prefix) => prefix.eq_ignore_ascii_case(CHECK_KEYWORD),
        None => false,
    }
}

fn parse_keyword_default_value(def_str: &str) -> Option<Value> {
    if def_str.eq_ignore_ascii_case("true") {
        Some(Value::Boolean(true))
    } else if def_str.eq_ignore_ascii_case("false") {
        Some(Value::Boolean(false))
    } else if def_str.eq_ignore_ascii_case("null") {
        Some(Value::Null)
    } else {
        None
    }
}

impl Executor {
    fn evaluate_check_truth(
        &self,
        expr: &Expr,
        row: &[Value],
        schema: &crate::catalog::TableSchema,
    ) -> Result<Option<bool>> {
        match expr {
            Expr::Nested(inner) => self.evaluate_check_truth(inner, row, schema),
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => Ok(self
                .evaluate_check_truth(expr, row, schema)?
                .map(|value| !value)),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let left = self.evaluate_check_truth(left, row, schema)?;
                let right = self.evaluate_check_truth(right, row, schema)?;
                Ok(match (left, right) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                })
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Or,
                right,
            } => {
                let left = self.evaluate_check_truth(left, row, schema)?;
                let right = self.evaluate_check_truth(right, row, schema)?;
                Ok(match (left, right) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                })
            }
            Expr::BinaryOp { left, op, right }
                if matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Gt
                        | BinaryOperator::Lt
                        | BinaryOperator::GtEq
                        | BinaryOperator::LtEq
                ) =>
            {
                let left_val = self.evaluate_value(left, row, schema, &[])?;
                let right_val = self.evaluate_value(right, row, schema, &[])?;
                if matches!(left_val, Value::Null) || matches!(right_val, Value::Null) {
                    Ok(None)
                } else {
                    self.evaluate_expr(expr, row, schema, &[]).map(Some)
                }
            }
            _ => self.evaluate_expr(expr, row, schema, &[]).map(Some),
        }
    }

    pub(super) fn evaluate_check_constraint(
        &self,
        check_sql: &str,
        schema: &crate::catalog::TableSchema,
        row: &[Value],
    ) -> bool {
        // Parse the CHECK expression and evaluate it against the column value
        // CHECK expressions reference the column by name, e.g. CHECK(age > 0)
        // We build a minimal SELECT with a WHERE clause to reuse existing expression evaluation
        // Strip "CHECK" prefix if present (sqlparser Display may include it)
        let expr_str = check_sql.trim();
        let expr_str = if starts_with_check_keyword_ascii(expr_str) {
            let rest = expr_str[5..].trim();
            if rest.starts_with('(') && rest.ends_with(')') {
                &rest[1..rest.len() - 1]
            } else {
                rest
            }
        } else {
            expr_str
        };
        let expr_str = expr_str.trim();
        let expr_str = expr_str
            .strip_prefix('(')
            .and_then(|inner| inner.strip_suffix(')'))
            .unwrap_or(expr_str)
            .trim();

        // Try to parse the check expression
        let parse_result = crate::parser::parse_sql(&format!("SELECT 1 WHERE {}", expr_str));
        if let Ok(stmts) = parse_result {
            if let Some(sqlparser::ast::Statement::Query(query)) = stmts.first() {
                if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(ref where_expr) = select.selection {
                        // SQL CHECK passes when the predicate is true or unknown.
                        return !matches!(
                            self.evaluate_check_truth(where_expr, row, schema),
                            Ok(Some(false))
                        );
                    }
                }
            }
        }
        true // If we can't parse, pass the check (don't break existing data)
    }

    pub(super) fn parse_default_value(&self, def_str: &str) -> Result<Value> {
        if let Ok(stmts) = crate::parser::parse_sql(&format!("SELECT {}", def_str)) {
            if let Some(sqlparser::ast::Statement::Query(query)) = stmts.first() {
                if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(sqlparser::ast::SelectItem::UnnamedExpr(expr)) =
                        select.projection.first()
                    {
                        let schema =
                            crate::catalog::TableSchema::new("_default".to_string(), vec![]);
                        return self.evaluate_value(expr, &[], &schema, &[]);
                    }
                }
            }
        }

        // Try parsing as integer
        if let Ok(n) = def_str.parse::<i64>() {
            return Ok(Value::Integer(n));
        }
        // Try parsing as float
        if let Ok(f) = def_str.parse::<f64>() {
            return Ok(Value::Float(f));
        }
        // Boolean
        if let Some(value) = parse_keyword_default_value(def_str) {
            return Ok(value);
        }
        // Strip quotes for string literals
        let trimmed = def_str.trim();
        if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
            || (trimmed.starts_with('"') && trimmed.ends_with('"'))
        {
            return Ok(Value::String(trimmed[1..trimmed.len() - 1].to_string()));
        }
        Ok(Value::String(def_str.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_keyword_default_value, starts_with_check_keyword_ascii};
    use crate::common::Value;

    #[test]
    fn check_keyword_prefix_matching_is_ascii_case_insensitive() {
        assert!(starts_with_check_keyword_ascii("CHECK(age > 0)"));
        assert!(starts_with_check_keyword_ascii("check(age > 0)"));
        assert!(starts_with_check_keyword_ascii("ChEcK age > 0"));
        assert!(!starts_with_check_keyword_ascii("age CHECK > 0"));
        assert!(!starts_with_check_keyword_ascii("chec"));
    }

    #[test]
    fn keyword_default_value_matching_is_ascii_case_insensitive() {
        assert_eq!(
            parse_keyword_default_value("TRUE"),
            Some(Value::Boolean(true))
        );
        assert_eq!(
            parse_keyword_default_value("false"),
            Some(Value::Boolean(false))
        );
        assert_eq!(parse_keyword_default_value("Null"), Some(Value::Null));
        assert_eq!(parse_keyword_default_value("truthy"), None);
    }
}
