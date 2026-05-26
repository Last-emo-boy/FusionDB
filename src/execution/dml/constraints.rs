use crate::common::{Result, Value};

use super::super::Executor;

impl Executor {
    pub(super) fn evaluate_check_constraint(
        &self,
        check_sql: &str,
        col_name: &str,
        value: &Value,
    ) -> bool {
        // Parse the CHECK expression and evaluate it against the column value
        // CHECK expressions reference the column by name, e.g. CHECK(age > 0)
        // We build a minimal SELECT with a WHERE clause to reuse existing expression evaluation
        if *value == Value::Null {
            return true; // NULL passes CHECK constraints (SQL standard)
        }

        // Strip "CHECK" prefix if present (sqlparser Display may include it)
        let expr_str = check_sql.trim();
        let expr_str = if expr_str.to_uppercase().starts_with("CHECK") {
            let rest = expr_str[5..].trim();
            if rest.starts_with('(') && rest.ends_with(')') {
                &rest[1..rest.len() - 1]
            } else {
                rest
            }
        } else {
            expr_str
        };

        // Try to parse the check expression
        let parse_result = crate::parser::parse_sql(&format!("SELECT 1 WHERE {}", expr_str));
        if let Ok(stmts) = parse_result {
            if let Some(sqlparser::ast::Statement::Query(query)) = stmts.first() {
                if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(ref where_expr) = select.selection {
                        // Build a single-column schema and row for evaluation
                        use crate::catalog::{Column, IndexType, TableSchema};
                        let schema = TableSchema::new(
                            "_check".to_string(),
                            vec![Column {
                                name: col_name.to_string(),
                                data_type: "TEXT".to_string(),
                                is_primary: false,
                                is_indexed: false,
                                index_type: IndexType::None,
                                default_value: None,
                                is_nullable: true,
                                is_unique: false,
                                check_expr: None,
                            }],
                        );
                        let row = vec![value.clone()];
                        return self
                            .evaluate_expr(where_expr, &row, &schema, &[])
                            .unwrap_or(false);
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
        match def_str.to_lowercase().as_str() {
            "true" => return Ok(Value::Boolean(true)),
            "false" => return Ok(Value::Boolean(false)),
            "null" => return Ok(Value::Null),
            _ => {}
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
