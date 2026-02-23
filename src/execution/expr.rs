use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    Value as SqlValue,
};
use std::cmp::Ordering;
use std::collections::HashSet;

use super::{Executor, QueryResult};

impl Executor {
    pub(crate) fn evaluate_expr(
        &self,
        expr: &Expr,
        row: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Result<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Handle logical operators with short-circuit evaluation
                match op {
                    BinaryOperator::And => {
                        let l = self.evaluate_expr(left, row, schema, params)?;
                        if !l { return Ok(false); }
                        return self.evaluate_expr(right, row, schema, params);
                    }
                    BinaryOperator::Or => {
                        let l = self.evaluate_expr(left, row, schema, params)?;
                        if l { return Ok(true); }
                        return self.evaluate_expr(right, row, schema, params);
                    }
                    _ => {}
                }

                let left_val = self.evaluate_value(left, row, schema, params)?;
                let right_val = self.evaluate_value(right, row, schema, params)?;

                match op {
                    BinaryOperator::Eq => Ok(left_val == right_val),
                    BinaryOperator::NotEq => Ok(left_val != right_val),
                    BinaryOperator::Gt => self.compare_values(&left_val, &right_val, |l, r| l > r),
                    BinaryOperator::Lt => self.compare_values(&left_val, &right_val, |l, r| l < r),
                    BinaryOperator::GtEq => {
                        self.compare_values(&left_val, &right_val, |l, r| l >= r)
                    }
                    BinaryOperator::LtEq => {
                        self.compare_values(&left_val, &right_val, |l, r| l <= r)
                    }
                    _ => Err(FusionError::Execution(format!(
                        "Unsupported operator: {}",
                        op
                    ))),
                }
            }
            Expr::MatchAgainst {
                columns,
                match_value,
                ..
            } => {
                let search_terms = if let SqlValue::SingleQuotedString(s) = match_value {
                    Self::tokenize(s)
                } else if let SqlValue::Placeholder(p) = match_value {
                    let idx = p.replace("$", "").parse::<usize>().unwrap_or(0);
                    if idx > 0 && idx <= params.len() {
                        if let Value::String(s) = &params[idx - 1] {
                            Self::tokenize(s)
                        } else {
                            return Err(FusionError::Execution(
                                "MATCH AGAINST parameter must be a string".to_string(),
                            ));
                        }
                    } else {
                        return Err(FusionError::Execution(
                            "Invalid parameter index".to_string(),
                        ));
                    }
                } else {
                    return Err(FusionError::Execution(
                        "MATCH AGAINST requires a string literal or placeholder".to_string(),
                    ));
                };

                if search_terms.is_empty() {
                    return Ok(false);
                }

                if columns.len() != 1 {
                    return Err(FusionError::Execution(
                        "MATCH currently supports only single column".to_string(),
                    ));
                }
                let col_ident = &columns[0];
                let col_name = col_ident.to_string();

                let col_idx = self.resolve_column_index(&col_name, schema)?;
                let val = &row[col_idx];

                if let Value::String(text) = val {
                    let text_tokens: HashSet<String> = Self::tokenize(text).into_iter().collect();
                    for term in search_terms {
                        if !text_tokens.contains(&term) {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                let mut found = false;
                for item in list {
                    let item_val = self.evaluate_value(item, row, schema, params)?;
                    if val == item_val {
                        found = true;
                        break;
                    }
                }
                if *negated {
                    Ok(!found)
                } else {
                    Ok(found)
                }
            }
            Expr::Like { expr, pattern, negated, .. } => {
                let s = self.evaluate_value(expr, row, schema, params)?;
                let p = self.evaluate_value(pattern, row, schema, params)?;
                if let (Value::String(s_str), Value::String(p_str)) = (s, p) {
                    let matched = Self::like_match(&s_str, &p_str);
                    if *negated {
                        Ok(!matched)
                    } else {
                        Ok(matched)
                    }
                } else {
                    Ok(false)
                }
            }
            Expr::ILike { expr, pattern, negated, .. } => {
                let s = self.evaluate_value(expr, row, schema, params)?;
                let p = self.evaluate_value(pattern, row, schema, params)?;
                if let (Value::String(s_str), Value::String(p_str)) = (s, p) {
                    let matched = Self::like_match(&s_str.to_lowercase(), &p_str.to_lowercase());
                    Ok(if *negated { !matched } else { matched })
                } else {
                    Ok(false)
                }
            }
            Expr::IsNull(expr) => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                Ok(val == Value::Null)
            }
            Expr::IsNotNull(expr) => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                Ok(val != Value::Null)
            }
            Expr::Between { expr, negated, low, high } => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                let low_val = self.evaluate_value(low, row, schema, params)?;
                let high_val = self.evaluate_value(high, row, schema, params)?;
                let ge = self.compare_values(&val, &low_val, |l, r| l >= r)?;
                let le = self.compare_values(&val, &high_val, |l, r| l <= r)?;
                let result = ge && le;
                Ok(if *negated { !result } else { result })
            }
            Expr::Nested(inner) => self.evaluate_expr(inner, row, schema, params),
            Expr::UnaryOp { op, expr } => {
                 match op {
                     sqlparser::ast::UnaryOperator::Not => {
                         let res = self.evaluate_expr(expr, row, schema, params)?;
                         Ok(!res)
                     }
                     _ => Err(FusionError::Execution("Unsupported unary operator in boolean expression".to_string())),
                 }
            }
            Expr::IsFalse(inner) => {
                let val = self.evaluate_value(inner, row, schema, params)?;
                Ok(val == Value::Boolean(false))
            }
            Expr::IsTrue(inner) => {
                let val = self.evaluate_value(inner, row, schema, params)?;
                Ok(val == Value::Boolean(true))
            }
            Expr::Value(v) => {
                match &v.value {
                    SqlValue::Boolean(b) => Ok(*b),
                    _ => Err(FusionError::Execution(format!("Cannot use {:?} as boolean", v.value))),
                }
            }
            _ => {
                // Fallback: try evaluate_value and check if it's a boolean
                match self.evaluate_value(expr, row, schema, params) {
                    Ok(Value::Boolean(b)) => Ok(b),
                    Ok(_) => Err(FusionError::Execution(format!("Unsupported expression type: {}", expr))),
                    Err(e) => Err(e),
                }
            }
        }
    }

    pub(crate) fn evaluate_value(
        &self,
        expr: &Expr,
        row: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Result<Value> {
        match expr {
            Expr::Identifier(ident) => {
                let col_name = ident.value.clone();
                let idx = self.resolve_column_index(&col_name, schema)?;
                Ok(row[idx].clone())
            }
            Expr::CompoundIdentifier(idents) => {
                let col_name = idents
                    .iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                let idx = self.resolve_column_index(&col_name, schema)?;
                Ok(row[idx].clone())
            }
            Expr::Function(func) => self.evaluate_function(func, row, schema, params),
            Expr::Ceil { expr, .. } => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                match val {
                    Value::Float(f) => Ok(Value::Integer(f.ceil() as i64)),
                    Value::Integer(n) => Ok(Value::Integer(n)),
                    _ => Ok(Value::Null),
                }
            }
            Expr::Floor { expr, .. } => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                match val {
                    Value::Float(f) => Ok(Value::Integer(f.floor() as i64)),
                    Value::Integer(n) => Ok(Value::Integer(n)),
                    _ => Ok(Value::Null),
                }
            }
            Expr::Nested(inner) => self.evaluate_value(inner, row, schema, params),
            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                match op {
                    sqlparser::ast::UnaryOperator::Minus => match val {
                        Value::Integer(n) => Ok(Value::Integer(-n)),
                        Value::Float(f) => Ok(Value::Float(-f)),
                        _ => Ok(Value::Null),
                    },
                    sqlparser::ast::UnaryOperator::Plus => Ok(val),
                    sqlparser::ast::UnaryOperator::Not => match val {
                        Value::Boolean(b) => Ok(Value::Boolean(!b)),
                        _ => Ok(Value::Null),
                    },
                    _ => Err(FusionError::Execution(format!("Unsupported unary op: {}", op))),
                }
            }
            Expr::Array(arr) => {
                let mut values = Vec::new();
                for elem in &arr.elem {
                    values.push(self.evaluate_value(elem, row, schema, params)?);
                }
                Ok(Value::Array(values))
            }
            Expr::Value(v) => {
                if let SqlValue::Placeholder(p) = &v.value {
                    let idx = p.replace("$", "").parse::<usize>().unwrap_or(0);
                    if idx > 0 && idx <= params.len() {
                        Ok(params[idx - 1].clone())
                    } else {
                        Err(FusionError::Execution(format!(
                            "Invalid parameter placeholder: {}",
                            p
                        )))
                    }
                } else {
                    Ok(self.sql_value_to_fusion_value(&v.value))
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_value(left, row, schema, params)?;
                let right_val = self.evaluate_value(right, row, schema, params)?;
                match op {
                    BinaryOperator::Plus => {
                        self.compute_math_op(&left_val, &right_val, |a, b| a + b, |a, b| a + b)
                    }
                    BinaryOperator::Minus => {
                        self.compute_math_op(&left_val, &right_val, |a, b| a - b, |a, b| a - b)
                    }
                    BinaryOperator::Multiply => {
                        self.compute_math_op(&left_val, &right_val, |a, b| a * b, |a, b| a * b)
                    }
                    BinaryOperator::Divide => {
                        match &right_val {
                            Value::Integer(0) | Value::Float(0.0) => {
                                return Err(FusionError::Execution("Division by zero".to_string()))
                            }
                            _ => {}
                        }
                        self.compute_math_op(&left_val, &right_val, |a, b| a / b, |a, b| a / b)
                    }
                    BinaryOperator::Modulo => {
                        self.compute_math_op(&left_val, &right_val, |a, b| a % b, |a, b| a % b)
                    }
                    BinaryOperator::StringConcat => {
                        let l = match left_val {
                            Value::String(s) => s,
                            Value::Integer(n) => n.to_string(),
                            Value::Float(f) => f.to_string(),
                            Value::Boolean(b) => b.to_string(),
                            Value::Null => return Ok(Value::Null),
                            other => format!("{:?}", other),
                        };
                        let r = match right_val {
                            Value::String(s) => s,
                            Value::Integer(n) => n.to_string(),
                            Value::Float(f) => f.to_string(),
                            Value::Boolean(b) => b.to_string(),
                            Value::Null => return Ok(Value::Null),
                            other => format!("{:?}", other),
                        };
                        Ok(Value::String(format!("{}{}", l, r)))
                    }
                    BinaryOperator::Arrow => {
                        if let Value::Object(map) = left_val {
                            if let Value::String(key) = right_val {
                                Ok(map.get(&key).cloned().unwrap_or(Value::Null))
                            } else {
                                Err(FusionError::Execution(
                                    "JSON key must be string".to_string(),
                                ))
                            }
                        } else {
                            Ok(Value::Null)
                        }
                    }
                    BinaryOperator::LongArrow => {
                        if let Value::Object(map) = left_val {
                            if let Value::String(key) = right_val {
                                let v = map.get(&key).cloned().unwrap_or(Value::Null);
                                if let Value::String(s) = v {
                                    Ok(Value::String(s))
                                } else {
                                    Ok(Value::String(v.to_string()))
                                }
                            } else {
                                Err(FusionError::Execution(
                                    "JSON key must be string".to_string(),
                                ))
                            }
                        } else {
                            Ok(Value::Null)
                        }
                    }
                    _ => Err(FusionError::Execution(format!(
                        "Unsupported operator in value expression: {}",
                        op
                    ))),
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    // Simple CASE: CASE expr WHEN val1 THEN res1 ...
                    let op_val = self.evaluate_value(op, row, schema, params)?;
                    for cw in conditions {
                        let cond_val = self.evaluate_value(&cw.condition, row, schema, params)?;
                        if op_val == cond_val {
                            return self.evaluate_value(&cw.result, row, schema, params);
                        }
                    }
                } else {
                    // Searched CASE: CASE WHEN cond1 THEN res1 ...
                    for cw in conditions {
                        if self.evaluate_expr(&cw.condition, row, schema, params)? {
                            return self.evaluate_value(&cw.result, row, schema, params);
                        }
                    }
                }
                if let Some(else_expr) = else_result {
                    self.evaluate_value(else_expr, row, schema, params)
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::Cast { expr, data_type, .. } => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                let type_str = format!("{}", data_type).to_uppercase();
                match type_str.as_str() {
                    "INT" | "INTEGER" | "BIGINT" | "INT4" | "INT8" | "SMALLINT" => match val {
                        Value::Integer(_) => Ok(val),
                        Value::Float(f) => Ok(Value::Integer(f as i64)),
                        Value::String(s) => s.trim().parse::<i64>()
                            .map(Value::Integer)
                            .map_err(|_| FusionError::Execution(format!("Cannot cast '{}' to INTEGER", s))),
                        Value::Boolean(b) => Ok(Value::Integer(if b { 1 } else { 0 })),
                        Value::Null => Ok(Value::Null),
                        _ => Err(FusionError::Execution(format!("Cannot cast {:?} to INTEGER", val))),
                    },
                    s if s.starts_with("FLOAT") || s.starts_with("DOUBLE") || s.starts_with("REAL") || s.starts_with("NUMERIC") || s.starts_with("DECIMAL") => match val {
                        Value::Float(_) => Ok(val),
                        Value::Integer(n) => Ok(Value::Float(n as f64)),
                        Value::String(s) => s.trim().parse::<f64>()
                            .map(Value::Float)
                            .map_err(|_| FusionError::Execution(format!("Cannot cast '{}' to FLOAT", s))),
                        Value::Null => Ok(Value::Null),
                        _ => Err(FusionError::Execution(format!("Cannot cast {:?} to FLOAT", val))),
                    },
                    "TEXT" | "VARCHAR" | "CHAR" | "STRING" => match val {
                        Value::String(_) => Ok(val),
                        Value::Integer(n) => Ok(Value::String(n.to_string())),
                        Value::Float(f) => Ok(Value::String(f.to_string())),
                        Value::Boolean(b) => Ok(Value::String(b.to_string())),
                        Value::Null => Ok(Value::Null),
                        other => Ok(Value::String(format!("{:?}", other))),
                    },
                    s if s.starts_with("VARCHAR") || s.starts_with("CHAR(") => match val {
                        Value::String(_) => Ok(val),
                        Value::Integer(n) => Ok(Value::String(n.to_string())),
                        Value::Float(f) => Ok(Value::String(f.to_string())),
                        Value::Boolean(b) => Ok(Value::String(b.to_string())),
                        Value::Null => Ok(Value::Null),
                        other => Ok(Value::String(format!("{:?}", other))),
                    },
                    "BOOLEAN" | "BOOL" => match val {
                        Value::Boolean(_) => Ok(val),
                        Value::Integer(n) => Ok(Value::Boolean(n != 0)),
                        Value::String(s) => match s.to_lowercase().as_str() {
                            "true" | "t" | "1" | "yes" => Ok(Value::Boolean(true)),
                            "false" | "f" | "0" | "no" => Ok(Value::Boolean(false)),
                            _ => Err(FusionError::Execution(format!("Cannot cast '{}' to BOOLEAN", s))),
                        },
                        Value::Null => Ok(Value::Null),
                        _ => Err(FusionError::Execution(format!("Cannot cast {:?} to BOOLEAN", val))),
                    },
                    _ => Err(FusionError::Execution(format!("Unsupported CAST target type: {}", type_str))),
                }
            }
            Expr::Between { expr, negated, low, high } => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                let low_val = self.evaluate_value(low, row, schema, params)?;
                let high_val = self.evaluate_value(high, row, schema, params)?;
                let ge = self.compare_values(&val, &low_val, |l, r| l >= r)?;
                let le = self.compare_values(&val, &high_val, |l, r| l <= r)?;
                let result = ge && le;
                Ok(Value::Boolean(if *negated { !result } else { result }))
            }
            Expr::IsFalse(inner) => {
                let val = self.evaluate_value(inner, row, schema, params)?;
                Ok(Value::Boolean(val == Value::Boolean(false)))
            }
            Expr::IsTrue(inner) => {
                let val = self.evaluate_value(inner, row, schema, params)?;
                Ok(Value::Boolean(val == Value::Boolean(true)))
            }
            Expr::IsNull(inner) => {
                let val = self.evaluate_value(inner, row, schema, params)?;
                Ok(Value::Boolean(val == Value::Null))
            }
            Expr::IsNotNull(inner) => {
                let val = self.evaluate_value(inner, row, schema, params)?;
                Ok(Value::Boolean(val != Value::Null))
            }
            _ => Err(FusionError::Execution(format!(
                "Unsupported value expression: {:?}",
                expr
            ))),
        }
    }

    pub(crate) fn evaluate_binary_op(
        &self,
        left_val: Value,
        op: &BinaryOperator,
        right_val: Value,
    ) -> Result<Value> {
        match op {
            BinaryOperator::Plus => match (left_val, right_val) {
                (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l + r)),
                (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l + r)),
                (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 + r)),
                (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l + r as f64)),
                _ => Err(FusionError::Execution(
                    "Type mismatch in addition".to_string(),
                )),
            },
            BinaryOperator::Minus => match (left_val, right_val) {
                (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l - r)),
                (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l - r)),
                (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 - r)),
                (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l - r as f64)),
                _ => Err(FusionError::Execution(
                    "Type mismatch in subtraction".to_string(),
                )),
            },
            BinaryOperator::Multiply => match (left_val, right_val) {
                (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l * r)),
                (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l * r)),
                (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 * r)),
                (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l * r as f64)),
                _ => Err(FusionError::Execution(
                    "Type mismatch in multiplication".to_string(),
                )),
            },
            BinaryOperator::Divide => match (left_val, right_val) {
                (Value::Integer(l), Value::Integer(r)) => {
                    if r == 0 {
                        return Err(FusionError::Execution("Division by zero".to_string()));
                    }
                    Ok(Value::Integer(l / r))
                }
                (Value::Float(l), Value::Float(r)) => {
                    if r == 0.0 {
                        return Err(FusionError::Execution("Division by zero".to_string()));
                    }
                    Ok(Value::Float(l / r))
                }
                (Value::Integer(l), Value::Float(r)) => {
                    if r == 0.0 {
                        return Err(FusionError::Execution("Division by zero".to_string()));
                    }
                    Ok(Value::Float(l as f64 / r))
                }
                (Value::Float(l), Value::Integer(r)) => {
                    if r == 0 {
                        return Err(FusionError::Execution("Division by zero".to_string()));
                    }
                    Ok(Value::Float(l / r as f64))
                }
                _ => Err(FusionError::Execution(
                    "Type mismatch in division".to_string(),
                )),
            },
            BinaryOperator::Eq => Ok(Value::Boolean(left_val == right_val)),
            BinaryOperator::NotEq => Ok(Value::Boolean(left_val != right_val)),
            BinaryOperator::Gt => Ok(Value::Boolean(
                left_val.compare(&right_val) == Ordering::Greater,
            )),
            BinaryOperator::Lt => Ok(Value::Boolean(
                left_val.compare(&right_val) == Ordering::Less,
            )),
            BinaryOperator::GtEq => Ok(Value::Boolean(
                left_val.compare(&right_val) != Ordering::Less,
            )),
            BinaryOperator::LtEq => Ok(Value::Boolean(
                left_val.compare(&right_val) != Ordering::Greater,
            )),
            _ => Err(FusionError::Execution(format!(
                "Unsupported operator: {}",
                op
            ))),
        }
    }

    pub(crate) fn like_match(text: &str, pattern: &str) -> bool {
        let text_chars: Vec<char> = text.chars().collect();
        let pattern_chars: Vec<char> = pattern.chars().collect();
        let mut t_idx = 0;
        let mut p_idx = 0;
        let mut t_backup = None;
        let mut p_backup = None;

        while t_idx < text_chars.len() {
            if p_idx < pattern_chars.len() && (pattern_chars[p_idx] == '?' || pattern_chars[p_idx] == text_chars[t_idx]) {
                t_idx += 1;
                p_idx += 1;
            } else if p_idx < pattern_chars.len() && pattern_chars[p_idx] == '_' {
                 t_idx += 1;
                 p_idx += 1;
            } else if p_idx < pattern_chars.len() && pattern_chars[p_idx] == '%' {
                p_backup = Some(p_idx + 1);
                p_idx += 1;
                t_backup = Some(t_idx + 1); 
            } else if let Some(p_back) = p_backup {
                p_idx = p_back;
                t_idx = t_backup.unwrap();
                t_backup = Some(t_idx + 1);
            } else {
                return false;
            }
        }

        while p_idx < pattern_chars.len() && pattern_chars[p_idx] == '%' {
            p_idx += 1;
        }

        p_idx == pattern_chars.len()
    }

    pub(crate) fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    pub(crate) fn resolve_column_index(&self, col_name: &str, schema: &TableSchema) -> Result<usize> {
        if let Some(idx) = schema.columns.iter().position(|c| c.name == col_name) {
            return Ok(idx);
        }

        if !col_name.contains('.') {
            let suffix = format!(".{}", col_name);
            let matches: Vec<usize> = schema
                .columns
                .iter()
                .enumerate()
                .filter(|(_, c)| c.name.ends_with(&suffix) || c.name == col_name)
                .map(|(i, _)| i)
                .collect();

            if matches.len() == 1 {
                return Ok(matches[0]);
            } else if matches.len() > 1 {
                return Err(FusionError::Execution(format!(
                    "Ambiguous column name: {}",
                    col_name
                )));
            }
        }

        Err(FusionError::Execution(format!(
            "Column {} not found",
            col_name
        )))
    }

    pub(crate) fn compute_math_op<I, F>(
        &self,
        left: &Value,
        right: &Value,
        op_int: I,
        op_float: F,
    ) -> Result<Value>
    where
        I: Fn(i64, i64) -> i64,
        F: Fn(f64, f64) -> f64,
    {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(op_int(*l, *r))),
            (Value::Float(l), Value::Float(r)) => Ok(Value::Float(op_float(*l, *r))),
            (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(op_float(*l as f64, *r))),
            (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(op_float(*l, *r as f64))),
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ => Err(FusionError::Execution(
                "Type mismatch in arithmetic operation".to_string(),
            )),
        }
    }

    pub(crate) fn compare_values<CF>(&self, left: &Value, right: &Value, op: CF) -> Result<bool>
    where
        CF: Fn(&f64, &f64) -> bool,
    {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Ok(op(&(*l as f64), &(*r as f64))),
            (Value::Float(l), Value::Float(r)) => Ok(op(l, r)),
            (Value::Integer(l), Value::Float(r)) => Ok(op(&(*l as f64), r)),
            (Value::Float(l), Value::Integer(r)) => Ok(op(l, &(*r as f64))),
            _ => Err(FusionError::Execution(
                "Type mismatch in comparison".to_string(),
            )),
        }
    }

    pub(crate) fn sql_value_to_fusion_value(&self, v: &SqlValue) -> Value {
        match v {
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Value::Integer(i)
                } else if let Ok(f) = n.parse::<f64>() {
                    Value::Float(f)
                } else {
                    Value::Null
                }
            }
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                if s.trim().starts_with('{') && s.trim().ends_with('}') {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
                        return self.json_value_to_fusion_value(&v);
                    }
                }
                Value::String(s.clone())
            }
            SqlValue::Boolean(b) => Value::Boolean(*b),
            SqlValue::Null => Value::Null,
            _ => Value::Null,
        }
    }

    pub(crate) fn json_value_to_fusion_value(&self, v: &serde_json::Value) -> Value {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Boolean(*b),
            serde_json::Value::Number(n) => {
                if n.is_i64() {
                    Value::Integer(n.as_i64().unwrap())
                } else {
                    Value::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => Value::String(s.clone()),
            serde_json::Value::Array(arr) => Value::Array(
                arr.iter()
                    .map(|x| self.json_value_to_fusion_value(x))
                    .collect(),
            ),
            serde_json::Value::Object(obj) => {
                let mut map = std::collections::HashMap::new();
                for (k, v) in obj {
                    map.insert(k.clone(), self.json_value_to_fusion_value(v));
                }
                Value::Object(map)
            }
        }
    }

    pub(crate) fn evaluate_function(
        &self,
        func: &Function,
        row: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Result<Value> {
        let name = func.name.to_string().to_uppercase();

        let args = match &func.args {
            FunctionArguments::List(list) => &list.args,
            _ => {
                return Err(FusionError::Execution(
                    "Unsupported function argument format".to_string(),
                ))
            }
        };

        match name.as_str() {
            "VECTOR_DISTANCE" => {
                if args.len() != 2 {
                    return Err(FusionError::Execution(
                        "VECTOR_DISTANCE requires 2 arguments".to_string(),
                    ));
                }

                let v1 = self.evaluate_arg(&args[0], row, schema, params)?;
                let v2 = self.evaluate_arg(&args[1], row, schema, params)?;

                self.compute_vector_distance(&v1, &v2)
            }
            "EMBEDDING" => {
                if args.len() != 1 {
                    return Err(FusionError::Execution(
                        "EMBEDDING requires 1 argument (text)".to_string(),
                    ));
                }
                let text_val = self.evaluate_arg(&args[0], row, schema, params)?;
                let text = match &text_val {
                    Value::String(s) => s.clone(),
                    other => format!("{:?}", other),
                };
                match self.embedding_registry.embed(&text) {
                    Some(vec) => Ok(Value::Vector(vec)),
                    None => Err(FusionError::Execution(
                        "No embedding provider available".to_string(),
                    )),
                }
            }
            "COSINE_SIMILARITY" => {
                if args.len() != 2 {
                    return Err(FusionError::Execution(
                        "COSINE_SIMILARITY requires 2 arguments".to_string(),
                    ));
                }
                let v1 = self.evaluate_arg(&args[0], row, schema, params)?;
                let v2 = self.evaluate_arg(&args[1], row, schema, params)?;
                let vec1 = self.extract_vector(&v1)?;
                let vec2 = self.extract_vector(&v2)?;
                if vec1.len() != vec2.len() {
                    return Err(FusionError::Execution(
                        "Vector dimensions mismatch".to_string(),
                    ));
                }
                let dot: f64 = vec1.iter().zip(vec2.iter()).map(|(a, b)| a * b).sum();
                let norm1: f64 = vec1.iter().map(|x| x * x).sum::<f64>().sqrt();
                let norm2: f64 = vec2.iter().map(|x| x * x).sum::<f64>().sqrt();
                let sim = if norm1 > 0.0 && norm2 > 0.0 {
                    dot / (norm1 * norm2)
                } else {
                    0.0
                };
                Ok(Value::Float(sim as f64))
            }
            "UPPER" => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::String(s) => Ok(Value::String(s.to_uppercase())),
                    _ => Ok(Value::Null),
                }
            }
            "LOWER" => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::String(s) => Ok(Value::String(s.to_lowercase())),
                    _ => Ok(Value::Null),
                }
            }
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::String(s) => Ok(Value::Integer(s.len() as i64)),
                    _ => Ok(Value::Null),
                }
            }
            "CONCAT" => {
                let mut result = String::new();
                for arg in args {
                    let val = self.evaluate_arg(arg, row, schema, params)?;
                    match val {
                        Value::String(s) => result.push_str(&s),
                        Value::Integer(n) => result.push_str(&n.to_string()),
                        Value::Float(f) => result.push_str(&f.to_string()),
                        Value::Boolean(b) => result.push_str(&b.to_string()),
                        Value::Null => {}
                        _ => result.push_str(&format!("{:?}", val)),
                    }
                }
                Ok(Value::String(result))
            }
            "COALESCE" => {
                for arg in args {
                    let val = self.evaluate_arg(arg, row, schema, params)?;
                    if val != Value::Null {
                        return Ok(val);
                    }
                }
                Ok(Value::Null)
            }
            "NULLIF" => {
                if args.len() != 2 {
                    return Err(FusionError::Execution("NULLIF requires 2 arguments".to_string()));
                }
                let v1 = self.evaluate_arg(&args[0], row, schema, params)?;
                let v2 = self.evaluate_arg(&args[1], row, schema, params)?;
                if v1 == v2 { Ok(Value::Null) } else { Ok(v1) }
            }
            "SUBSTRING" | "SUBSTR" => {
                if args.is_empty() || args.len() > 3 {
                    return Err(FusionError::Execution("SUBSTRING requires 1-3 arguments".to_string()));
                }
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                let s = match val { Value::String(s) => s, _ => return Ok(Value::Null) };
                let start = if args.len() >= 2 {
                    match self.evaluate_arg(&args[1], row, schema, params)? {
                        Value::Integer(n) => (n - 1).max(0) as usize,
                        _ => 0,
                    }
                } else { 0 };
                let len = if args.len() == 3 {
                    match self.evaluate_arg(&args[2], row, schema, params)? {
                        Value::Integer(n) => Some(n.max(0) as usize),
                        _ => None,
                    }
                } else { None };
                let chars: Vec<char> = s.chars().collect();
                let end = len.map(|l| (start + l).min(chars.len())).unwrap_or(chars.len());
                let result: String = chars[start.min(chars.len())..end].iter().collect();
                Ok(Value::String(result))
            }
            "REPLACE" => {
                if args.len() != 3 {
                    return Err(FusionError::Execution("REPLACE requires 3 arguments".to_string()));
                }
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                let from = self.evaluate_arg(&args[1], row, schema, params)?;
                let to = self.evaluate_arg(&args[2], row, schema, params)?;
                match (val, from, to) {
                    (Value::String(s), Value::String(f), Value::String(t)) => {
                        Ok(Value::String(s.replace(&f, &t)))
                    }
                    _ => Ok(Value::Null),
                }
            }
            "TRIM" => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::String(s) => Ok(Value::String(s.trim().to_string())),
                    _ => Ok(Value::Null),
                }
            }
            "ABS" => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::Integer(n) => Ok(Value::Integer(n.abs())),
                    Value::Float(f) => Ok(Value::Float(f.abs())),
                    _ => Ok(Value::Null),
                }
            }
            "ROUND" => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                let precision = if args.len() >= 2 {
                    match self.evaluate_arg(&args[1], row, schema, params)? {
                        Value::Integer(n) => n as i32,
                        _ => 0,
                    }
                } else { 0 };
                match val {
                    Value::Float(f) => {
                        let factor = 10f64.powi(precision);
                        Ok(Value::Float((f * factor).round() / factor))
                    }
                    Value::Integer(n) => Ok(Value::Integer(n)),
                    _ => Ok(Value::Null),
                }
            }
            "CEIL" | "CEILING" => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::Float(f) => Ok(Value::Integer(f.ceil() as i64)),
                    Value::Integer(n) => Ok(Value::Integer(n)),
                    _ => Ok(Value::Null),
                }
            }
            "FLOOR" => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::Float(f) => Ok(Value::Integer(f.floor() as i64)),
                    Value::Integer(n) => Ok(Value::Integer(n)),
                    _ => Ok(Value::Null),
                }
            }
            "MOD" => {
                if args.len() < 2 {
                    return Err(FusionError::Execution("MOD requires 2 arguments".to_string()));
                }
                let a = self.evaluate_arg(&args[0], row, schema, params)?;
                let b = self.evaluate_arg(&args[1], row, schema, params)?;
                match (a, b) {
                    (Value::Integer(a), Value::Integer(b)) if b != 0 => Ok(Value::Integer(a % b)),
                    (Value::Float(a), Value::Float(b)) if b != 0.0 => Ok(Value::Float(a % b)),
                    (Value::Integer(a), Value::Float(b)) if b != 0.0 => Ok(Value::Float(a as f64 % b)),
                    (Value::Float(a), Value::Integer(b)) if b != 0 => Ok(Value::Float(a % b as f64)),
                    _ => Ok(Value::Null),
                }
            }
            "POWER" | "POW" => {
                if args.len() < 2 {
                    return Err(FusionError::Execution("POWER requires 2 arguments".to_string()));
                }
                let base = self.evaluate_arg(&args[0], row, schema, params)?;
                let exp = self.evaluate_arg(&args[1], row, schema, params)?;
                let b = match base { Value::Integer(n) => n as f64, Value::Float(f) => f, _ => return Ok(Value::Null) };
                let e = match exp { Value::Integer(n) => n as f64, Value::Float(f) => f, _ => return Ok(Value::Null) };
                Ok(Value::Float(b.powf(e)))
            }
            "SQRT" => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::Integer(n) => Ok(Value::Float((n as f64).sqrt())),
                    Value::Float(f) => Ok(Value::Float(f.sqrt())),
                    _ => Ok(Value::Null),
                }
            }
            "NOW" | "CURRENT_TIMESTAMP" => {
                let dur = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                Ok(Value::Integer(dur.as_secs() as i64))
            }
            "CURRENT_DATE" => {
                let dur = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let days = dur.as_secs() / 86400;
                Ok(Value::Integer(days as i64))
            }
            _ => Err(FusionError::Execution(format!(
                "Unsupported function: {}",
                name
            ))),
        }
    }

    pub(crate) fn evaluate_arg(
        &self,
        arg: &FunctionArg,
        row: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Result<Value> {
        match arg {
            FunctionArg::Named { arg, .. } => self.evaluate_arg_expr(arg, row, schema, params),
            FunctionArg::Unnamed(arg) => self.evaluate_arg_expr(arg, row, schema, params),
            _ => Err(FusionError::Execution(
                "Unsupported function argument type".to_string(),
            )),
        }
    }

    pub(crate) fn evaluate_arg_expr(
        &self,
        arg_expr: &FunctionArgExpr,
        row: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Result<Value> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => self.evaluate_value(expr, row, schema, params),
            _ => Err(FusionError::Execution(
                "Unsupported function argument type".to_string(),
            )),
        }
    }

    pub(crate) fn compute_vector_distance(&self, v1: &Value, v2: &Value) -> Result<Value> {
        let vec1 = self.extract_vector(v1)?;
        let vec2 = self.extract_vector(v2)?;

        if vec1.len() != vec2.len() {
            return Err(FusionError::Execution(
                "Vector dimensions mismatch".to_string(),
            ));
        }

        let mut sum_sq = 0.0;
        for (a, b) in vec1.iter().zip(vec2.iter()) {
            sum_sq += (a - b).powi(2);
        }

        Ok(Value::Float(sum_sq.sqrt()))
    }

    pub(crate) fn extract_vector(&self, v: &Value) -> Result<Vec<f64>> {
        match v {
            Value::Vector(vec) => Ok(vec.iter().map(|&x| x as f64).collect()),
            Value::Array(arr) => {
                let mut res = Vec::new();
                for item in arr {
                    match item {
                        Value::Integer(i) => res.push(*i as f64),
                        Value::Float(f) => res.push(*f),
                        _ => {
                            return Err(FusionError::Execution(
                                "Vector elements must be numbers".to_string(),
                            ))
                        }
                    }
                }
                Ok(res)
            }
            _ => Err(FusionError::Execution(format!(
                "Value is not a vector: {:?}",
                v
            ))),
        }
    }

    pub(crate) fn compare_for_sort(&self, v1: &Value, v2: &Value) -> Ordering {
        v1.compare(v2)
    }

    #[allow(dead_code)]
    pub(crate) fn get_type_order(&self, v: &Value) -> u8 {
        v.get_type_order()
    }

    pub(crate) fn value_to_index_string(&self, val: &Value) -> Option<String> {
        match val {
            Value::Integer(i) => Some(crate::common::encoding::encode_i64_comparable(*i)),
            Value::String(s) => Some(s.clone()),
            Value::Boolean(b) => Some(b.to_string()),
            _ => None,
        }
    }

    pub(crate) fn extract_aggregates_from_expr(&self, expr: &Expr, aggregates: &mut Vec<(Expr, String)>) {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                if matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "STRING_AGG" | "GROUP_CONCAT") {
                    // Check for DISTINCT modifier (e.g., COUNT(DISTINCT col))
                    let is_distinct = if let FunctionArguments::List(args) = &func.args {
                        args.duplicate_treatment == Some(sqlparser::ast::DuplicateTreatment::Distinct)
                    } else {
                        false
                    };
                    let effective_name = if is_distinct && name == "COUNT" {
                        "COUNT_DISTINCT".to_string()
                    } else {
                        name
                    };
                    if !aggregates.iter().any(|(e, _)| e == expr) {
                        aggregates.push((expr.clone(), effective_name));
                    }
                } else if let FunctionArguments::List(args) = &func.args {
                    for arg in &args.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            self.extract_aggregates_from_expr(e, aggregates);
                        }
                    }
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.extract_aggregates_from_expr(left, aggregates);
                self.extract_aggregates_from_expr(right, aggregates);
            }
            Expr::Nested(expr) => self.extract_aggregates_from_expr(expr, aggregates),
            Expr::UnaryOp { expr, .. } => self.extract_aggregates_from_expr(expr, aggregates),
            Expr::Cast { expr, .. } => self.extract_aggregates_from_expr(expr, aggregates),
            _ => {}
        }
    }

    pub(crate) fn extract_columns_from_expr(&self, expr: &Expr, cols: &mut HashSet<String>) {
        match expr {
            Expr::Identifier(ident) => {
                cols.insert(ident.value.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                self.extract_columns_from_expr(left, cols);
                self.extract_columns_from_expr(right, cols);
            }
            Expr::Nested(expr) => self.extract_columns_from_expr(expr, cols),
            Expr::UnaryOp { expr, .. } => self.extract_columns_from_expr(expr, cols),
            Expr::Cast { expr, .. } => self.extract_columns_from_expr(expr, cols),
            Expr::Function(func) => {
                if let FunctionArguments::List(args) = &func.args {
                    for arg in &args.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            self.extract_columns_from_expr(e, cols);
                        }
                    }
                }
            }
            Expr::InList { expr, list, .. } => {
                self.extract_columns_from_expr(expr, cols);
                for e in list {
                    self.extract_columns_from_expr(e, cols);
                }
            }
            Expr::Between { expr, low, high, .. } => {
                self.extract_columns_from_expr(expr, cols);
                self.extract_columns_from_expr(low, cols);
                self.extract_columns_from_expr(high, cols);
            }
            Expr::IsNull(expr) => self.extract_columns_from_expr(expr, cols),
            Expr::IsNotNull(expr) => self.extract_columns_from_expr(expr, cols),
            Expr::InSubquery { expr, .. } => self.extract_columns_from_expr(expr, cols),
            Expr::Like { expr, .. } => self.extract_columns_from_expr(expr, cols),
            Expr::ILike { expr, .. } => self.extract_columns_from_expr(expr, cols),
            Expr::Case { operand, conditions, else_result, .. } => {
                if let Some(op) = operand {
                    self.extract_columns_from_expr(op, cols);
                }
                for cw in conditions {
                    self.extract_columns_from_expr(&cw.condition, cols);
                    self.extract_columns_from_expr(&cw.result, cols);
                }
                if let Some(el) = else_result {
                    self.extract_columns_from_expr(el, cols);
                }
            }
            _ => {}
        }
    }

    pub(crate) fn evaluate_final_group_expr(
        &self,
        expr: &Expr,
        group_key: &[Value],
        group_exprs: &[Expr],
        agg_map: &std::collections::HashMap<Expr, Value>,
        _schema: &TableSchema,
        _params: &[Value],
    ) -> Result<Value> {
        // 1. Check if it is a pre-calculated aggregate
        if let Some(val) = agg_map.get(expr) {
            return Ok(val.clone());
        }

        // 2. Check if it matches a group expression
        if let Some(idx) = group_exprs.iter().position(|e| e == expr) {
            return Ok(group_key[idx].clone());
        }

        // 3. Recurse / Evaluate
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let l = self.evaluate_final_group_expr(left, group_key, group_exprs, agg_map, _schema, _params)?;
                let r = self.evaluate_final_group_expr(right, group_key, group_exprs, agg_map, _schema, _params)?;
                self.evaluate_binary_op(l, op, r)
            },
            Expr::Nested(e) => self.evaluate_final_group_expr(e, group_key, group_exprs, agg_map, _schema, _params),
            Expr::Value(v) => Ok(self.sql_value_to_fusion_value(&v.value)),
            Expr::Identifier(ident) => {
                Err(FusionError::Execution(format!("Column '{}' must appear in the GROUP BY clause or be used in an aggregate function", ident.value)))
            },
            Expr::UnaryOp { op, expr } => {
                 let val = self.evaluate_final_group_expr(expr, group_key, group_exprs, agg_map, _schema, _params)?;
                 match op {
                     sqlparser::ast::UnaryOperator::Minus => {
                         match val {
                             Value::Integer(i) => Ok(Value::Integer(-i)),
                             Value::Float(f) => Ok(Value::Float(-f)),
                             _ => Err(FusionError::Execution("Unary minus on non-number".to_string())),
                         }
                     },
                     _ => Err(FusionError::Execution("Unsupported unary operator in GROUP BY".to_string())),
                 }
            },
            _ => Err(FusionError::Execution("Unsupported expression in GROUP BY projection".to_string())),
        }
    }

    // Optimize: Parallel Scan for Wildcard LIKE using Rayon
    pub(crate) fn parallel_filter_rows(&self, rows: Vec<Vec<Value>>, filter_expr: &Expr, schema: &TableSchema, params: &[Value]) -> Vec<Vec<Value>> {
        use rayon::iter::IntoParallelIterator;
        use rayon::iter::ParallelIterator;
        
        rows.into_par_iter()
            .filter(|row| {
                self.evaluate_expr(filter_expr, row, schema, params).unwrap_or(false)
            })
            .collect()
    }

    /// Pre-execute subqueries in an expression tree and replace them with
    /// concrete values (InSubquery → InList, Subquery → Value).
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
