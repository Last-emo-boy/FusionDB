use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use sqlparser::ast::{BinaryOperator, Expr, Value as SqlValue};
use std::cmp::Ordering;

use super::Executor;

impl Executor {
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
                let col_name = Self::compound_identifier_name(idents);
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
                    _ => Err(FusionError::Execution(format!(
                        "Unsupported unary op: {}",
                        op
                    ))),
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
                    let idx = Self::placeholder_index(p);
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
            Expr::Cast {
                expr, data_type, ..
            } => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                let type_str = format!("{}", data_type).to_uppercase();
                match type_str.as_str() {
                    "INT" | "INTEGER" | "BIGINT" | "INT4" | "INT8" | "SMALLINT" => match val {
                        Value::Integer(_) => Ok(val),
                        Value::Float(f) => Ok(Value::Integer(f as i64)),
                        Value::String(s) => {
                            s.trim().parse::<i64>().map(Value::Integer).map_err(|_| {
                                FusionError::Execution(format!("Cannot cast '{}' to INTEGER", s))
                            })
                        }
                        Value::Boolean(b) => Ok(Value::Integer(if b { 1 } else { 0 })),
                        Value::Null => Ok(Value::Null),
                        _ => Err(FusionError::Execution(format!(
                            "Cannot cast {:?} to INTEGER",
                            val
                        ))),
                    },
                    s if s.starts_with("FLOAT")
                        || s.starts_with("DOUBLE")
                        || s.starts_with("REAL")
                        || s.starts_with("NUMERIC")
                        || s.starts_with("DECIMAL") =>
                    {
                        match val {
                            Value::Float(_) => Ok(val),
                            Value::Integer(n) => Ok(Value::Float(n as f64)),
                            Value::String(s) => {
                                s.trim().parse::<f64>().map(Value::Float).map_err(|_| {
                                    FusionError::Execution(format!("Cannot cast '{}' to FLOAT", s))
                                })
                            }
                            Value::Null => Ok(Value::Null),
                            _ => Err(FusionError::Execution(format!(
                                "Cannot cast {:?} to FLOAT",
                                val
                            ))),
                        }
                    }
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
                            _ => Err(FusionError::Execution(format!(
                                "Cannot cast '{}' to BOOLEAN",
                                s
                            ))),
                        },
                        Value::Null => Ok(Value::Null),
                        _ => Err(FusionError::Execution(format!(
                            "Cannot cast {:?} to BOOLEAN",
                            val
                        ))),
                    },
                    _ => Err(FusionError::Execution(format!(
                        "Unsupported CAST target type: {}",
                        type_str
                    ))),
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
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

    pub(crate) fn resolve_column_index(
        &self,
        col_name: &str,
        schema: &TableSchema,
    ) -> Result<usize> {
        if let Some(idx) = schema.columns.iter().position(|c| c.name == col_name) {
            return Ok(idx);
        }

        let fallback_name = col_name.rsplit('.').next().unwrap_or(col_name);
        let suffix = format!(".{}", fallback_name);
        let matches: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.name == fallback_name || c.name.ends_with(&suffix))
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
}
