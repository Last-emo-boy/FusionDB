use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use chrono::{Datelike, Timelike};
use sqlparser::ast::{
    AccessExpr, BinaryOperator, DateTimeField, Expr, Subscript, TrimWhereField, Value as SqlValue,
};
use std::cmp::Ordering;
use std::fmt::Write as _;

use super::Executor;

fn decimal_index_string_for_value(decimal: &str) -> String {
    let mut value = String::with_capacity("dec:".len() + decimal.len());
    value.push_str("dec:");
    value.push_str(decimal);
    value
}

fn concat_string_values(left: &str, right: &str) -> String {
    let mut value = String::with_capacity(left.len() + right.len());
    value.push_str(left);
    value.push_str(right);
    value
}

fn append_string_concat_value(result: &mut String, value: Value) {
    match value {
        Value::String(s) => result.push_str(&s),
        Value::Integer(n) => write!(result, "{}", n).expect("writing to String cannot fail"),
        Value::Float(f) => write!(result, "{}", f).expect("writing to String cannot fail"),
        Value::Boolean(b) => write!(result, "{}", b).expect("writing to String cannot fail"),
        other => write!(result, "{:?}", other).expect("writing to String cannot fail"),
    }
}

fn concat_operator_values(left_val: Value, right_val: Value) -> Value {
    match (left_val, right_val) {
        (Value::Array(mut left), Value::Array(right)) => {
            left.extend(right);
            Value::Array(left)
        }
        (Value::Array(mut left), right) => {
            left.push(right);
            Value::Array(left)
        }
        (left, Value::Array(mut right)) => {
            right.insert(0, left);
            Value::Array(right)
        }
        (Value::Null, _) | (_, Value::Null) => Value::Null,
        (Value::String(left), Value::String(right)) => {
            Value::String(concat_string_values(&left, &right))
        }
        (left, right) => {
            let mut result = String::new();
            append_string_concat_value(&mut result, left);
            append_string_concat_value(&mut result, right);
            Value::String(result)
        }
    }
}

fn column_suffix_for_fallback(fallback_name: &str) -> String {
    let mut suffix = String::with_capacity(1 + fallback_name.len());
    suffix.push('.');
    suffix.push_str(fallback_name);
    suffix
}

fn ascii_case_insensitive_ends_with(value: &str, suffix: &str) -> bool {
    let value = value.as_bytes();
    let suffix = suffix.as_bytes();
    if suffix.len() > value.len() {
        return false;
    }

    let start = value.len() - suffix.len();
    value[start..]
        .iter()
        .zip(suffix)
        .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

fn column_matches_fallback_name(column_name: &str, fallback_name: &str, suffix: &str) -> bool {
    column_name == fallback_name
        || column_name.ends_with(suffix)
        || column_name.eq_ignore_ascii_case(fallback_name)
        || ascii_case_insensitive_ends_with(column_name, suffix)
}

fn trim_string_value(
    value: &str,
    trim_where: Option<&TrimWhereField>,
    trim_chars: Option<&str>,
) -> String {
    match trim_chars {
        Some(chars) => {
            let predicate = |c: char| chars.contains(c);
            match trim_where {
                Some(TrimWhereField::Leading) => value.trim_start_matches(predicate).to_string(),
                Some(TrimWhereField::Trailing) => value.trim_end_matches(predicate).to_string(),
                _ => value.trim_matches(predicate).to_string(),
            }
        }
        None => match trim_where {
            Some(TrimWhereField::Leading) => value.trim_start().to_string(),
            Some(TrimWhereField::Trailing) => value.trim_end().to_string(),
            _ => value.trim().to_string(),
        },
    }
}

fn position_of_substring(haystack: &str, needle: &str) -> i64 {
    if needle.is_empty() {
        return 1;
    }
    match haystack.find(needle) {
        Some(byte_index) => haystack[..byte_index].chars().count() as i64 + 1,
        None => 0,
    }
}

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
                let idx = self.resolve_column_index(&ident.value, schema)?;
                Ok(row[idx].clone())
            }
            Expr::CompoundIdentifier(idents) => {
                let col_name = Self::compound_identifier_name(idents);
                let idx = self.resolve_column_index(&col_name, schema)?;
                Ok(row[idx].clone())
            }
            Expr::CompoundFieldAccess { root, access_chain } => {
                let mut value = self.evaluate_value(root, row, schema, params)?;
                for access in access_chain {
                    value = self.evaluate_access(value, access, row, schema, params)?;
                }
                Ok(value)
            }
            Expr::Function(func) => self.evaluate_function(func, row, schema, params),
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                let s = match val {
                    Value::String(s) => s,
                    _ => return Ok(Value::Null),
                };
                let start = if let Some(from) = substring_from {
                    match self.evaluate_value(from, row, schema, params)? {
                        Value::Integer(n) => (n - 1).max(0) as usize,
                        _ => 0,
                    }
                } else {
                    0
                };
                let len = if let Some(for_expr) = substring_for {
                    match self.evaluate_value(for_expr, row, schema, params)? {
                        Value::Integer(n) => Some(n.max(0) as usize),
                        _ => None,
                    }
                } else {
                    None
                };
                let chars: Vec<char> = s.chars().collect();
                let end = len
                    .map(|value| (start + value).min(chars.len()))
                    .unwrap_or(chars.len());
                Ok(Value::String(
                    chars[start.min(chars.len())..end].iter().collect(),
                ))
            }
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
                let mut values = Vec::with_capacity(arr.elem.len());
                for elem in &arr.elem {
                    values.push(self.evaluate_value(elem, row, schema, params)?);
                }
                Ok(Value::Array(values))
            }
            Expr::TypedString(typed) => {
                let raw = typed
                    .value
                    .clone()
                    .value
                    .into_string()
                    .unwrap_or_else(|| typed.value.to_string().trim_matches('\'').to_string());
                Self::typed_string_value(&typed.data_type, &raw)
            }
            Expr::Interval(interval) => {
                let raw = self.evaluate_value(&interval.value, row, schema, params)?;
                Self::coerce_value_to_sql_type(
                    raw,
                    &sqlparser::ast::DataType::Interval {
                        fields: None,
                        precision: None,
                    },
                )
            }
            Expr::Extract { field, expr, .. } => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                Self::extract_datetime_field(field, val)
            }
            Expr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                let s = match val {
                    Value::String(s) => s,
                    _ => return Ok(Value::Null),
                };
                let trim_chars = if let Some(what) = trim_what {
                    match self.evaluate_value(what, row, schema, params)? {
                        Value::String(chars) => Some(chars),
                        Value::Null => return Ok(Value::Null),
                        _ => None,
                    }
                } else if let Some(char_exprs) = trim_characters {
                    let mut chars = String::new();
                    for char_expr in char_exprs {
                        match self.evaluate_value(char_expr, row, schema, params)? {
                            Value::String(part) => chars.push_str(&part),
                            Value::Null => return Ok(Value::Null),
                            _ => {}
                        }
                    }
                    Some(chars)
                } else {
                    None
                };
                Ok(Value::String(trim_string_value(
                    &s,
                    trim_where.as_ref(),
                    trim_chars.as_deref(),
                )))
            }
            Expr::Position { expr, r#in } => {
                let needle = self.evaluate_value(expr, row, schema, params)?;
                let haystack = self.evaluate_value(r#in, row, schema, params)?;
                match (needle, haystack) {
                    (Value::String(needle), Value::String(haystack)) => {
                        Ok(Value::Integer(position_of_substring(&haystack, &needle)))
                    }
                    _ => Ok(Value::Null),
                }
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
                    BinaryOperator::Plus => self.compute_add_op(&left_val, &right_val),
                    BinaryOperator::Minus => self.compute_subtract_op(&left_val, &right_val),
                    BinaryOperator::Multiply => self.compute_multiply_op(&left_val, &right_val),
                    BinaryOperator::Divide => {
                        match &right_val {
                            Value::Integer(0) | Value::Float(0.0) => {
                                return Err(FusionError::Execution("Division by zero".to_string()));
                            }
                            _ => {}
                        }
                        self.compute_divide_op(&left_val, &right_val)
                    }
                    BinaryOperator::Modulo => {
                        self.compute_math_op(&left_val, &right_val, |a, b| a % b, |a, b| a % b)
                    }
                    BinaryOperator::StringConcat => Ok(concat_operator_values(left_val, right_val)),
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
                Self::coerce_value_to_sql_type(val, data_type)
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
                let (val, low_val) =
                    self.align_comparison_values(expr, val, low, low_val, schema)?;
                let (val, high_val) =
                    self.align_comparison_values(expr, val, high, high_val, schema)?;
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
            Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => Ok(Value::Boolean(self.evaluate_quantified_array_comparison(
                left, compare_op, right, false, row, schema, params,
            )?)),
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => Ok(Value::Boolean(self.evaluate_quantified_array_comparison(
                left, compare_op, right, true, row, schema, params,
            )?)),
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
                (Value::Decimal(l), Value::Decimal(r)) => decimal_math(&l, &r, |a, b| a + b),
                (Value::Decimal(l), r) => decimal_math(&l, &r.to_plain_string(), |a, b| a + b),
                (l, Value::Decimal(r)) => decimal_math(&l.to_plain_string(), &r, |a, b| a + b),
                _ => Err(FusionError::Execution(
                    "Type mismatch in addition".to_string(),
                )),
            },
            BinaryOperator::Minus => match (left_val, right_val) {
                (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l - r)),
                (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l - r)),
                (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 - r)),
                (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l - r as f64)),
                (Value::Decimal(l), Value::Decimal(r)) => decimal_math(&l, &r, |a, b| a - b),
                (Value::Decimal(l), r) => decimal_math(&l, &r.to_plain_string(), |a, b| a - b),
                (l, Value::Decimal(r)) => decimal_math(&l.to_plain_string(), &r, |a, b| a - b),
                _ => Err(FusionError::Execution(
                    "Type mismatch in subtraction".to_string(),
                )),
            },
            BinaryOperator::Multiply => match (left_val, right_val) {
                (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l * r)),
                (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l * r)),
                (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 * r)),
                (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l * r as f64)),
                (Value::Decimal(l), Value::Decimal(r)) => decimal_math(&l, &r, |a, b| a * b),
                (Value::Decimal(l), r) => decimal_math(&l, &r.to_plain_string(), |a, b| a * b),
                (l, Value::Decimal(r)) => decimal_math(&l.to_plain_string(), &r, |a, b| a * b),
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
                (Value::Decimal(l), Value::Decimal(r)) => {
                    if r == "0" {
                        return Err(FusionError::Execution("Division by zero".to_string()));
                    }
                    decimal_math(&l, &r, |a, b| a / b)
                }
                (Value::Decimal(l), r) => {
                    if r.as_f64() == Some(0.0) {
                        return Err(FusionError::Execution("Division by zero".to_string()));
                    }
                    decimal_math(&l, &r.to_plain_string(), |a, b| a / b)
                }
                (l, Value::Decimal(r)) => {
                    if r == "0" {
                        return Err(FusionError::Execution("Division by zero".to_string()));
                    }
                    decimal_math(&l.to_plain_string(), &r, |a, b| a / b)
                }
                _ => Err(FusionError::Execution(
                    "Type mismatch in division".to_string(),
                )),
            },
            BinaryOperator::Eq => Ok(Value::Boolean(
                left_val.compare(&right_val) == Ordering::Equal,
            )),
            BinaryOperator::NotEq => Ok(Value::Boolean(
                left_val.compare(&right_val) != Ordering::Equal,
            )),
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
        if let Some(idx) = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(col_name))
        {
            return Ok(idx);
        }

        let fallback_name = col_name.rsplit('.').next().unwrap_or(col_name);
        let suffix = column_suffix_for_fallback(fallback_name);
        let mut matches = Vec::with_capacity(schema.columns.len());
        for (index, column) in schema.columns.iter().enumerate() {
            if column_matches_fallback_name(&column.name, fallback_name, &suffix) {
                matches.push(index);
            }
        }

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
            (Value::Decimal(l), Value::Decimal(r)) => decimal_math(l, r, op_float),
            (Value::Decimal(l), r) => decimal_math(l, &r.to_plain_string(), op_float),
            (l, Value::Decimal(r)) => decimal_math(&l.to_plain_string(), r, op_float),
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ => Err(FusionError::Execution(
                "Type mismatch in arithmetic operation".to_string(),
            )),
        }
    }

    fn compute_add_op(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Timestamp(micros), Value::Interval(delta))
            | (Value::Interval(delta), Value::Timestamp(micros)) => {
                Ok(Value::Timestamp(micros.saturating_add(*delta)))
            }
            (Value::Date(days), Value::Interval(delta)) => Self::date_plus_interval(*days, *delta),
            (Value::Interval(delta), Value::Date(days)) => Self::date_plus_interval(*days, *delta),
            (Value::Interval(left), Value::Interval(right)) => {
                Ok(Value::Interval(left.saturating_add(*right)))
            }
            _ => self.compute_math_op(left, right, |a, b| a + b, |a, b| a + b),
        }
    }

    fn compute_subtract_op(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Timestamp(micros), Value::Interval(delta)) => {
                Ok(Value::Timestamp(micros.saturating_sub(*delta)))
            }
            (Value::Timestamp(left), Value::Timestamp(right)) => {
                Ok(Value::Interval(left.saturating_sub(*right)))
            }
            (Value::Date(days), Value::Interval(delta)) => {
                Self::date_plus_interval(*days, delta.saturating_neg())
            }
            (Value::Date(left), Value::Date(right)) => Ok(Value::Integer((*left - *right) as i64)),
            (Value::Interval(left), Value::Interval(right)) => {
                Ok(Value::Interval(left.saturating_sub(*right)))
            }
            _ => self.compute_math_op(left, right, |a, b| a - b, |a, b| a - b),
        }
    }

    fn compute_multiply_op(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Interval(micros), Value::Integer(multiplier))
            | (Value::Integer(multiplier), Value::Interval(micros)) => {
                Ok(Value::Interval(micros.saturating_mul(*multiplier)))
            }
            (Value::Interval(micros), Value::Float(multiplier))
            | (Value::Float(multiplier), Value::Interval(micros)) => Ok(Value::Interval(
                (*micros as f64 * *multiplier).round() as i64,
            )),
            (Value::Interval(micros), Value::Decimal(multiplier))
            | (Value::Decimal(multiplier), Value::Interval(micros)) => {
                let multiplier = multiplier.parse::<f64>().map_err(|_| {
                    FusionError::Execution("Invalid decimal interval multiplier".to_string())
                })?;
                Ok(Value::Interval((*micros as f64 * multiplier).round() as i64))
            }
            _ => self.compute_math_op(left, right, |a, b| a * b, |a, b| a * b),
        }
    }

    fn compute_divide_op(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Interval(_), Value::Integer(0)) | (Value::Interval(_), Value::Float(0.0)) => {
                Err(FusionError::Execution("Division by zero".to_string()))
            }
            (Value::Interval(micros), Value::Integer(divisor)) => {
                Ok(Value::Interval(micros / divisor))
            }
            (Value::Interval(micros), Value::Float(divisor)) => {
                Ok(Value::Interval((*micros as f64 / *divisor).round() as i64))
            }
            (Value::Interval(micros), Value::Decimal(divisor)) => {
                let divisor = divisor.parse::<f64>().map_err(|_| {
                    FusionError::Execution("Invalid decimal interval divisor".to_string())
                })?;
                if divisor == 0.0 {
                    return Err(FusionError::Execution("Division by zero".to_string()));
                }
                Ok(Value::Interval((*micros as f64 / divisor).round() as i64))
            }
            _ => self.compute_math_op(left, right, |a, b| a / b, |a, b| a / b),
        }
    }

    fn date_plus_interval(days: i32, delta_micros: i64) -> Result<Value> {
        let Some(date) = chrono::NaiveDate::from_num_days_from_ce_opt(days) else {
            return Err(FusionError::Execution("Invalid DATE value".to_string()));
        };
        let Some(datetime) = date.and_hms_opt(0, 0, 0) else {
            return Err(FusionError::Execution("Invalid DATE midnight".to_string()));
        };
        Ok(Value::Timestamp(
            datetime
                .and_utc()
                .timestamp_micros()
                .saturating_add(delta_micros),
        ))
    }

    pub(crate) fn compare_values<CF>(&self, left: &Value, right: &Value, op: CF) -> Result<bool>
    where
        CF: Fn(&f64, &f64) -> bool,
    {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(false),
            (Value::Integer(l), Value::Integer(r)) => Ok(op(&(*l as f64), &(*r as f64))),
            (Value::Float(l), Value::Float(r)) => Ok(op(l, r)),
            (Value::Integer(l), Value::Float(r)) => Ok(op(&(*l as f64), r)),
            (Value::Float(l), Value::Integer(r)) => Ok(op(l, &(*r as f64))),
            (Value::Decimal(l), Value::Decimal(r)) => Ok(op(
                &l.parse::<f64>().unwrap_or_default(),
                &r.parse::<f64>().unwrap_or_default(),
            )),
            (Value::Decimal(l), r) => Ok(op(
                &l.parse::<f64>().unwrap_or_default(),
                &r.as_f64().unwrap_or_default(),
            )),
            (l, Value::Decimal(r)) => Ok(op(
                &l.as_f64().unwrap_or_default(),
                &r.parse::<f64>().unwrap_or_default(),
            )),
            (Value::Date(l), Value::Date(r)) => Ok(op(&(*l as f64), &(*r as f64))),
            (Value::Timestamp(l), Value::Timestamp(r)) => Ok(op(&(*l as f64), &(*r as f64))),
            (Value::Interval(l), Value::Interval(r)) => Ok(op(&(*l as f64), &(*r as f64))),
            _ => Err(FusionError::Execution(
                "Type mismatch in comparison".to_string(),
            )),
        }
    }

    pub(crate) fn compare_binary_predicate(
        &self,
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> Result<bool> {
        match op {
            BinaryOperator::Eq => {
                if matches!(left, Value::Null) || matches!(right, Value::Null) {
                    return Ok(false);
                }
                Ok(left.compare(right) == Ordering::Equal)
            }
            BinaryOperator::NotEq => {
                if matches!(left, Value::Null) || matches!(right, Value::Null) {
                    return Ok(false);
                }
                Ok(left.compare(right) != Ordering::Equal)
            }
            BinaryOperator::Gt => self.compare_values(left, right, |l, r| l > r),
            BinaryOperator::Lt => self.compare_values(left, right, |l, r| l < r),
            BinaryOperator::GtEq => self.compare_values(left, right, |l, r| l >= r),
            BinaryOperator::LtEq => self.compare_values(left, right, |l, r| l <= r),
            _ => Err(FusionError::Execution(format!(
                "Unsupported quantified comparison operator: {}",
                op
            ))),
        }
    }

    pub(crate) fn evaluate_quantified_array_comparison(
        &self,
        left_expr: &Expr,
        compare_op: &BinaryOperator,
        right_expr: &Expr,
        require_all: bool,
        row: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Result<bool> {
        let left_value = self.evaluate_value(left_expr, row, schema, params)?;
        let right_value = self.evaluate_value(right_expr, row, schema, params)?;
        let Value::Array(values) = right_value else {
            return Err(FusionError::Execution(format!(
                "Quantified comparison requires an array on the right side, got {:?}",
                right_value
            )));
        };

        if values.is_empty() {
            return Ok(require_all);
        }

        for value in values {
            let (left, right) = self.align_comparison_values(
                left_expr,
                left_value.clone(),
                right_expr,
                value,
                schema,
            )?;
            let matched = self.compare_binary_predicate(&left, compare_op, &right)?;
            if require_all && !matched {
                return Ok(false);
            }
            if !require_all && matched {
                return Ok(true);
            }
        }

        Ok(require_all)
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
            SqlValue::EscapedStringLiteral(s) | SqlValue::NationalStringLiteral(s) => {
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
            serde_json::Value::Array(arr) => {
                let mut values = Vec::with_capacity(arr.len());
                for value in arr {
                    values.push(self.json_value_to_fusion_value(value));
                }
                Value::Array(values)
            }
            serde_json::Value::Object(obj) => {
                let mut map = std::collections::HashMap::with_capacity(obj.len());
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
            Value::Decimal(d) => Some(decimal_index_string_for_value(d)),
            Value::Date(days) => Some(crate::common::encoding::encode_i64_comparable(*days as i64)),
            Value::Timestamp(micros) => {
                Some(crate::common::encoding::encode_i64_comparable(*micros))
            }
            Value::Interval(micros) => {
                Some(crate::common::encoding::encode_i64_comparable(*micros))
            }
            _ => None,
        }
    }

    fn extract_datetime_field(field: &DateTimeField, value: Value) -> Result<Value> {
        let micros = match value {
            Value::Timestamp(micros) => micros,
            Value::Date(days) => {
                let Some(date) = chrono::NaiveDate::from_num_days_from_ce_opt(days) else {
                    return Err(FusionError::Execution(format!(
                        "Cannot extract {} from invalid DATE",
                        field
                    )));
                };
                date.and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .timestamp_micros()
            }
            Value::String(s) => match Value::timestamp_from_str(&s) {
                Some(Value::Timestamp(micros)) => micros,
                _ => {
                    return Err(FusionError::Execution(format!(
                        "Cannot extract {} from '{}'",
                        field, s
                    )));
                }
            },
            other => {
                return Err(FusionError::Execution(format!(
                    "Cannot extract {} from {:?}",
                    field, other
                )));
            }
        };

        let Some(dt) = chrono::DateTime::from_timestamp_micros(micros) else {
            return Err(FusionError::Execution(format!(
                "Cannot extract {} from timestamp {}",
                field, micros
            )));
        };
        let dt = dt.naive_utc();
        let value = match field {
            DateTimeField::Epoch => {
                if micros % 1_000_000 == 0 {
                    Value::Integer(micros / 1_000_000)
                } else {
                    Value::Float(micros as f64 / 1_000_000.0)
                }
            }
            DateTimeField::Year | DateTimeField::Years => Value::Integer(dt.year() as i64),
            DateTimeField::Month | DateTimeField::Months => Value::Integer(dt.month() as i64),
            DateTimeField::Day | DateTimeField::Days => Value::Integer(dt.day() as i64),
            DateTimeField::Hour | DateTimeField::Hours => Value::Integer(dt.hour() as i64),
            DateTimeField::Minute | DateTimeField::Minutes => Value::Integer(dt.minute() as i64),
            DateTimeField::Second | DateTimeField::Seconds => Value::Integer(dt.second() as i64),
            DateTimeField::Microsecond | DateTimeField::Microseconds => {
                Value::Integer(dt.and_utc().timestamp_subsec_micros() as i64)
            }
            DateTimeField::Millisecond | DateTimeField::Milliseconds => {
                Value::Integer(dt.and_utc().timestamp_subsec_millis() as i64)
            }
            DateTimeField::Doy | DateTimeField::DayOfYear => Value::Integer(dt.ordinal() as i64),
            DateTimeField::Dow | DateTimeField::DayOfWeek => {
                Value::Integer(dt.weekday().num_days_from_sunday() as i64)
            }
            DateTimeField::Isodow => Value::Integer(dt.weekday().number_from_monday() as i64),
            DateTimeField::Isoyear => Value::Integer(dt.iso_week().year() as i64),
            DateTimeField::IsoWeek | DateTimeField::Week(_) => {
                Value::Integer(dt.iso_week().week() as i64)
            }
            DateTimeField::Quarter => Value::Integer((dt.month0() / 3 + 1) as i64),
            _ => {
                return Err(FusionError::Execution(format!(
                    "Unsupported EXTRACT field: {}",
                    field
                )));
            }
        };
        Ok(value)
    }

    fn evaluate_access(
        &self,
        value: Value,
        access: &AccessExpr,
        row: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Result<Value> {
        match access {
            AccessExpr::Subscript(Subscript::Index { index }) => {
                let index = match self.evaluate_value(index, row, schema, params)? {
                    Value::Integer(value) => value,
                    _ => return Ok(Value::Null),
                };
                let Value::Array(values) = value else {
                    return Ok(Value::Null);
                };
                if index <= 0 {
                    return Ok(Value::Null);
                }
                Ok(values
                    .get((index - 1) as usize)
                    .cloned()
                    .unwrap_or(Value::Null))
            }
            _ => Ok(Value::Null),
        }
    }
}

fn decimal_math<F>(left: &str, right: &str, op: F) -> Result<Value>
where
    F: Fn(f64, f64) -> f64,
{
    let l = left
        .parse::<f64>()
        .map_err(|_| FusionError::Execution(format!("Cannot use '{}' as DECIMAL", left)))?;
    let r = right
        .parse::<f64>()
        .map_err(|_| FusionError::Execution(format!("Cannot use '{}' as DECIMAL", right)))?;
    Value::decimal_from_f64(op(l, r))
        .ok_or_else(|| FusionError::Execution("DECIMAL arithmetic overflow".to_string()))
}

#[cfg(test)]
mod tests {
    use super::{
        ascii_case_insensitive_ends_with, column_matches_fallback_name, column_suffix_for_fallback,
        concat_operator_values, concat_string_values, decimal_index_string_for_value,
        position_of_substring, trim_string_value,
    };
    use crate::common::Value;
    use sqlparser::ast::TrimWhereField;

    #[test]
    fn decimal_index_string_for_value_preallocates_exact_value() {
        let value = decimal_index_string_for_value("123.45");

        assert_eq!(value, "dec:123.45");
        assert!(value.capacity() >= value.len());
    }

    #[test]
    fn concat_string_values_preallocates_exact_value() {
        let value = concat_string_values("hello ", "world");

        assert_eq!(value, "hello world");
        assert!(value.capacity() >= value.len());
    }

    #[test]
    fn concat_operator_values_preserves_scalar_null_and_array_behavior() {
        assert_eq!(
            concat_operator_values(Value::String("id=".to_string()), Value::Integer(42)),
            Value::String("id=42".to_string())
        );
        assert_eq!(
            concat_operator_values(Value::Float(3.5), Value::Boolean(false)),
            Value::String("3.5false".to_string())
        );
        assert_eq!(
            concat_operator_values(Value::Blob(vec![1, 2]), Value::String("x".to_string())),
            Value::String("Blob([1, 2])x".to_string())
        );
        assert_eq!(
            concat_operator_values(Value::Null, Value::String("x".to_string())),
            Value::Null
        );
        assert_eq!(
            concat_operator_values(
                Value::Array(vec![Value::Integer(1)]),
                Value::Array(vec![Value::Integer(2)])
            ),
            Value::Array(vec![Value::Integer(1), Value::Integer(2)])
        );
    }

    #[test]
    fn column_suffix_for_fallback_preallocates_exact_suffix() {
        let suffix = column_suffix_for_fallback("customer_id");

        assert_eq!(suffix, ".customer_id");
        assert!(suffix.capacity() >= suffix.len());
    }

    #[test]
    fn ascii_case_insensitive_ends_with_matches_ascii_lowercase_suffix() {
        let value = "Orders.CustomerID_Ä";
        let suffix = ".customerid_Ä";

        assert_eq!(
            ascii_case_insensitive_ends_with(value, suffix),
            value
                .to_ascii_lowercase()
                .ends_with(&suffix.to_ascii_lowercase())
        );
        assert!(!ascii_case_insensitive_ends_with(
            "Orders.CustomerID_Ä",
            ".orderid_Ä"
        ));
        assert!(!ascii_case_insensitive_ends_with("id", ".customerid"));
    }

    #[test]
    fn trim_string_value_handles_where_and_character_set() {
        assert_eq!(trim_string_value("  hi  ", None, None), "hi");
        assert_eq!(
            trim_string_value("  hi  ", Some(&TrimWhereField::Leading), None),
            "hi  "
        );
        assert_eq!(
            trim_string_value("  hi  ", Some(&TrimWhereField::Trailing), None),
            "  hi"
        );
        assert_eq!(
            trim_string_value("xxhixx", Some(&TrimWhereField::Both), Some("x")),
            "hi"
        );
        assert_eq!(
            trim_string_value("xyhixy", Some(&TrimWhereField::Leading), Some("xy")),
            "hixy"
        );
    }

    #[test]
    fn position_of_substring_returns_one_based_index_or_zero() {
        assert_eq!(position_of_substring("abc", "b"), 2);
        assert_eq!(position_of_substring("abc", "z"), 0);
        assert_eq!(position_of_substring("abc", ""), 1);
        assert_eq!(position_of_substring("a\u{e9}bc", "b"), 3);
    }

    #[test]
    fn column_matches_fallback_name_preserves_legacy_lowercase_matching() {
        for (column_name, fallback_name) in [
            ("customer_id", "customer_id"),
            ("Orders.customer_id", "customer_id"),
            ("Orders.CustomerID_Ä", "customerid_Ä"),
            ("CUSTOMER_ID", "customer_id"),
            ("different_customer_id", "customer_id"),
        ] {
            let suffix = column_suffix_for_fallback(fallback_name);
            let fallback_lower = fallback_name.to_ascii_lowercase();
            let suffix_lower = suffix.to_ascii_lowercase();
            let legacy_match = column_name == fallback_name
                || column_name.ends_with(&suffix)
                || column_name.eq_ignore_ascii_case(fallback_name)
                || column_name.to_ascii_lowercase().ends_with(&suffix_lower)
                || column_name.to_ascii_lowercase() == fallback_lower;

            assert_eq!(
                column_matches_fallback_name(column_name, fallback_name, &suffix),
                legacy_match,
                "{column_name} should match legacy behavior for {fallback_name}"
            );
        }
    }
}
