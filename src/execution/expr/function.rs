use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use sqlparser::ast::{Function, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName};
use std::borrow::Cow;
use std::fmt::Write as _;

use super::{function_name_eq_ascii, Executor};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EvaluatedFunction {
    VectorDistance,
    Embedding,
    CosineSimilarity,
    Upper,
    Lower,
    Length,
    Ascii,
    Concat,
    Coalesce,
    ArrayAppend,
    NullIf,
    Substring,
    Replace,
    Trim,
    Abs,
    Round,
    Ceil,
    Floor,
    Mod,
    Power,
    Sqrt,
    ToTimestamp,
    Now,
    CurrentDate,
}

fn evaluated_function_kind(name: &ObjectName) -> Option<EvaluatedFunction> {
    if function_name_eq_ascii(name, "VECTOR_DISTANCE") {
        Some(EvaluatedFunction::VectorDistance)
    } else if function_name_eq_ascii(name, "EMBEDDING") {
        Some(EvaluatedFunction::Embedding)
    } else if function_name_eq_ascii(name, "COSINE_SIMILARITY") {
        Some(EvaluatedFunction::CosineSimilarity)
    } else if function_name_eq_ascii(name, "UPPER") {
        Some(EvaluatedFunction::Upper)
    } else if function_name_eq_ascii(name, "LOWER") {
        Some(EvaluatedFunction::Lower)
    } else if function_name_eq_ascii(name, "LENGTH")
        || function_name_eq_ascii(name, "CHAR_LENGTH")
        || function_name_eq_ascii(name, "CHARACTER_LENGTH")
    {
        Some(EvaluatedFunction::Length)
    } else if function_name_eq_ascii(name, "ASCII") {
        Some(EvaluatedFunction::Ascii)
    } else if function_name_eq_ascii(name, "CONCAT") {
        Some(EvaluatedFunction::Concat)
    } else if function_name_eq_ascii(name, "COALESCE") {
        Some(EvaluatedFunction::Coalesce)
    } else if function_name_eq_ascii(name, "ARRAY_APPEND") {
        Some(EvaluatedFunction::ArrayAppend)
    } else if function_name_eq_ascii(name, "NULLIF") {
        Some(EvaluatedFunction::NullIf)
    } else if function_name_eq_ascii(name, "SUBSTRING") || function_name_eq_ascii(name, "SUBSTR") {
        Some(EvaluatedFunction::Substring)
    } else if function_name_eq_ascii(name, "REPLACE") {
        Some(EvaluatedFunction::Replace)
    } else if function_name_eq_ascii(name, "TRIM") {
        Some(EvaluatedFunction::Trim)
    } else if function_name_eq_ascii(name, "ABS") {
        Some(EvaluatedFunction::Abs)
    } else if function_name_eq_ascii(name, "ROUND") {
        Some(EvaluatedFunction::Round)
    } else if function_name_eq_ascii(name, "CEIL") || function_name_eq_ascii(name, "CEILING") {
        Some(EvaluatedFunction::Ceil)
    } else if function_name_eq_ascii(name, "FLOOR") {
        Some(EvaluatedFunction::Floor)
    } else if function_name_eq_ascii(name, "MOD") {
        Some(EvaluatedFunction::Mod)
    } else if function_name_eq_ascii(name, "POWER") || function_name_eq_ascii(name, "POW") {
        Some(EvaluatedFunction::Power)
    } else if function_name_eq_ascii(name, "SQRT") {
        Some(EvaluatedFunction::Sqrt)
    } else if function_name_eq_ascii(name, "TO_TIMESTAMP") {
        Some(EvaluatedFunction::ToTimestamp)
    } else if function_name_eq_ascii(name, "NOW")
        || function_name_eq_ascii(name, "CURRENT_TIMESTAMP")
    {
        Some(EvaluatedFunction::Now)
    } else if function_name_eq_ascii(name, "CURRENT_DATE") {
        Some(EvaluatedFunction::CurrentDate)
    } else {
        None
    }
}

fn append_concat_value(result: &mut String, value: Value) {
    match value {
        Value::String(s) => result.push_str(&s),
        Value::Integer(n) => write!(result, "{}", n).expect("writing to String cannot fail"),
        Value::Float(f) => write!(result, "{}", f).expect("writing to String cannot fail"),
        Value::Boolean(b) => write!(result, "{}", b).expect("writing to String cannot fail"),
        Value::Null => {}
        other => write!(result, "{:?}", other).expect("writing to String cannot fail"),
    }
}

fn concat_result_buffer(arg_count: usize) -> String {
    String::with_capacity(arg_count)
}

fn char_boundary_byte_index(s: &str, char_pos: usize) -> Option<usize> {
    if char_pos == 0 {
        return Some(0);
    }

    let mut seen = 0;
    for (byte_index, _) in s.char_indices() {
        if seen == char_pos {
            return Some(byte_index);
        }
        seen += 1;
    }

    (seen == char_pos).then_some(s.len())
}

fn substring_by_chars(s: &str, start: usize, len: Option<usize>) -> String {
    let start_byte = match char_boundary_byte_index(s, start) {
        Some(start_byte) => start_byte,
        None => return String::new(),
    };
    let end_byte = match len {
        Some(len) => char_boundary_byte_index(s, start + len).unwrap_or(s.len()),
        None => s.len(),
    };

    s[start_byte..end_byte].to_string()
}

fn embedding_text_for_value(value: &Value) -> Cow<'_, str> {
    match value {
        Value::String(s) => Cow::Borrowed(s.as_str()),
        other => Cow::Owned(format!("{:?}", other)),
    }
}

impl Executor {
    pub(crate) fn evaluate_function(
        &self,
        func: &Function,
        row: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Result<Value> {
        let function = evaluated_function_kind(&func.name);

        let args = match &func.args {
            FunctionArguments::List(list) => &list.args,
            _ => {
                return Err(FusionError::Execution(
                    "Unsupported function argument format".to_string(),
                ));
            }
        };

        match function {
            Some(EvaluatedFunction::VectorDistance) => {
                if args.len() != 2 {
                    return Err(FusionError::Execution(
                        "VECTOR_DISTANCE requires 2 arguments".to_string(),
                    ));
                }

                let v1 = self.evaluate_arg(&args[0], row, schema, params)?;
                let v2 = self.evaluate_arg(&args[1], row, schema, params)?;

                self.compute_vector_distance(&v1, &v2)
            }
            Some(EvaluatedFunction::Embedding) => {
                if args.len() != 1 {
                    return Err(FusionError::Execution(
                        "EMBEDDING requires 1 argument (text)".to_string(),
                    ));
                }
                let text_val = self.evaluate_arg(&args[0], row, schema, params)?;
                let text = embedding_text_for_value(&text_val);
                match self.embedding_registry.embed(text.as_ref()) {
                    Some(vec) => Ok(Value::Vector(vec)),
                    None => Err(FusionError::Execution(
                        "No embedding provider available".to_string(),
                    )),
                }
            }
            Some(EvaluatedFunction::CosineSimilarity) => {
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
            Some(EvaluatedFunction::Upper) => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::String(s) => Ok(Value::String(s.to_uppercase())),
                    _ => Ok(Value::Null),
                }
            }
            Some(EvaluatedFunction::Lower) => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::String(s) => Ok(Value::String(s.to_lowercase())),
                    _ => Ok(Value::Null),
                }
            }
            Some(EvaluatedFunction::Length) => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::String(s) => Ok(Value::Integer(s.len() as i64)),
                    _ => Ok(Value::Null),
                }
            }
            Some(EvaluatedFunction::Ascii) => {
                if args.len() != 1 {
                    return Err(FusionError::Execution(
                        "ASCII requires 1 argument".to_string(),
                    ));
                }
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::String(s) => s
                        .chars()
                        .next()
                        .map(|ch| Value::Integer(ch as i64))
                        .map_or(Ok(Value::Null), Ok),
                    _ => Ok(Value::Null),
                }
            }
            Some(EvaluatedFunction::Concat) => {
                let mut result = concat_result_buffer(args.len());
                for arg in args {
                    let val = self.evaluate_arg(arg, row, schema, params)?;
                    append_concat_value(&mut result, val);
                }
                Ok(Value::String(result))
            }
            Some(EvaluatedFunction::Coalesce) => {
                for arg in args {
                    let val = self.evaluate_arg(arg, row, schema, params)?;
                    if val != Value::Null {
                        return Ok(val);
                    }
                }
                Ok(Value::Null)
            }
            Some(EvaluatedFunction::ArrayAppend) => {
                if args.len() != 2 {
                    return Err(FusionError::Execution(
                        "ARRAY_APPEND requires 2 arguments".to_string(),
                    ));
                }
                let array = self.evaluate_arg(&args[0], row, schema, params)?;
                let value = self.evaluate_arg(&args[1], row, schema, params)?;
                match array {
                    Value::Array(mut values) => {
                        values.push(value);
                        Ok(Value::Array(values))
                    }
                    Value::Null => Ok(Value::Array(vec![value])),
                    _ => Ok(Value::Null),
                }
            }
            Some(EvaluatedFunction::NullIf) => {
                if args.len() != 2 {
                    return Err(FusionError::Execution(
                        "NULLIF requires 2 arguments".to_string(),
                    ));
                }
                let v1 = self.evaluate_arg(&args[0], row, schema, params)?;
                let v2 = self.evaluate_arg(&args[1], row, schema, params)?;
                if v1 == v2 {
                    Ok(Value::Null)
                } else {
                    Ok(v1)
                }
            }
            Some(EvaluatedFunction::Substring) => {
                if args.is_empty() || args.len() > 3 {
                    return Err(FusionError::Execution(
                        "SUBSTRING requires 1-3 arguments".to_string(),
                    ));
                }
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                let s = match val {
                    Value::String(s) => s,
                    _ => return Ok(Value::Null),
                };
                let start = if args.len() >= 2 {
                    match self.evaluate_arg(&args[1], row, schema, params)? {
                        Value::Integer(n) => (n - 1).max(0) as usize,
                        _ => 0,
                    }
                } else {
                    0
                };
                let len = if args.len() == 3 {
                    match self.evaluate_arg(&args[2], row, schema, params)? {
                        Value::Integer(n) => Some(n.max(0) as usize),
                        _ => None,
                    }
                } else {
                    None
                };
                Ok(Value::String(substring_by_chars(&s, start, len)))
            }
            Some(EvaluatedFunction::Replace) => {
                if args.len() != 3 {
                    return Err(FusionError::Execution(
                        "REPLACE requires 3 arguments".to_string(),
                    ));
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
            Some(EvaluatedFunction::Trim) => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::String(s) => Ok(Value::String(s.trim().to_string())),
                    _ => Ok(Value::Null),
                }
            }
            Some(EvaluatedFunction::Abs) => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::Integer(n) => Ok(Value::Integer(n.abs())),
                    Value::Float(f) => Ok(Value::Float(f.abs())),
                    _ => Ok(Value::Null),
                }
            }
            Some(EvaluatedFunction::Round) => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                let precision = if args.len() >= 2 {
                    match self.evaluate_arg(&args[1], row, schema, params)? {
                        Value::Integer(n) => n as i32,
                        _ => 0,
                    }
                } else {
                    0
                };
                match val {
                    Value::Float(f) => {
                        let factor = 10f64.powi(precision);
                        Ok(Value::Float((f * factor).round() / factor))
                    }
                    Value::Integer(n) => Ok(Value::Integer(n)),
                    _ => Ok(Value::Null),
                }
            }
            Some(EvaluatedFunction::Ceil) => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::Float(f) => Ok(Value::Integer(f.ceil() as i64)),
                    Value::Integer(n) => Ok(Value::Integer(n)),
                    _ => Ok(Value::Null),
                }
            }
            Some(EvaluatedFunction::Floor) => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::Float(f) => Ok(Value::Integer(f.floor() as i64)),
                    Value::Integer(n) => Ok(Value::Integer(n)),
                    _ => Ok(Value::Null),
                }
            }
            Some(EvaluatedFunction::Mod) => {
                if args.len() < 2 {
                    return Err(FusionError::Execution(
                        "MOD requires 2 arguments".to_string(),
                    ));
                }
                let a = self.evaluate_arg(&args[0], row, schema, params)?;
                let b = self.evaluate_arg(&args[1], row, schema, params)?;
                match (a, b) {
                    (Value::Integer(a), Value::Integer(b)) if b != 0 => Ok(Value::Integer(a % b)),
                    (Value::Float(a), Value::Float(b)) if b != 0.0 => Ok(Value::Float(a % b)),
                    (Value::Integer(a), Value::Float(b)) if b != 0.0 => {
                        Ok(Value::Float(a as f64 % b))
                    }
                    (Value::Float(a), Value::Integer(b)) if b != 0 => {
                        Ok(Value::Float(a % b as f64))
                    }
                    _ => Ok(Value::Null),
                }
            }
            Some(EvaluatedFunction::Power) => {
                if args.len() < 2 {
                    return Err(FusionError::Execution(
                        "POWER requires 2 arguments".to_string(),
                    ));
                }
                let base = self.evaluate_arg(&args[0], row, schema, params)?;
                let exp = self.evaluate_arg(&args[1], row, schema, params)?;
                let b = match base {
                    Value::Integer(n) => n as f64,
                    Value::Float(f) => f,
                    _ => return Ok(Value::Null),
                };
                let e = match exp {
                    Value::Integer(n) => n as f64,
                    Value::Float(f) => f,
                    _ => return Ok(Value::Null),
                };
                Ok(Value::Float(b.powf(e)))
            }
            Some(EvaluatedFunction::Sqrt) => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::Integer(n) => Ok(Value::Float((n as f64).sqrt())),
                    Value::Float(f) => Ok(Value::Float(f.sqrt())),
                    _ => Ok(Value::Null),
                }
            }
            Some(EvaluatedFunction::ToTimestamp) => {
                if args.len() != 1 {
                    return Err(FusionError::Execution(
                        "TO_TIMESTAMP requires 1 argument".to_string(),
                    ));
                }
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::Integer(seconds) => {
                        Ok(Value::Timestamp(seconds.saturating_mul(1_000_000)))
                    }
                    Value::Float(seconds) => {
                        Ok(Value::Timestamp((seconds * 1_000_000.0).round() as i64))
                    }
                    Value::Decimal(seconds) => seconds
                        .parse::<f64>()
                        .map(|seconds| Value::Timestamp((seconds * 1_000_000.0).round() as i64))
                        .map_err(|_| {
                            FusionError::Execution(format!(
                                "Cannot use '{}' as TO_TIMESTAMP argument",
                                seconds
                            ))
                        }),
                    Value::String(s) => {
                        if let Ok(seconds) = s.trim().parse::<f64>() {
                            Ok(Value::Timestamp((seconds * 1_000_000.0).round() as i64))
                        } else {
                            Value::timestamp_from_str(&s).ok_or_else(|| {
                                FusionError::Execution(format!(
                                    "Cannot use '{}' as TO_TIMESTAMP argument",
                                    s
                                ))
                            })
                        }
                    }
                    Value::Timestamp(_) => Ok(val),
                    _ => Ok(Value::Null),
                }
            }
            Some(EvaluatedFunction::Now) => {
                let dur = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                Ok(Value::Integer(dur.as_secs() as i64))
            }
            Some(EvaluatedFunction::CurrentDate) => {
                let dur = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let days = dur.as_secs() / 86400;
                Ok(Value::Integer(days as i64))
            }
            None => Err(FusionError::Execution(format!(
                "Unsupported function: {}",
                func.name.to_string().to_uppercase()
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
            Value::Vector(vec) => {
                let mut res = Vec::with_capacity(vec.len());
                for &item in vec {
                    res.push(item as f64);
                }
                Ok(res)
            }
            Value::Array(arr) => {
                let mut res = Vec::with_capacity(arr.len());
                for item in arr {
                    match item {
                        Value::Integer(i) => res.push(*i as f64),
                        Value::Float(f) => res.push(*f),
                        _ => {
                            return Err(FusionError::Execution(
                                "Vector elements must be numbers".to_string(),
                            ));
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
}

#[cfg(test)]
mod tests {
    use super::{
        append_concat_value, char_boundary_byte_index, concat_result_buffer,
        embedding_text_for_value, evaluated_function_kind, substring_by_chars,
    };
    use crate::common::Value;
    use sqlparser::ast::{Ident, ObjectName};
    use std::borrow::Cow;

    #[test]
    fn concat_result_buffer_preallocates_by_argument_count() {
        let result = concat_result_buffer(3);

        assert_eq!(result, "");
        assert!(result.capacity() >= 3);
    }

    #[test]
    fn substring_by_chars_preserves_character_boundaries() {
        let value = "a\u{e9}bc";

        assert_eq!(char_boundary_byte_index(value, 0), Some(0));
        assert_eq!(char_boundary_byte_index(value, 1), Some(1));
        assert_eq!(char_boundary_byte_index(value, 2), Some(3));
        assert_eq!(char_boundary_byte_index(value, 4), Some(value.len()));
        assert_eq!(char_boundary_byte_index(value, 5), None);
        assert_eq!(substring_by_chars(value, 1, Some(2)), "\u{e9}b");
        assert_eq!(substring_by_chars(value, 2, None), "bc");
        assert_eq!(substring_by_chars(value, 4, Some(1)), "");
        assert_eq!(substring_by_chars(value, 5, Some(1)), "");
    }

    #[test]
    fn append_concat_value_preserves_exact_scalar_and_fallback_text() {
        let mut result = String::with_capacity(64);
        result.push_str("prefix:");

        append_concat_value(&mut result, Value::String("db".to_string()));
        append_concat_value(&mut result, Value::Integer(42));
        append_concat_value(&mut result, Value::Float(3.5));
        append_concat_value(&mut result, Value::Boolean(true));
        append_concat_value(&mut result, Value::Null);
        append_concat_value(&mut result, Value::Array(vec![Value::Integer(7)]));

        assert_eq!(result, "prefix:db423.5trueArray([Integer(7)])");
        assert!(result.capacity() >= result.len());
    }

    #[test]
    fn embedding_text_for_value_borrows_strings_and_preserves_fallback_text() {
        let string_value = Value::String("red apple".to_string());
        let text = embedding_text_for_value(&string_value);

        assert!(matches!(text, Cow::Borrowed("red apple")));

        let fallback_value = Value::Integer(42);
        let text = embedding_text_for_value(&fallback_value);

        assert_eq!(text, Cow::Owned::<str>("Integer(42)".to_string()));
    }

    #[test]
    fn evaluated_function_kind_matches_aliases_without_display_string() {
        let lower = ObjectName::from(vec![Ident::new("lower")]);
        let char_length = ObjectName::from(vec![Ident::new("char_length")]);
        let substr = ObjectName::from(vec![Ident::new("substr")]);
        let current_timestamp = ObjectName::from(vec![Ident::new("current_timestamp")]);
        let qualified = ObjectName::from(vec![Ident::new("pg_catalog"), Ident::new("lower")]);

        assert!(evaluated_function_kind(&lower).is_some());
        assert_eq!(
            evaluated_function_kind(&char_length),
            evaluated_function_kind(&ObjectName::from(vec![Ident::new("length")]))
        );
        assert_eq!(
            evaluated_function_kind(&substr),
            evaluated_function_kind(&ObjectName::from(vec![Ident::new("substring")]))
        );
        assert_eq!(
            evaluated_function_kind(&current_timestamp),
            evaluated_function_kind(&ObjectName::from(vec![Ident::new("now")]))
        );
        assert!(evaluated_function_kind(&qualified).is_none());
    }
}
