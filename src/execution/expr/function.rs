use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use sqlparser::ast::{Function, FunctionArg, FunctionArgExpr, FunctionArguments};

use super::Executor;

impl Executor {
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
                ));
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
            "ASCII" => {
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
            "ARRAY_APPEND" => {
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
            "NULLIF" => {
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
            "SUBSTRING" | "SUBSTR" => {
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
                let chars: Vec<char> = s.chars().collect();
                let end = len
                    .map(|l| (start + l).min(chars.len()))
                    .unwrap_or(chars.len());
                let result: String = chars[start.min(chars.len())..end].iter().collect();
                Ok(Value::String(result))
            }
            "REPLACE" => {
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
            "POWER" | "POW" => {
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
            "SQRT" => {
                let val = self.evaluate_arg(&args[0], row, schema, params)?;
                match val {
                    Value::Integer(n) => Ok(Value::Float((n as f64).sqrt())),
                    Value::Float(f) => Ok(Value::Float(f.sqrt())),
                    _ => Ok(Value::Null),
                }
            }
            "TO_TIMESTAMP" => {
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
