use crate::common::Value;
use std::cmp::Ordering;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub(crate) enum AggregateAccumulator {
    Count(i64),
    CountDistinct(HashSet<String>),
    Sum(f64, bool), // val, is_int
    Avg(f64, i64),  // sum, count
    Min(Option<Value>),
    Max(Option<Value>),
    StringAgg(Vec<String>, String), // values, separator
}

impl AggregateAccumulator {
    pub(crate) fn new(func_name: &str) -> Self {
        match func_name.to_uppercase().as_str() {
            "COUNT" => AggregateAccumulator::Count(0),
            "COUNT_DISTINCT" => AggregateAccumulator::CountDistinct(HashSet::new()),
            "SUM" => AggregateAccumulator::Sum(0.0, true),
            "AVG" => AggregateAccumulator::Avg(0.0, 0),
            "MIN" => AggregateAccumulator::Min(None),
            "MAX" => AggregateAccumulator::Max(None),
            "STRING_AGG" | "GROUP_CONCAT" => AggregateAccumulator::StringAgg(Vec::new(), ",".to_string()),
            _ => AggregateAccumulator::Count(0),
        }
    }

    pub(crate) fn update(&mut self, val: &Value) {
        match self {
            AggregateAccumulator::Count(c) => {
                if *val != Value::Null {
                    *c += 1;
                }
            }
            AggregateAccumulator::CountDistinct(set) => {
                if *val != Value::Null {
                    set.insert(format!("{:?}", val));
                }
            }
            AggregateAccumulator::Sum(sum, is_int) => match val {
                Value::Integer(i) => *sum += *i as f64,
                Value::Float(f) => {
                    *is_int = false;
                    *sum += *f;
                }
                _ => {}
            },
            AggregateAccumulator::Avg(sum, count) => match val {
                Value::Integer(i) => {
                    *sum += *i as f64;
                    *count += 1;
                }
                Value::Float(f) => {
                    *sum += *f;
                    *count += 1;
                }
                _ => {}
            },
            AggregateAccumulator::Min(min) => {
                if *val == Value::Null {
                    return;
                }
                if min.is_none() {
                    *min = Some(val.clone());
                } else if let Some(current) = min {
                    if val.compare(current) == Ordering::Less {
                        *min = Some(val.clone());
                    }
                }
            }
            AggregateAccumulator::Max(max) => {
                if *val == Value::Null {
                    return;
                }
                if max.is_none() {
                    *max = Some(val.clone());
                } else if let Some(current) = max {
                    if val.compare(current) == Ordering::Greater {
                        *max = Some(val.clone());
                    }
                }
            }
            AggregateAccumulator::StringAgg(vals, _sep) => {
                if *val != Value::Null {
                    let s = match val {
                        Value::String(s) => s.clone(),
                        Value::Integer(i) => i.to_string(),
                        Value::Float(f) => f.to_string(),
                        Value::Boolean(b) => b.to_string(),
                        _ => return,
                    };
                    vals.push(s);
                }
            }
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        match self {
            AggregateAccumulator::Count(c) => Value::Integer(*c),
            AggregateAccumulator::CountDistinct(set) => Value::Integer(set.len() as i64),
            AggregateAccumulator::Sum(sum, is_int) => {
                if *is_int {
                    Value::Integer(*sum as i64)
                } else {
                    Value::Float(*sum)
                }
            }
            AggregateAccumulator::Avg(sum, count) => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Float(*sum / *count as f64)
                }
            }
            AggregateAccumulator::Min(min) => min.clone().unwrap_or(Value::Null),
            AggregateAccumulator::Max(max) => max.clone().unwrap_or(Value::Null),
            AggregateAccumulator::StringAgg(vals, sep) => {
                if vals.is_empty() {
                    Value::Null
                } else {
                    Value::String(vals.join(sep))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_accumulator() {
        let mut acc = AggregateAccumulator::new("COUNT");
        acc.update(&Value::Integer(1));
        acc.update(&Value::Integer(2));
        acc.update(&Value::Null);
        assert_eq!(acc.finalize(), Value::Integer(2));
    }

    #[test]
    fn test_sum_int_accumulator() {
        let mut acc = AggregateAccumulator::new("SUM");
        acc.update(&Value::Integer(10));
        acc.update(&Value::Integer(20));
        assert_eq!(acc.finalize(), Value::Integer(30));
    }

    #[test]
    fn test_sum_float_accumulator() {
        let mut acc = AggregateAccumulator::new("SUM");
        acc.update(&Value::Integer(10));
        acc.update(&Value::Float(2.5));
        assert_eq!(acc.finalize(), Value::Float(12.5));
    }

    #[test]
    fn test_avg_accumulator() {
        let mut acc = AggregateAccumulator::new("AVG");
        acc.update(&Value::Integer(10));
        acc.update(&Value::Integer(20));
        assert_eq!(acc.finalize(), Value::Float(15.0));
    }

    #[test]
    fn test_avg_empty() {
        let acc = AggregateAccumulator::new("AVG");
        assert_eq!(acc.finalize(), Value::Null);
    }

    #[test]
    fn test_min_accumulator() {
        let mut acc = AggregateAccumulator::new("MIN");
        acc.update(&Value::Integer(30));
        acc.update(&Value::Integer(10));
        acc.update(&Value::Integer(20));
        assert_eq!(acc.finalize(), Value::Integer(10));
    }

    #[test]
    fn test_max_accumulator() {
        let mut acc = AggregateAccumulator::new("MAX");
        acc.update(&Value::Integer(10));
        acc.update(&Value::Integer(30));
        acc.update(&Value::Integer(20));
        assert_eq!(acc.finalize(), Value::Integer(30));
    }

    #[test]
    fn test_min_with_nulls() {
        let mut acc = AggregateAccumulator::new("MIN");
        acc.update(&Value::Null);
        acc.update(&Value::Integer(5));
        acc.update(&Value::Null);
        assert_eq!(acc.finalize(), Value::Integer(5));
    }

    #[test]
    fn test_max_empty() {
        let acc = AggregateAccumulator::new("MAX");
        assert_eq!(acc.finalize(), Value::Null);
    }

    #[test]
    fn test_unknown_func_defaults_to_count() {
        let mut acc = AggregateAccumulator::new("UNKNOWN_FUNC");
        acc.update(&Value::Integer(1));
        assert_eq!(acc.finalize(), Value::Integer(1));
    }
}
