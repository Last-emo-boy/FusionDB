use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use crate::common::Value;

/// Converts row-oriented data (from the executor) into an Arrow RecordBatch
/// for vectorized aggregation.
pub fn rows_to_record_batch(columns: &[String], rows: &[Vec<Value>]) -> Option<RecordBatch> {
    if rows.is_empty() || columns.is_empty() {
        return None;
    }

    let num_cols = columns.len();
    let mut fields = Vec::with_capacity(num_cols);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for (col_idx, col_name) in columns.iter().enumerate() {
        // Infer type from first non-null value
        let mut is_int = false;
        let mut is_float = false;
        for row in rows {
            if col_idx < row.len() {
                match &row[col_idx] {
                    Value::Integer(_) => {
                        is_int = true;
                        break;
                    }
                    Value::Float(_) => {
                        is_float = true;
                        break;
                    }
                    Value::Null => continue,
                    _ => break,
                }
            }
        }

        if is_int {
            fields.push(Field::new(col_name, DataType::Int64, true));
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                values.push(if col_idx < row.len() {
                    match &row[col_idx] {
                        Value::Integer(i) => Some(*i),
                        _ => None,
                    }
                } else {
                    None
                });
            }
            arrays.push(Arc::new(Int64Array::from(values)));
        } else if is_float {
            fields.push(Field::new(col_name, DataType::Float64, true));
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                values.push(if col_idx < row.len() {
                    match &row[col_idx] {
                        Value::Float(f) => Some(*f),
                        Value::Integer(i) => Some(*i as f64),
                        _ => None,
                    }
                } else {
                    None
                });
            }
            arrays.push(Arc::new(Float64Array::from(values)));
        } else {
            fields.push(Field::new(col_name, DataType::Utf8, true));
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                values.push(if col_idx < row.len() {
                    match &row[col_idx] {
                        Value::String(s) => Some(s.clone()),
                        Value::Integer(i) => Some(i.to_string()),
                        Value::Float(f) => Some(f.to_string()),
                        Value::Null => None,
                        other => Some(format!("{:?}", other)),
                    }
                } else {
                    None
                });
            }
            arrays.push(Arc::new(StringArray::from(values)));
        }
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays).ok()
}

/// Vectorized COUNT using Arrow (counts non-null values in a column).
pub fn arrow_count(batch: &RecordBatch, col_idx: usize) -> Option<u64> {
    if col_idx >= batch.num_columns() {
        return None;
    }
    let array = batch.column(col_idx);
    Some((array.len() - array.null_count()) as u64)
}

/// Vectorized SUM for numeric columns.
pub fn arrow_sum(batch: &RecordBatch, col_idx: usize) -> Option<Value> {
    if col_idx >= batch.num_columns() {
        return None;
    }
    let array = batch.column(col_idx);
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()?;
            let sum = compute::sum(arr)?;
            Some(Value::Integer(sum))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()?;
            let sum = compute::sum(arr)?;
            Some(Value::Float(sum))
        }
        _ => None,
    }
}

/// Vectorized MIN for numeric columns.
pub fn arrow_min(batch: &RecordBatch, col_idx: usize) -> Option<Value> {
    if col_idx >= batch.num_columns() {
        return None;
    }
    let array = batch.column(col_idx);
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()?;
            let min = compute::min(arr)?;
            Some(Value::Integer(min))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()?;
            let min = compute::min(arr)?;
            Some(Value::Float(min))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            let min = compute::min_string(arr)?;
            Some(Value::String(min.to_string()))
        }
        _ => None,
    }
}

/// Vectorized MAX for numeric columns.
pub fn arrow_max(batch: &RecordBatch, col_idx: usize) -> Option<Value> {
    if col_idx >= batch.num_columns() {
        return None;
    }
    let array = batch.column(col_idx);
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()?;
            let max = compute::max(arr)?;
            Some(Value::Integer(max))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()?;
            let max = compute::max(arr)?;
            Some(Value::Float(max))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            let max = compute::max_string(arr)?;
            Some(Value::String(max.to_string()))
        }
        _ => None,
    }
}

/// Vectorized AVG for numeric columns.
pub fn arrow_avg(batch: &RecordBatch, col_idx: usize) -> Option<Value> {
    if col_idx >= batch.num_columns() {
        return None;
    }
    let array = batch.column(col_idx);
    let count = (array.len() - array.null_count()) as f64;
    if count == 0.0 {
        return Some(Value::Null);
    }
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()?;
            let sum = compute::sum(arr)? as f64;
            Some(Value::Float(sum / count))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()?;
            let sum = compute::sum(arr)?;
            Some(Value::Float(sum / count))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_data() -> (Vec<String>, Vec<Vec<Value>>) {
        let columns = vec!["id".to_string(), "score".to_string(), "name".to_string()];
        let rows = vec![
            vec![
                Value::Integer(1),
                Value::Float(85.5),
                Value::String("Alice".into()),
            ],
            vec![
                Value::Integer(2),
                Value::Float(92.0),
                Value::String("Bob".into()),
            ],
            vec![
                Value::Integer(3),
                Value::Float(78.3),
                Value::String("Carol".into()),
            ],
            vec![
                Value::Integer(4),
                Value::Float(95.1),
                Value::String("Dave".into()),
            ],
        ];
        (columns, rows)
    }

    #[test]
    fn test_rows_to_record_batch() {
        let (cols, rows) = sample_data();
        let batch = rows_to_record_batch(&cols, &rows).unwrap();
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_arrow_count() {
        let (cols, rows) = sample_data();
        let batch = rows_to_record_batch(&cols, &rows).unwrap();
        assert_eq!(arrow_count(&batch, 0), Some(4));
    }

    #[test]
    fn test_arrow_sum_int() {
        let (cols, rows) = sample_data();
        let batch = rows_to_record_batch(&cols, &rows).unwrap();
        assert_eq!(arrow_sum(&batch, 0), Some(Value::Integer(10)));
    }

    #[test]
    fn test_arrow_sum_float() {
        let (cols, rows) = sample_data();
        let batch = rows_to_record_batch(&cols, &rows).unwrap();
        if let Some(Value::Float(sum)) = arrow_sum(&batch, 1) {
            assert!((sum - 350.9).abs() < 0.01);
        } else {
            panic!("Expected float sum");
        }
    }

    #[test]
    fn test_arrow_min_max() {
        let (cols, rows) = sample_data();
        let batch = rows_to_record_batch(&cols, &rows).unwrap();
        assert_eq!(arrow_min(&batch, 0), Some(Value::Integer(1)));
        assert_eq!(arrow_max(&batch, 0), Some(Value::Integer(4)));
    }

    #[test]
    fn test_arrow_avg() {
        let (cols, rows) = sample_data();
        let batch = rows_to_record_batch(&cols, &rows).unwrap();
        if let Some(Value::Float(avg)) = arrow_avg(&batch, 0) {
            assert!((avg - 2.5).abs() < 0.01);
        } else {
            panic!("Expected float avg");
        }
    }

    #[test]
    fn test_empty_batch() {
        let cols = vec!["id".to_string()];
        let rows: Vec<Vec<Value>> = vec![];
        assert!(rows_to_record_batch(&cols, &rows).is_none());
    }
}
