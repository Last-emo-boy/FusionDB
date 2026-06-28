use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use chrono::Datelike;
use sqlparser::ast::{DataType, Expr};

use super::Executor;

impl Executor {
    pub(crate) fn coerce_row_to_schema(
        &self,
        row: Vec<Value>,
        schema: &TableSchema,
    ) -> Result<Vec<Value>> {
        if row.len() != schema.columns.len() {
            return Err(FusionError::Execution("Column count mismatch".to_string()));
        }

        let mut coerced = Vec::with_capacity(row.len());
        for (value, column) in row.into_iter().zip(schema.columns.iter()) {
            coerced.push(Self::coerce_value_to_column_type(value, &column.data_type)?);
        }
        Ok(coerced)
    }

    pub(crate) fn coerce_value_to_column_type(value: Value, data_type: &str) -> Result<Value> {
        if matches!(value, Value::Null) {
            return Ok(Value::Null);
        }

        let data_type = data_type.trim();
        if Self::is_integer_type_name(data_type) {
            return Self::coerce_to_integer(value);
        }
        if Self::is_float_type_name(data_type) {
            return Self::coerce_to_float(value);
        }
        if Self::is_decimal_type_name(data_type) {
            return Self::coerce_to_decimal(value);
        }
        if Self::is_boolean_type_name(data_type) {
            return Self::coerce_to_boolean(value);
        }
        if Self::is_date_type_name(data_type) {
            return Self::coerce_to_date(value);
        }
        if Self::is_timestamp_type_name(data_type) {
            return Self::coerce_to_timestamp(value);
        }
        if Self::is_interval_type_name(data_type) {
            return Self::coerce_to_interval(value);
        }
        if Self::is_text_type_name(data_type) {
            return Ok(match value {
                Value::String(_) => value,
                other => Value::String(other.to_plain_string()),
            });
        }
        Ok(value)
    }

    pub(crate) fn coerce_value_to_sql_type(value: Value, data_type: &DataType) -> Result<Value> {
        if matches!(value, Value::Null) {
            return Ok(Value::Null);
        }
        match data_type {
            DataType::TinyInt(_)
            | DataType::TinyIntUnsigned(_)
            | DataType::SmallInt(_)
            | DataType::SmallIntUnsigned(_)
            | DataType::Int2(_)
            | DataType::Int2Unsigned(_)
            | DataType::Int(_)
            | DataType::Int4(_)
            | DataType::Integer(_)
            | DataType::IntUnsigned(_)
            | DataType::Int4Unsigned(_)
            | DataType::IntegerUnsigned(_)
            | DataType::MediumInt(_)
            | DataType::MediumIntUnsigned(_)
            | DataType::BigInt(_)
            | DataType::Int8(_)
            | DataType::Int64
            | DataType::BigIntUnsigned(_)
            | DataType::Int8Unsigned(_)
            | DataType::Int16
            | DataType::Int32
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Self::coerce_to_integer(value),
            DataType::Float(_)
            | DataType::FloatUnsigned(_)
            | DataType::Float4
            | DataType::Float8
            | DataType::Float32
            | DataType::Float64
            | DataType::Real
            | DataType::RealUnsigned
            | DataType::Double(_)
            | DataType::DoubleUnsigned(_)
            | DataType::DoublePrecision
            | DataType::DoublePrecisionUnsigned => Self::coerce_to_float(value),
            DataType::Numeric(_)
            | DataType::Decimal(_)
            | DataType::DecimalUnsigned(_)
            | DataType::BigNumeric(_)
            | DataType::BigDecimal(_)
            | DataType::Dec(_)
            | DataType::DecUnsigned(_) => Self::coerce_to_decimal(value),
            DataType::Bool | DataType::Boolean => Self::coerce_to_boolean(value),
            DataType::Date | DataType::Date32 => Self::coerce_to_date(value),
            DataType::Timestamp(_, _) | DataType::TimestampNtz(_) | DataType::Datetime(_) => {
                Self::coerce_to_timestamp(value)
            }
            DataType::Interval { .. } => Self::coerce_to_interval(value),
            DataType::Char(_)
            | DataType::Character(_)
            | DataType::Varchar(_)
            | DataType::CharacterVarying(_)
            | DataType::CharVarying(_)
            | DataType::Nvarchar(_)
            | DataType::Text
            | DataType::String(_)
            | DataType::CharacterLargeObject(_)
            | DataType::CharLargeObject(_)
            | DataType::Clob(_) => Ok(match value {
                Value::String(_) => value,
                other => Value::String(other.to_plain_string()),
            }),
            _ => Ok(value),
        }
    }

    pub(crate) fn typed_string_value(data_type: &DataType, raw_value: &str) -> Result<Value> {
        Self::coerce_value_to_sql_type(Value::String(raw_value.to_string()), data_type)
    }

    pub(crate) fn align_comparison_values(
        &self,
        left_expr: &Expr,
        left_value: Value,
        right_expr: &Expr,
        right_value: Value,
        schema: &TableSchema,
    ) -> Result<(Value, Value)> {
        if let Some(data_type) = self
            .comparison_column_type(left_expr, schema)
            .or_else(|| self.comparison_column_type(right_expr, schema))
        {
            return Ok((
                Self::coerce_value_to_column_type(left_value, data_type)?,
                Self::coerce_value_to_column_type(right_value, data_type)?,
            ));
        }
        Ok((left_value, right_value))
    }

    pub(crate) fn comparison_column_type<'a>(
        &self,
        expr: &Expr,
        schema: &'a TableSchema,
    ) -> Option<&'a str> {
        match expr {
            Expr::Identifier(ident) => self
                .resolve_column_index(&ident.value, schema)
                .ok()
                .map(|index| schema.columns[index].data_type.as_str()),
            Expr::CompoundIdentifier(idents) => {
                let mut col_name = String::new();
                for (index, ident) in idents.iter().enumerate() {
                    if index > 0 {
                        col_name.push('.');
                    }
                    col_name.push_str(&ident.value);
                }
                self.resolve_column_index(&col_name, schema)
                    .ok()
                    .map(|index| schema.columns[index].data_type.as_str())
            }
            Expr::Nested(expr) | Expr::Cast { expr, .. } => {
                self.comparison_column_type(expr, schema)
            }
            _ => None,
        }
    }

    fn type_name_matches_any(data_type: &str, candidates: &[&str]) -> bool {
        candidates
            .iter()
            .any(|candidate| data_type.eq_ignore_ascii_case(candidate))
    }

    fn type_name_starts_with_ascii_case_insensitive(data_type: &str, prefix: &str) -> bool {
        data_type
            .as_bytes()
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
    }

    pub(crate) fn is_integer_type_name(data_type: &str) -> bool {
        Self::type_name_matches_any(
            data_type,
            &[
                "INT",
                "INT2",
                "INT4",
                "INT8",
                "INTEGER",
                "SMALLINT",
                "BIGINT",
                "TINYINT",
                "MEDIUMINT",
                "SERIAL",
                "SERIAL2",
                "SERIAL4",
                "SERIAL8",
                "SMALLSERIAL",
                "BIGSERIAL",
            ],
        )
    }

    pub(crate) fn is_float_type_name(data_type: &str) -> bool {
        Self::type_name_matches_any(
            data_type,
            &[
                "FLOAT",
                "FLOAT4",
                "FLOAT8",
                "REAL",
                "DOUBLE",
                "DOUBLE PRECISION",
            ],
        ) || Self::type_name_starts_with_ascii_case_insensitive(data_type, "FLOAT(")
    }

    pub(crate) fn is_decimal_type_name(data_type: &str) -> bool {
        Self::type_name_matches_any(data_type, &["NUMERIC", "DECIMAL", "DEC"])
            || Self::type_name_starts_with_ascii_case_insensitive(data_type, "NUMERIC(")
            || Self::type_name_starts_with_ascii_case_insensitive(data_type, "DECIMAL(")
            || Self::type_name_starts_with_ascii_case_insensitive(data_type, "DEC(")
    }

    fn is_boolean_type_name(data_type: &str) -> bool {
        Self::type_name_matches_any(data_type, &["BOOL", "BOOLEAN"])
    }

    fn is_date_type_name(data_type: &str) -> bool {
        Self::type_name_matches_any(data_type, &["DATE", "DATE32"])
    }

    fn is_timestamp_type_name(data_type: &str) -> bool {
        Self::type_name_matches_any(
            data_type,
            &[
                "TIMESTAMP",
                "TIMESTAMP WITHOUT TIME ZONE",
                "TIMESTAMP WITH TIME ZONE",
                "TIMESTAMPTZ",
                "DATETIME",
            ],
        ) || Self::type_name_starts_with_ascii_case_insensitive(data_type, "TIMESTAMP(")
            || Self::type_name_starts_with_ascii_case_insensitive(data_type, "DATETIME(")
    }

    fn is_interval_type_name(data_type: &str) -> bool {
        data_type.eq_ignore_ascii_case("INTERVAL")
            || Self::type_name_starts_with_ascii_case_insensitive(data_type, "INTERVAL ")
    }

    fn is_text_type_name(data_type: &str) -> bool {
        Self::type_name_matches_any(data_type, &["TEXT", "STRING", "VARCHAR", "CHAR"])
            || Self::type_name_starts_with_ascii_case_insensitive(data_type, "VARCHAR(")
            || Self::type_name_starts_with_ascii_case_insensitive(data_type, "CHAR(")
            || Self::type_name_starts_with_ascii_case_insensitive(data_type, "CHARACTER")
    }

    fn coerce_to_integer(value: Value) -> Result<Value> {
        match value {
            Value::Integer(_) => Ok(value),
            Value::Float(f) => Ok(Value::Integer(f as i64)),
            Value::Decimal(s) => s
                .split('.')
                .next()
                .unwrap_or(&s)
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|_| FusionError::Execution(format!("Cannot cast '{}' to INTEGER", s))),
            Value::String(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|_| FusionError::Execution(format!("Cannot cast '{}' to INTEGER", s))),
            Value::Boolean(b) => Ok(Value::Integer(if b { 1 } else { 0 })),
            other => Err(FusionError::Execution(format!(
                "Cannot cast {:?} to INTEGER",
                other
            ))),
        }
    }

    fn coerce_to_float(value: Value) -> Result<Value> {
        match value {
            Value::Float(_) => Ok(value),
            Value::Integer(n) => Ok(Value::Float(n as f64)),
            Value::Decimal(s) => s
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| FusionError::Execution(format!("Cannot cast '{}' to FLOAT", s))),
            Value::String(s) => s
                .trim()
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| FusionError::Execution(format!("Cannot cast '{}' to FLOAT", s))),
            other => Err(FusionError::Execution(format!(
                "Cannot cast {:?} to FLOAT",
                other
            ))),
        }
    }

    fn coerce_to_decimal(value: Value) -> Result<Value> {
        match value {
            Value::Decimal(_) => Ok(value),
            Value::Integer(n) => Ok(Value::Decimal(n.to_string())),
            Value::Float(f) => Value::decimal_from_f64(f)
                .ok_or_else(|| FusionError::Execution(format!("Cannot cast '{}' to DECIMAL", f))),
            Value::String(s) => Value::normalize_decimal(s.trim())
                .map(Value::Decimal)
                .ok_or_else(|| FusionError::Execution(format!("Cannot cast '{}' to DECIMAL", s))),
            other => Err(FusionError::Execution(format!(
                "Cannot cast {:?} to DECIMAL",
                other
            ))),
        }
    }

    fn is_true_boolean_literal(value: &str) -> bool {
        value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("t")
            || value == "1"
            || value.eq_ignore_ascii_case("yes")
            || value.eq_ignore_ascii_case("on")
    }

    fn is_false_boolean_literal(value: &str) -> bool {
        value.eq_ignore_ascii_case("false")
            || value.eq_ignore_ascii_case("f")
            || value == "0"
            || value.eq_ignore_ascii_case("no")
            || value.eq_ignore_ascii_case("off")
    }

    fn coerce_to_boolean(value: Value) -> Result<Value> {
        match value {
            Value::Boolean(_) => Ok(value),
            Value::Integer(n) => Ok(Value::Boolean(n != 0)),
            Value::Decimal(s) => Ok(Value::Boolean(s != "0")),
            Value::String(s) => {
                let trimmed = s.trim();
                if Self::is_true_boolean_literal(trimmed) {
                    Ok(Value::Boolean(true))
                } else if Self::is_false_boolean_literal(trimmed) {
                    Ok(Value::Boolean(false))
                } else {
                    Err(FusionError::Execution(format!(
                        "Cannot cast '{}' to BOOLEAN",
                        s
                    )))
                }
            }
            other => Err(FusionError::Execution(format!(
                "Cannot cast {:?} to BOOLEAN",
                other
            ))),
        }
    }

    fn coerce_to_date(value: Value) -> Result<Value> {
        match value {
            Value::Date(_) => Ok(value),
            Value::Timestamp(micros) => {
                let Some(dt) = chrono::DateTime::from_timestamp_micros(micros) else {
                    return Err(FusionError::Execution(format!(
                        "Cannot cast '{}' to DATE",
                        micros
                    )));
                };
                Ok(Value::Date(dt.naive_utc().date().num_days_from_ce()))
            }
            Value::String(s) => Value::date_from_str(&s)
                .ok_or_else(|| FusionError::Execution(format!("Cannot cast '{}' to DATE", s))),
            other => Err(FusionError::Execution(format!(
                "Cannot cast {:?} to DATE",
                other
            ))),
        }
    }

    fn coerce_to_timestamp(value: Value) -> Result<Value> {
        match value {
            Value::Timestamp(_) => Ok(value),
            Value::Date(days) => {
                let Some(date) = chrono::NaiveDate::from_num_days_from_ce_opt(days) else {
                    return Err(FusionError::Execution(format!(
                        "Cannot cast '{}' to TIMESTAMP",
                        days
                    )));
                };
                let dt = date.and_hms_opt(0, 0, 0).unwrap();
                Ok(Value::Timestamp(dt.and_utc().timestamp_micros()))
            }
            Value::Integer(value) => {
                let micros = if value.abs() >= 1_000_000_000_000_000 {
                    value / 1_000
                } else if value.abs() >= 1_000_000_000_000 {
                    value
                } else {
                    value.saturating_mul(1_000_000)
                };
                Ok(Value::Timestamp(micros))
            }
            Value::String(s) => Value::timestamp_from_str(&s)
                .ok_or_else(|| FusionError::Execution(format!("Cannot cast '{}' to TIMESTAMP", s))),
            other => Err(FusionError::Execution(format!(
                "Cannot cast {:?} to TIMESTAMP",
                other
            ))),
        }
    }

    fn coerce_to_interval(value: Value) -> Result<Value> {
        match value {
            Value::Interval(_) => Ok(value),
            Value::Integer(seconds) => Ok(Value::Interval(seconds.saturating_mul(1_000_000))),
            Value::String(s) => Value::interval_from_str(&s)
                .ok_or_else(|| FusionError::Execution(format!("Cannot cast '{}' to INTERVAL", s))),
            other => Err(FusionError::Execution(format!(
                "Cannot cast {:?} to INTERVAL",
                other
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Executor, Value};

    #[test]
    fn coerce_to_boolean_matches_ascii_case_without_lowercase_allocation() {
        for literal in [" TrUe ", "T", "1", "YeS", "oN"] {
            assert_eq!(
                Executor::coerce_to_boolean(Value::String(literal.to_string())).unwrap(),
                Value::Boolean(true)
            );
        }

        for literal in [" FaLsE ", "F", "0", "nO", "oFf"] {
            assert_eq!(
                Executor::coerce_to_boolean(Value::String(literal.to_string())).unwrap(),
                Value::Boolean(false)
            );
        }
    }

    #[test]
    fn coerce_to_boolean_preserves_invalid_error_text() {
        let error = Executor::coerce_to_boolean(Value::String(" maybe ".to_string())).unwrap_err();

        assert_eq!(
            error.to_string(),
            "Execution error: Cannot cast ' maybe ' to BOOLEAN"
        );
    }

    #[test]
    fn coerce_value_to_column_type_matches_type_names_without_uppercase_allocation() {
        assert_eq!(
            Executor::coerce_value_to_column_type(Value::String("42".to_string()), " iNt4 ")
                .unwrap(),
            Value::Integer(42)
        );
        assert_eq!(
            Executor::coerce_value_to_column_type(
                Value::String("1.5".to_string()),
                "DOUBLE precision"
            )
            .unwrap(),
            Value::Float(1.5)
        );
        assert_eq!(
            Executor::coerce_value_to_column_type(Value::String("YeS".to_string()), "bool")
                .unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            Executor::coerce_value_to_column_type(Value::Integer(7), "character varying(20)")
                .unwrap(),
            Value::String("7".to_string())
        );
    }

    #[test]
    fn type_name_helpers_match_prefixes_case_insensitively() {
        assert!(Executor::is_decimal_type_name("numeric(10,2)"));
        assert!(Executor::is_float_type_name("float(24)"));
        assert!(Executor::is_timestamp_type_name("timestamp(6)"));
        assert!(Executor::is_interval_type_name("interval day"));
        assert!(Executor::is_text_type_name("Character Large Object"));
        assert!(!Executor::is_timestamp_type_name(" timestamp"));
        assert!(!Executor::is_text_type_name("varchar2"));
    }
}
