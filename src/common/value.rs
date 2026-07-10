use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;

use chrono::Datelike;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Decimal(String),
    String(String),
    Date(i32),
    Timestamp(i64),
    Interval(i64),
    Blob(Vec<u8>),
    Vector(Vec<f32>),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
}

impl Value {
    pub fn date_from_str(value: &str) -> Option<Self> {
        let date = chrono::NaiveDate::parse_from_str(value.trim(), "%Y-%m-%d").ok()?;
        Some(Value::Date(date.num_days_from_ce()))
    }

    pub fn timestamp_from_str(value: &str) -> Option<Self> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return None;
        }

        if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(trimmed) {
            return Some(Value::Timestamp(timestamp.timestamp_micros()));
        }

        let fixed_offset_formats = [
            "%Y-%m-%d %H:%M:%S%.f%:z",
            "%Y-%m-%d %H:%M:%S%.f%z",
            "%Y-%m-%d %H:%M:%S%.f %:z",
            "%Y-%m-%d %H:%M:%S%.f %z",
            "%Y-%m-%dT%H:%M:%S%.f%:z",
            "%Y-%m-%dT%H:%M:%S%.f%z",
        ];
        for format in fixed_offset_formats {
            if let Ok(timestamp) = chrono::DateTime::parse_from_str(trimmed, format) {
                return Some(Value::Timestamp(timestamp.timestamp_micros()));
            }
        }
        if let Some(normalized) = Self::normalize_hour_only_timezone_offset(trimmed) {
            for format in fixed_offset_formats {
                if let Ok(timestamp) = chrono::DateTime::parse_from_str(&normalized, format) {
                    return Some(Value::Timestamp(timestamp.timestamp_micros()));
                }
            }
        }

        let normalized = trimmed.trim_end_matches('Z').replace('T', " ");
        let timestamp = chrono::NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| {
                chrono::NaiveDate::parse_from_str(&normalized, "%Y-%m-%d")
                    .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
            })
            .ok()?;
        Some(Value::Timestamp(timestamp.and_utc().timestamp_micros()))
    }

    fn normalize_hour_only_timezone_offset(value: &str) -> Option<String> {
        let split = value.rfind(['+', '-'])?;
        let suffix = &value[split..];
        if suffix.len() != 3 {
            return None;
        }
        let mut chars = suffix.chars();
        let sign = chars.next()?;
        if !matches!(sign, '+' | '-') || !chars.all(|ch| ch.is_ascii_digit()) {
            return None;
        }
        Some(format!("{}{}:00", &value[..split], suffix))
    }

    pub fn interval_from_str(value: &str) -> Option<Self> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return None;
        }
        if let Ok(seconds) = trimmed.parse::<i64>() {
            return Some(Value::Interval(seconds.saturating_mul(1_000_000)));
        }

        let mut total_micros = 0i64;
        let mut saw_part = false;
        let parts = trimmed.split_whitespace().collect::<Vec<_>>();
        let mut idx = 0usize;
        while idx + 1 < parts.len() {
            let Ok(amount) = parts[idx].parse::<f64>() else {
                return None;
            };
            let unit = parts[idx + 1].to_ascii_lowercase();
            let seconds = match unit.trim_end_matches('s') {
                "microsecond" => {
                    total_micros =
                        total_micros.saturating_add((amount.round() as i64).max(i64::MIN));
                    saw_part = true;
                    idx += 2;
                    continue;
                }
                "millisecond" => amount / 1_000.0,
                "second" => amount,
                "minute" => amount * 60.0,
                "hour" => amount * 3_600.0,
                "day" => amount * 86_400.0,
                "week" => amount * 604_800.0,
                "month" => amount * 2_592_000.0,
                "year" => amount * 31_536_000.0,
                _ => return None,
            };
            total_micros = total_micros.saturating_add((seconds * 1_000_000.0).round() as i64);
            saw_part = true;
            idx += 2;
        }

        saw_part.then_some(Value::Interval(total_micros))
    }

    pub fn normalize_decimal(value: &str) -> Option<String> {
        let mut s = value.trim();
        if s.is_empty() {
            return None;
        }
        let negative = s.starts_with('-');
        if negative || s.starts_with('+') {
            s = &s[1..];
        }
        if s.is_empty() || s.eq_ignore_ascii_case("nan") || s.eq_ignore_ascii_case("inf") {
            return None;
        }

        let mut parts = s.split('.');
        let int_part = parts.next().unwrap_or_default();
        let frac_part = parts.next();
        if parts.next().is_some() {
            return None;
        }
        if int_part.is_empty() && frac_part.map_or(true, str::is_empty) {
            return None;
        }
        if !int_part.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let mut int_digits = int_part.trim_start_matches('0').to_string();
        if int_digits.is_empty() {
            int_digits.push('0');
        }

        let mut frac_digits = frac_part.unwrap_or_default().to_string();
        if !frac_digits.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        while frac_digits.ends_with('0') {
            frac_digits.pop();
        }

        let is_zero = int_digits == "0" && frac_digits.is_empty();
        let mut out = String::new();
        if negative && !is_zero {
            out.push('-');
        }
        out.push_str(&int_digits);
        if !frac_digits.is_empty() {
            out.push('.');
            out.push_str(&frac_digits);
        }
        Some(out)
    }

    pub fn decimal_from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        Self::normalize_decimal(&format!("{:.12}", value)).map(Value::Decimal)
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Integer(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Decimal(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    pub fn to_plain_string(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Decimal(s) => s.clone(),
            Value::String(s) => s.clone(),
            Value::Date(days) => Self::format_date(*days),
            Value::Timestamp(micros) => Self::format_timestamp(*micros),
            Value::Interval(micros) => Self::format_interval(*micros),
            Value::Blob(b) => format!("<BLOB len={}>", b.len()),
            Value::Vector(v) => format!("{:?}", v),
            Value::Array(v) => format!("{:?}", v),
            Value::Object(v) => format!("{:?}", v),
        }
    }

    pub fn format_date(days_from_ce: i32) -> String {
        chrono::NaiveDate::from_num_days_from_ce_opt(days_from_ce)
            .map(|date| date.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| days_from_ce.to_string())
    }

    pub fn format_timestamp(micros: i64) -> String {
        chrono::DateTime::from_timestamp_micros(micros)
            .map(|dt| dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            .unwrap_or_else(|| micros.to_string())
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }

    pub fn format_interval(micros: i64) -> String {
        if micros % 1_000_000 == 0 {
            format!("{} seconds", micros / 1_000_000)
        } else {
            format!("{} microseconds", micros)
        }
    }

    fn compare_decimal_strings(left: &str, right: &str) -> Ordering {
        let left = Self::normalize_decimal(left).unwrap_or_else(|| "0".to_string());
        let right = Self::normalize_decimal(right).unwrap_or_else(|| "0".to_string());
        let left_neg = left.starts_with('-');
        let right_neg = right.starts_with('-');
        if left_neg != right_neg {
            return if left_neg {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        let l = left.trim_start_matches('-');
        let r = right.trim_start_matches('-');
        let (li, lf) = l.split_once('.').unwrap_or((l, ""));
        let (ri, rf) = r.split_once('.').unwrap_or((r, ""));
        let abs_order = li
            .len()
            .cmp(&ri.len())
            .then_with(|| li.cmp(ri))
            .then_with(|| {
                let max = lf.len().max(rf.len());
                let mut lp = lf.to_string();
                let mut rp = rf.to_string();
                lp.extend(std::iter::repeat('0').take(max - lf.len()));
                rp.extend(std::iter::repeat('0').take(max - rf.len()));
                lp.cmp(&rp)
            });
        if left_neg {
            abs_order.reverse()
        } else {
            abs_order
        }
    }

    pub fn from_json(v: &serde_json::Value) -> Value {
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
                Value::Array(arr.iter().map(Value::from_json).collect())
            }
            serde_json::Value::Object(obj) => {
                let mut map = HashMap::new();
                for (k, v) in obj {
                    map.insert(k.clone(), Value::from_json(v));
                }
                Value::Object(map)
            }
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Boolean(b) => serde_json::Value::Bool(*b),
            Value::Integer(i) => serde_json::Value::Number((*i).into()),
            Value::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::Decimal(s) => serde_json::Value::String(s.clone()),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Date(days) => serde_json::Value::String(Self::format_date(*days)),
            Value::Timestamp(micros) => serde_json::Value::String(Self::format_timestamp(*micros)),
            Value::Interval(micros) => serde_json::Value::String(Self::format_interval(*micros)),
            Value::Blob(b) => serde_json::Value::String(format!("<BLOB len={}>", b.len())), // Base64?
            Value::Vector(v) => serde_json::json!(v),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|x| x.to_json()).collect())
            }
            Value::Object(obj) => {
                let mut map = serde_json::Map::new();
                for (k, v) in obj {
                    map.insert(k.clone(), v.to_json());
                }
                serde_json::Value::Object(map)
            }
        }
    }

    pub fn compare(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            (Value::Integer(i1), Value::Integer(i2)) => i1.cmp(i2),
            (Value::Float(f1), Value::Float(f2)) => f1.partial_cmp(f2).unwrap_or(Ordering::Equal),
            (Value::Integer(i1), Value::Float(f2)) => {
                (*i1 as f64).partial_cmp(f2).unwrap_or(Ordering::Equal)
            }
            (Value::Float(f1), Value::Integer(i2)) => {
                f1.partial_cmp(&(*i2 as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::Decimal(d1), Value::Decimal(d2)) => Self::compare_decimal_strings(d1, d2),
            (Value::Decimal(d), Value::Integer(i)) | (Value::Integer(i), Value::Decimal(d)) => {
                let ord = Self::compare_decimal_strings(d, &i.to_string());
                if matches!(self, Value::Integer(_)) {
                    ord.reverse()
                } else {
                    ord
                }
            }
            (Value::Decimal(d), Value::Float(f)) | (Value::Float(f), Value::Decimal(d)) => {
                let ord = d
                    .parse::<f64>()
                    .ok()
                    .and_then(|d| d.partial_cmp(f))
                    .unwrap_or(Ordering::Equal);
                if matches!(self, Value::Float(_)) {
                    ord.reverse()
                } else {
                    ord
                }
            }

            (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
            (Value::Boolean(b1), Value::Boolean(b2)) => b1.cmp(b2),
            (Value::Date(d1), Value::Date(d2)) => d1.cmp(d2),
            (Value::Timestamp(t1), Value::Timestamp(t2)) => t1.cmp(t2),
            (Value::Date(d), Value::Timestamp(t)) => {
                let day_micros = (*d as i64 - 719_163) * 86_400_000_000;
                day_micros.cmp(t)
            }
            (Value::Timestamp(t), Value::Date(d)) => {
                let day_micros = (*d as i64 - 719_163) * 86_400_000_000;
                t.cmp(&day_micros)
            }
            (Value::Interval(i1), Value::Interval(i2)) => i1.cmp(i2),

            (v1, v2) => {
                let t1 = v1.get_type_order();
                let t2 = v2.get_type_order();
                t1.cmp(&t2)
            }
        }
    }

    pub fn get_type_order(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Boolean(_) => 1,
            Value::Integer(_) | Value::Float(_) | Value::Decimal(_) => 2,
            Value::String(_) => 3,
            Value::Date(_) | Value::Timestamp(_) => 4,
            Value::Interval(_) => 5,
            Value::Blob(_) => 6,
            Value::Vector(_) => 7,
            Value::Array(_) => 8,
            Value::Object(_) => 9,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Boolean(b) => b.hash(state),
            Value::Integer(i) => i.hash(state),
            Value::Float(f) => {
                // Float doesn't implement Hash. We use bytes representation.
                // Note: NaN != NaN, so HashMap might behave weirdly if keys are NaN.
                // For DB grouping, usually we treat NaNs as equal.
                // Here we just hash the bit pattern.
                f.to_bits().hash(state)
            }
            Value::Decimal(d) => d.hash(state),
            Value::String(s) => s.hash(state),
            Value::Date(d) => d.hash(state),
            Value::Timestamp(t) => t.hash(state),
            Value::Interval(i) => i.hash(state),
            Value::Blob(b) => b.hash(state),
            Value::Vector(v) => {
                for f in v {
                    f.to_bits().hash(state);
                }
            }
            Value::Array(arr) => arr.hash(state),
            Value::Object(obj) => {
                // HashMap is not Hashable because order is undefined.
                // We need to sort keys to hash it deterministically.
                let mut sorted_keys: Vec<&String> = obj.keys().collect();
                sorted_keys.sort();
                for k in sorted_keys {
                    k.hash(state);
                    obj.get(k).unwrap().hash(state);
                }
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Decimal(d) => write!(f, "{}", d),
            Value::String(s) => write!(f, "'{}'", s),
            Value::Date(days) => write!(f, "{}", Self::format_date(*days)),
            Value::Timestamp(micros) => write!(f, "{}", Self::format_timestamp(*micros)),
            Value::Interval(micros) => write!(f, "{}", Self::format_interval(*micros)),
            Value::Blob(b) => write!(f, "<BLOB len={}>", b.len()),
            Value::Vector(v) => write!(f, "<VECTOR dim={}>", v.len()),
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
            Value::Object(obj) => {
                write!(f, "{{")?;
                for (i, (k, v)) in obj.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", k, v)?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_integers() {
        assert_eq!(
            Value::Integer(1).compare(&Value::Integer(2)),
            Ordering::Less
        );
        assert_eq!(
            Value::Integer(2).compare(&Value::Integer(2)),
            Ordering::Equal
        );
        assert_eq!(
            Value::Integer(3).compare(&Value::Integer(2)),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_mixed_numeric() {
        assert_eq!(
            Value::Integer(1).compare(&Value::Float(1.5)),
            Ordering::Less
        );
        assert_eq!(
            Value::Float(2.5).compare(&Value::Integer(2)),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_null_ordering() {
        assert_eq!(Value::Null.compare(&Value::Integer(1)), Ordering::Less);
        assert_eq!(Value::Integer(1).compare(&Value::Null), Ordering::Greater);
        assert_eq!(Value::Null.compare(&Value::Null), Ordering::Equal);
    }

    #[test]
    fn test_compare_strings() {
        assert_eq!(
            Value::String("abc".into()).compare(&Value::String("def".into())),
            Ordering::Less
        );
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", Value::Null), "NULL");
        assert_eq!(format!("{}", Value::Integer(42)), "42");
        assert_eq!(format!("{}", Value::Float(2.5)), "2.5");
        assert_eq!(format!("{}", Value::Boolean(true)), "true");
        assert_eq!(format!("{}", Value::String("hello".into())), "'hello'");
    }

    #[test]
    fn test_from_conversions() {
        let v: Value = 42i64.into();
        assert_eq!(v, Value::Integer(42));

        let v: Value = 2.5f64.into();
        assert_eq!(v, Value::Float(2.5));

        let v: Value = "hello".into();
        assert_eq!(v, Value::String("hello".to_string()));

        let v: Value = true.into();
        assert_eq!(v, Value::Boolean(true));
    }

    #[test]
    fn test_json_roundtrip() {
        let original = Value::Integer(42);
        let json = original.to_json();
        let restored = Value::from_json(&json);
        assert_eq!(original, restored);

        let original = Value::String("test".into());
        let json = original.to_json();
        let restored = Value::from_json(&json);
        assert_eq!(original, restored);
    }

    #[test]
    fn test_hash_equality() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Value::Integer(1));
        set.insert(Value::Integer(1));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_type_order() {
        assert!(Value::Null.get_type_order() < Value::Boolean(true).get_type_order());
        assert!(Value::Boolean(true).get_type_order() < Value::Integer(0).get_type_order());
        assert_eq!(
            Value::Integer(0).get_type_order(),
            Value::Float(0.0).get_type_order()
        );
        assert!(Value::Integer(0).get_type_order() < Value::String("".into()).get_type_order());
    }
}
