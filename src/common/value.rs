use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;

use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Blob(Vec<u8>),
    Vector(Vec<f32>),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
}

impl Value {
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
            Value::String(s) => serde_json::Value::String(s.clone()),
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

            (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
            (Value::Boolean(b1), Value::Boolean(b2)) => b1.cmp(b2),

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
            Value::Integer(_) | Value::Float(_) => 2,
            Value::String(_) => 3,
            Value::Blob(_) => 4,
            Value::Vector(_) => 5,
            Value::Array(_) => 6,
            Value::Object(_) => 7,
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
            Value::String(s) => s.hash(state),
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
            Value::String(s) => write!(f, "'{}'", s),
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
