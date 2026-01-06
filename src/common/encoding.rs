use crate::common::Value;

/// Encodes an i64 into a lexicographically comparable hex string.
/// We flip the sign bit so that:
/// i64::MIN (-2^63) -> 0x00...
/// 0 -> 0x80...
/// i64::MAX -> 0xFF...
pub fn encode_i64_comparable(i: i64) -> String {
    let u = (i as u64) ^ (1 << 63);
    format!("{:016x}", u)
}

/// Decodes a comparable hex string back to i64.
pub fn decode_i64_comparable(s: &str) -> Option<i64> {
    let u = u64::from_str_radix(s, 16).ok()?;
    Some((u ^ (1 << 63)) as i64)
}

/// Encodes a Value into a string key component.
/// - Integers are encoded as comparable hex.
/// - Strings are used as is (assuming they don't contain separators, or we accept potential issues for now).
/// - Others use generic string representation or UUID.
pub fn encode_key(v: &Value) -> String {
    match v {
        Value::Integer(i) => encode_i64_comparable(*i),
        Value::String(s) => s.clone(), // TODO: Escape separators?
        _ => v.to_string(),
    }
}
