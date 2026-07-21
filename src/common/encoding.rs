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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Value;

    #[test]
    fn test_row_encoding_decoding() {
        let row = vec![
            Value::Integer(12345),
            Value::String("hello world".to_string()),
            Value::Boolean(true),
            Value::Float(2.5),
            Value::Null,
        ];

        let encoded = RowEncoder::encode(&row);
        let decoded = RowDecoder::decode(&encoded).expect("Decoding failed");

        assert_eq!(row, decoded);
    }

    #[test]
    fn test_row_encoding_partial() {
        let row = vec![
            Value::Integer(12345),
            Value::String("hello world".to_string()),
            Value::Boolean(true),
        ];

        let encoded = RowEncoder::encode(&row);

        // Decode only index 1 (String)
        let decoded = RowDecoder::decode_partial(&encoded, &[1]).expect("Decoding failed");

        assert_eq!(decoded[0], Value::Null);
        assert_eq!(decoded[1], Value::String("hello world".to_string()));
        assert_eq!(decoded[2], Value::Null);
    }

    #[test]
    fn test_row_encoding_partial_duplicate_indices() {
        let row = vec![
            Value::Integer(12345),
            Value::String("hello world".to_string()),
            Value::Boolean(true),
        ];

        let encoded = RowEncoder::encode(&row);
        let decoded = RowDecoder::decode_partial(&encoded, &[1, 1]).expect("Decoding failed");

        assert_eq!(decoded[0], Value::Null);
        assert_eq!(decoded[1], Value::String("hello world".to_string()));
        assert_eq!(decoded[2], Value::Null);
    }

    #[test]
    fn test_row_encoding_single_column() {
        let row = vec![
            Value::Integer(12345),
            Value::String("hello world".to_string()),
            Value::Boolean(true),
        ];

        let encoded = RowEncoder::encode(&row);

        let decoded = RowDecoder::decode_column(&encoded, 1).expect("Decoding failed");
        assert_eq!(decoded, Some(Value::String("hello world".to_string())));
        assert_eq!(RowDecoder::decode_column(&encoded, 4).unwrap(), None);
    }

    #[test]
    fn test_rollback_scenario() {
        let row = vec![
            Value::Integer(9999999),
            Value::String("rollback_test".to_string()),
        ];

        let encoded = RowEncoder::encode(&row);
        let decoded = RowDecoder::decode(&encoded).expect("Decoding failed");
        assert_eq!(row, decoded);
    }

    #[test]
    fn test_i64_comparable_ordering() {
        let a = encode_i64_comparable(-100);
        let b = encode_i64_comparable(0);
        let c = encode_i64_comparable(100);
        // Lexicographic ordering should match numeric ordering
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn test_i64_comparable_roundtrip() {
        for val in [i64::MIN, -1, 0, 1, i64::MAX] {
            let encoded = encode_i64_comparable(val);
            let decoded = decode_i64_comparable(&encoded).unwrap();
            assert_eq!(val, decoded, "Roundtrip failed for {}", val);
        }
    }

    #[test]
    fn test_encode_key_integer() {
        let key = encode_key(&Value::Integer(42));
        assert_eq!(key, encode_i64_comparable(42));
    }

    #[test]
    fn test_encode_key_string() {
        let key = encode_key(&Value::String("hello".to_string()));
        assert_eq!(key, "hello");
    }

    #[test]
    fn test_row_encoding_empty() {
        let row: Vec<Value> = vec![];
        let encoded = RowEncoder::encode(&row);
        let decoded = RowDecoder::decode(&encoded).expect("Decoding failed");
        assert_eq!(row, decoded);
    }

    #[test]
    fn test_row_encoding_single_null() {
        let row = vec![Value::Null];
        let encoded = RowEncoder::encode(&row);
        let decoded = RowDecoder::decode(&encoded).expect("Decoding failed");
        assert_eq!(row, decoded);
    }

    #[test]
    fn test_decode_scalar_span_matches_bincode_for_every_variant() {
        let mut float_nan_negative = f64::NAN;
        if float_nan_negative.is_sign_positive() {
            float_nan_negative = -float_nan_negative;
        }
        let values = vec![
            Value::Null,
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Integer(0),
            Value::Integer(-1),
            Value::Integer(i64::MIN),
            Value::Integer(i64::MAX),
            Value::Float(0.0),
            Value::Float(-0.0),
            Value::Float(1.5e-300),
            Value::Float(f64::NAN),
            Value::Float(float_nan_negative),
            Value::Float(f64::INFINITY),
            Value::Decimal("12345.6789".to_string()),
            Value::Decimal(String::new()),
            Value::String("héllo, wörld".to_string()),
            Value::String(String::new()),
            Value::Date(0),
            Value::Date(-719_162),
            Value::Timestamp(i64::MAX),
            Value::Interval(-42),
            Value::Blob(vec![0, 1, 2, 255]),
            Value::Vector(vec![1.0, -2.5]),
            Value::Array(vec![Value::Integer(1), Value::Null]),
        ];

        for value in &values {
            let span = bincode::serialize(value).expect("value serializes");
            let via_bincode: Value = bincode::deserialize(&span).expect("value deserializes");
            let row = RowEncoder::encode(std::slice::from_ref(value));
            let via_column = RowDecoder::decode_column(&row, 0)
                .expect("column decodes")
                .expect("column exists");
            match (value, &via_column) {
                (Value::Float(expected), Value::Float(actual)) => {
                    assert_eq!(expected.to_bits(), actual.to_bits(), "bits for {:?}", value);
                }
                _ => assert_eq!(&via_column, value, "decode_column for {:?}", value),
            }
            match (value, &via_bincode) {
                (Value::Float(expected), Value::Float(actual)) => {
                    assert_eq!(expected.to_bits(), actual.to_bits(), "bits for {:?}", value);
                }
                _ => assert_eq!(&via_bincode, value, "bincode roundtrip for {:?}", value),
            }
        }
    }

    #[test]
    fn test_decode_scalar_span_rejects_malformed_spans() {
        // Every rejection must fall back to bincode so malformed data stays a
        // loud error (or, for trailing bytes, keeps bincode's tolerance).
        assert_eq!(RowDecoder::decode_scalar_span(&[]), None);
        assert_eq!(RowDecoder::decode_scalar_span(&[2, 0, 0]), None);
        // Boolean with an out-of-range payload byte.
        assert_eq!(RowDecoder::decode_scalar_span(&[1, 0, 0, 0, 2]), None);
        // Integer with a truncated payload.
        assert_eq!(RowDecoder::decode_scalar_span(&[2, 0, 0, 0, 1, 2, 3]), None);
        // Null with trailing bytes: bincode would tolerate, fast path defers.
        assert_eq!(RowDecoder::decode_scalar_span(&[0, 0, 0, 0, 9]), None);
        // String whose declared length disagrees with the span.
        let mut bad_string = bincode::serialize(&Value::String("ab".to_string())).unwrap();
        bad_string.pop();
        assert_eq!(RowDecoder::decode_scalar_span(&bad_string), None);
        // String with invalid UTF-8 payload.
        let mut bad_utf8 = bincode::serialize(&Value::String("ab".to_string())).unwrap();
        let last = bad_utf8.len() - 1;
        bad_utf8[last] = 0xFF;
        assert_eq!(RowDecoder::decode_scalar_span(&bad_utf8), None);
        // Non-scalar variants defer to bincode.
        let blob = bincode::serialize(&Value::Blob(vec![1, 2])).unwrap();
        assert_eq!(RowDecoder::decode_scalar_span(&blob), None);
    }
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
        Value::Date(days) => encode_i64_comparable(*days as i64),
        Value::Timestamp(micros) | Value::Interval(micros) => encode_i64_comparable(*micros),
        Value::String(s) => s.clone(), // TODO: Escape separators?
        _ => v.to_string(),
    }
}

pub struct RowEncoder;

impl RowEncoder {
    pub fn encode(row: &[Value]) -> Vec<u8> {
        // Format: [Count: u16] [Offset1: u32] [Offset2: u32] ... [Data1] [Data2] ...
        let count = row.len() as u16;

        // Count (2) + offsets (4 * count)
        let header_size = 2 + 4 * count as usize;
        let mut result = Vec::with_capacity(header_size);
        result.extend_from_slice(&count.to_le_bytes());

        result.resize(header_size, 0);
        for (idx, val) in row.iter().enumerate() {
            let abs_offset = result.len() as u32;
            let off_pos = 2 + idx * 4;
            result[off_pos..off_pos + 4].copy_from_slice(&abs_offset.to_le_bytes());

            let _ = bincode::serialize_into(&mut result, val);
        }

        result
    }
}

pub struct RowDecoder;

impl RowDecoder {
    fn column_bounds(data: &[u8], idx: usize) -> Option<(usize, usize)> {
        if data.len() < 2 {
            return None;
        }

        let count = u16::from_le_bytes([data[0], data[1]]) as usize;
        let header_size = 2 + 4 * count;

        if data.len() < header_size || idx >= count {
            return None;
        }

        let off_pos = 2 + idx * 4;
        let start = u32::from_le_bytes([
            data[off_pos],
            data[off_pos + 1],
            data[off_pos + 2],
            data[off_pos + 3],
        ]) as usize;

        let end = if idx + 1 < count {
            let next_off_pos = off_pos + 4;
            u32::from_le_bytes([
                data[next_off_pos],
                data[next_off_pos + 1],
                data[next_off_pos + 2],
                data[next_off_pos + 3],
            ]) as usize
        } else {
            data.len()
        };

        if start > data.len() || end > data.len() || start > end {
            return None;
        }

        Some((start, end))
    }

    pub fn decode(data: &[u8]) -> bincode::Result<Vec<Value>> {
        if data.len() < 2 {
            return bincode::deserialize(data);
        }

        let count = u16::from_le_bytes([data[0], data[1]]);
        let header_size = 2 + 4 * count as usize;

        if data.len() < header_size {
            return bincode::deserialize(data);
        }

        let mut row = Vec::with_capacity(count as usize);
        for i in 0..count as usize {
            let off_pos = 2 + i * 4;
            let start = u32::from_le_bytes([
                data[off_pos],
                data[off_pos + 1],
                data[off_pos + 2],
                data[off_pos + 3],
            ]) as usize;

            let end = if i + 1 < count as usize {
                let next_off_pos = off_pos + 4;
                u32::from_le_bytes([
                    data[next_off_pos],
                    data[next_off_pos + 1],
                    data[next_off_pos + 2],
                    data[next_off_pos + 3],
                ]) as usize
            } else {
                data.len()
            };

            if start > data.len() || end > data.len() || start > end {
                return bincode::deserialize(data);
            }

            let val_bytes = &data[start..end];
            let val: Value = bincode::deserialize(val_bytes)?;
            row.push(val);
        }

        Ok(row)
    }

    /// Manual decode of one bincode-encoded `Value` span, mirroring the
    /// bincode 1.x fixint little-endian layout (u32 variant tag + payload) for
    /// the scalar variants. Returns `None` for anything that is not an
    /// exactly-sized well-formed scalar span; callers must then fall back to
    /// `bincode::deserialize` so malformed data keeps the identical (loud)
    /// error behavior and non-scalar variants decode as before.
    fn decode_scalar_span(bytes: &[u8]) -> Option<Value> {
        let (tag, body) = bytes.split_first_chunk::<4>()?;
        match u32::from_le_bytes(*tag) {
            0 if body.is_empty() => Some(Value::Null),
            1 => match body {
                [0] => Some(Value::Boolean(false)),
                [1] => Some(Value::Boolean(true)),
                _ => None,
            },
            2 => Some(Value::Integer(i64::from_le_bytes(body.try_into().ok()?))),
            3 => Some(Value::Float(f64::from_le_bytes(body.try_into().ok()?))),
            tag @ (4 | 5) => {
                let (len, text) = body.split_first_chunk::<8>()?;
                if u64::from_le_bytes(*len) != text.len() as u64 {
                    return None;
                }
                let text = std::str::from_utf8(text).ok()?.to_string();
                Some(match tag {
                    4 => Value::Decimal(text),
                    _ => Value::String(text),
                })
            }
            6 => Some(Value::Date(i32::from_le_bytes(body.try_into().ok()?))),
            7 => Some(Value::Timestamp(i64::from_le_bytes(body.try_into().ok()?))),
            8 => Some(Value::Interval(i64::from_le_bytes(body.try_into().ok()?))),
            _ => None,
        }
    }

    pub fn decode_column(data: &[u8], idx: usize) -> bincode::Result<Option<Value>> {
        if data.len() < 2 {
            let row: Vec<Value> = bincode::deserialize(data)?;
            return Ok(row.get(idx).cloned());
        }

        let Some((start, end)) = Self::column_bounds(data, idx) else {
            return Ok(None);
        };

        let span = &data[start..end];
        if let Some(value) = Self::decode_scalar_span(span) {
            return Ok(Some(value));
        }
        bincode::deserialize(span).map(Some)
    }

    // Partially decode row, returning full Vec<Value> but with Nulls for skipped columns
    pub fn decode_partial(data: &[u8], indices: &[usize]) -> bincode::Result<Vec<Value>> {
        if data.len() < 2 {
            return bincode::deserialize(data);
        }

        let count = u16::from_le_bytes([data[0], data[1]]) as usize;
        let header_size = 2 + 4 * count;

        if data.len() < header_size {
            return bincode::deserialize(data);
        }

        let mut row = vec![Value::Null; count];

        for &idx in indices {
            if idx >= count {
                continue;
            }

            let off_pos = 2 + idx * 4;
            let start = u32::from_le_bytes([
                data[off_pos],
                data[off_pos + 1],
                data[off_pos + 2],
                data[off_pos + 3],
            ]) as usize;

            let end = if idx + 1 < count {
                let next_off_pos = off_pos + 4;
                u32::from_le_bytes([
                    data[next_off_pos],
                    data[next_off_pos + 1],
                    data[next_off_pos + 2],
                    data[next_off_pos + 3],
                ]) as usize
            } else {
                data.len()
            };

            if start > data.len() || end > data.len() || start > end {
                return bincode::deserialize(data);
            }

            let val_bytes = &data[start..end];
            row[idx] = bincode::deserialize(val_bytes)?;
        }

        Ok(row)
    }
}
