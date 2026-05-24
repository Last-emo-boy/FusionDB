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
            Value::Float(3.14),
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

pub struct RowEncoder;

impl RowEncoder {
    pub fn encode(row: &[Value]) -> Vec<u8> {
        // Format: [Count: u16] [Offset1: u32] [Offset2: u32] ... [Data1] [Data2] ...
        let count = row.len() as u16;
        let mut offsets = Vec::with_capacity(count as usize);
        let mut data_buf = Vec::new();

        for val in row {
            offsets.push(data_buf.len() as u32);
            let bytes = bincode::serialize(val).unwrap_or_default();
            data_buf.extend_from_slice(&bytes);
        }

        // Calculate header size
        // Count (2) + Offsets (4 * count)
        let header_size = 2 + 4 * count as u32;

        let mut result = Vec::with_capacity(header_size as usize + data_buf.len());
        result.extend_from_slice(&count.to_le_bytes());
        for offset in offsets {
            let abs_offset = header_size + offset;
            result.extend_from_slice(&abs_offset.to_le_bytes());
        }
        result.extend_from_slice(&data_buf);
        result
    }
}

pub struct RowDecoder;

impl RowDecoder {
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

    // Partially decode row, returning full Vec<Value> but with Nulls for skipped columns
    pub fn decode_partial(data: &[u8], indices: &[usize]) -> bincode::Result<Vec<Value>> {
        if data.len() < 2 {
            return bincode::deserialize(data);
        }

        let count = u16::from_le_bytes([data[0], data[1]]);
        let header_size = 2 + 4 * count as usize;

        if data.len() < header_size {
            return bincode::deserialize(data);
        }

        let mut row = vec![Value::Null; count as usize];

        for &idx in indices {
            if idx >= count as usize {
                continue;
            }

            let off_pos = 2 + idx * 4;
            let start = u32::from_le_bytes([
                data[off_pos],
                data[off_pos + 1],
                data[off_pos + 2],
                data[off_pos + 3],
            ]) as usize;

            let end = if idx + 1 < count as usize {
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

            if start >= data.len() || end > data.len() {
                return bincode::deserialize(data);
            }

            let val_bytes = &data[start..end];
            row[idx] = bincode::deserialize(val_bytes).map_err(|e| {
                eprintln!("DEBUG: RowDecoder Partial Fail. Count: {}, Idx: {}, Start: {}, End: {}, DataLen: {}, Err: {}", count, idx, start, end, data.len(), e);
                e
            })?;
        }

        Ok(row)
    }
}
