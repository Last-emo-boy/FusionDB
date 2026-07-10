const STRUCTURED_KEY_MAGIC: &[u8] = b"\x02FDBK";
const ORDERED_VALUE_TYPE_INTEGER: u8 = 0x11;
const ORDERED_VALUE_TYPE_BYTES: u8 = 0x12;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum KeyNamespace {
    Data = 1,
    SecondaryIndex = 2,
    CompositeIndex = 3,
    FullText = 4,
    CountSummary = 5,
}

impl KeyNamespace {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Data),
            2 => Some(Self::SecondaryIndex),
            3 => Some(Self::CompositeIndex),
            4 => Some(Self::FullText),
            5 => Some(Self::CountSummary),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StructuredKey<'a> {
    namespace: KeyNamespace,
    components: Vec<&'a [u8]>,
}

impl<'a> StructuredKey<'a> {
    pub(crate) fn namespace(&self) -> KeyNamespace {
        self.namespace
    }

    pub(crate) fn components(&self) -> &[&'a [u8]] {
        &self.components
    }
}

pub(crate) fn encode_key(namespace: KeyNamespace, components: &[&[u8]]) -> Vec<u8> {
    let capacity = STRUCTURED_KEY_MAGIC.len()
        + 1
        + components
            .iter()
            .map(|component| encoded_uvarint_len(component.len()) + component.len())
            .sum::<usize>();
    let mut key = Vec::with_capacity(capacity);
    key.extend_from_slice(STRUCTURED_KEY_MAGIC);
    key.push(namespace as u8);
    for component in components {
        encode_uvarint(component.len(), &mut key);
        key.extend_from_slice(component);
    }
    key
}

pub(crate) fn encode_prefix(namespace: KeyNamespace, components: &[&[u8]]) -> Vec<u8> {
    encode_key(namespace, components)
}

pub(crate) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for idx in (0..end.len()).rev() {
        if end[idx] != u8::MAX {
            end[idx] += 1;
            end.truncate(idx + 1);
            return Some(end);
        }
    }
    None
}

pub(crate) fn parse_key(input: &[u8]) -> Option<StructuredKey<'_>> {
    let remaining = input.strip_prefix(STRUCTURED_KEY_MAGIC)?;
    let (&namespace_byte, mut remaining) = remaining.split_first()?;
    let namespace = KeyNamespace::from_byte(namespace_byte)?;
    let mut components = Vec::new();

    while !remaining.is_empty() {
        let (component_len, consumed) = decode_uvarint(remaining)?;
        remaining = &remaining[consumed..];
        if component_len > remaining.len() {
            return None;
        }
        let (component, rest) = remaining.split_at(component_len);
        components.push(component);
        remaining = rest;
    }

    Some(StructuredKey {
        namespace,
        components,
    })
}

pub(crate) fn parse_key_exact(
    input: &[u8],
    namespace: KeyNamespace,
    component_count: usize,
) -> Option<StructuredKey<'_>> {
    let key = parse_key(input)?;
    if key.namespace != namespace || key.components.len() != component_count {
        return None;
    }
    Some(key)
}

pub(crate) fn encode_ordered_i64_component(value: i64) -> [u8; 9] {
    let encoded = ((value as u64) ^ (1 << 63)).to_be_bytes();
    let mut component = [0; 9];
    component[0] = ORDERED_VALUE_TYPE_INTEGER;
    component[1..].copy_from_slice(&encoded);
    component
}

pub(crate) fn decode_ordered_i64_component(component: &[u8]) -> Option<i64> {
    if component.len() != 9 || component[0] != ORDERED_VALUE_TYPE_INTEGER {
        return None;
    }
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&component[1..]);
    Some((u64::from_be_bytes(bytes) ^ (1 << 63)) as i64)
}

pub(crate) fn encode_ordered_bytes_component(bytes: &[u8]) -> Vec<u8> {
    let mut component = Vec::with_capacity(1 + bytes.len() + 2);
    component.push(ORDERED_VALUE_TYPE_BYTES);
    for &byte in bytes {
        if byte == 0 {
            component.push(0);
            component.push(0xff);
        } else {
            component.push(byte);
        }
    }
    component.push(0);
    component.push(0);
    component
}

pub(crate) fn decode_ordered_bytes_component(component: &[u8]) -> Option<Vec<u8>> {
    let payload = component.strip_prefix(&[ORDERED_VALUE_TYPE_BYTES])?;
    let mut decoded = Vec::with_capacity(payload.len());
    let mut idx = 0;
    while idx < payload.len() {
        let byte = payload[idx];
        if byte != 0 {
            decoded.push(byte);
            idx += 1;
            continue;
        }

        let marker = *payload.get(idx + 1)?;
        match marker {
            0 => {
                if idx + 2 == payload.len() {
                    return Some(decoded);
                }
                return None;
            }
            0xff => {
                decoded.push(0);
                idx += 2;
            }
            _ => return None,
        }
    }
    None
}

fn encode_uvarint(mut value: usize, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_uvarint(input: &[u8]) -> Option<(usize, usize)> {
    let mut value = 0u64;
    for (idx, &byte) in input.iter().take(10).enumerate() {
        let chunk = (byte & 0x7f) as u64;
        if idx == 9 && chunk > 1 {
            return None;
        }
        value |= chunk << (idx * 7);
        if byte & 0x80 == 0 {
            let value = usize::try_from(value).ok()?;
            return Some((value, idx + 1));
        }
    }
    None
}

fn encoded_uvarint_len(mut value: usize) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        len += 1;
        value >>= 7;
    }
    len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_key_roundtrips_delimiter_and_binary_components() {
        let table = b"tenant:archive,orders|2026";
        let column = b"quoted:col,with|delims";
        let row_id = b"row\0id:\xff";
        let key = encode_key(KeyNamespace::Data, &[table, column, row_id]);
        let parsed = parse_key_exact(&key, KeyNamespace::Data, 3).expect("key should parse");

        assert_eq!(parsed.namespace(), KeyNamespace::Data);
        assert_eq!(parsed.components(), &[&table[..], &column[..], &row_id[..]]);
    }

    #[test]
    fn structured_key_prefix_is_prefix_of_longer_key() {
        let table = b"orders";
        let prefix = encode_prefix(KeyNamespace::SecondaryIndex, &[table]);
        let key = encode_key(
            KeyNamespace::SecondaryIndex,
            &[table, b"status", b"open", b"0000000000000001"],
        );
        assert!(key.starts_with(&prefix));
        assert_eq!(
            parse_key_exact(&prefix, KeyNamespace::SecondaryIndex, 1)
                .expect("prefix should parse")
                .components(),
            &[&table[..]]
        );
    }

    #[test]
    fn structured_key_rejects_malformed_inputs() {
        let key = encode_key(KeyNamespace::CompositeIndex, &[b"orders", b"a,b"]);
        assert!(parse_key(&key[1..]).is_none());

        let mut unknown_namespace = key.clone();
        unknown_namespace[STRUCTURED_KEY_MAGIC.len()] = 0xff;
        assert!(parse_key(&unknown_namespace).is_none());

        let mut truncated_component = key.clone();
        truncated_component.pop();
        assert!(parse_key(&truncated_component).is_none());

        let mut unterminated_varint = encode_key(KeyNamespace::Data, &[]);
        unterminated_varint.push(0x80);
        assert!(parse_key(&unterminated_varint).is_none());
    }

    #[test]
    fn structured_key_exact_parser_rejects_wrong_shape() {
        let key = encode_key(KeyNamespace::FullText, &[b"docs", b"body", b"token"]);

        assert!(parse_key_exact(&key, KeyNamespace::FullText, 3).is_some());
        assert!(parse_key_exact(&key, KeyNamespace::FullText, 2).is_none());
        assert!(parse_key_exact(&key, KeyNamespace::CountSummary, 3).is_none());
    }

    #[test]
    fn prefix_end_returns_exclusive_lexicographic_bound() {
        assert_eq!(prefix_end(&[0x01, 0xff]), Some(vec![0x02]));
        assert_eq!(prefix_end(&[0x01, 0x7f]), Some(vec![0x01, 0x80]));
        assert_eq!(prefix_end(&[0xff, 0xff]), None);
    }

    #[test]
    fn ordered_i64_components_sort_numerically() {
        let mut encoded = [i64::MAX, 1, 0, -1, i64::MIN]
            .into_iter()
            .map(|value| (encode_ordered_i64_component(value), value))
            .collect::<Vec<_>>();
        encoded.sort_by(|left, right| left.0.cmp(&right.0));

        let values = encoded.iter().map(|(_, value)| *value).collect::<Vec<_>>();
        assert_eq!(values, vec![i64::MIN, -1, 0, 1, i64::MAX]);

        for (component, value) in encoded {
            assert_eq!(decode_ordered_i64_component(&component), Some(value));
        }
    }

    #[test]
    fn ordered_bytes_components_sort_like_raw_bytes() {
        let values: [&[u8]; 7] = [b"", b"a", b"a\0", b"a\0b", b"a\x01", b"aa", b"b"];
        let mut encoded = values
            .iter()
            .map(|value| (encode_ordered_bytes_component(value), *value))
            .collect::<Vec<_>>();
        encoded.sort_by(|left, right| left.0.cmp(&right.0));

        let sorted_values = encoded.iter().map(|(_, value)| *value).collect::<Vec<_>>();
        assert_eq!(sorted_values, values);

        for (component, value) in encoded {
            assert_eq!(
                decode_ordered_bytes_component(&component),
                Some(value.to_vec())
            );
        }
    }

    #[test]
    fn ordered_bytes_decoder_rejects_malformed_escape_sequences() {
        assert!(decode_ordered_bytes_component(&[ORDERED_VALUE_TYPE_BYTES]).is_none());
        assert!(decode_ordered_bytes_component(&[ORDERED_VALUE_TYPE_BYTES, 0]).is_none());
        assert!(decode_ordered_bytes_component(&[ORDERED_VALUE_TYPE_BYTES, 0, 1]).is_none());
        assert!(
            decode_ordered_bytes_component(&[ORDERED_VALUE_TYPE_BYTES, b'a', 0, 0, b'b']).is_none()
        );
    }
}
