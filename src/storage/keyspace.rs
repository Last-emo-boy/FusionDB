use std::fmt;

const STRUCTURED_KEY_MAGIC: &[u8] = b"\0FDBK";
const STRUCTURED_KEY_VERSION: u8 = 2;
const DATA_ROUTE_UNSHARDED: u8 = 0;
const DATA_ROUTE_SHARD: u8 = 1;
const ORDERED_VALUE_TYPE_INTEGER: u8 = 0x11;
const ORDERED_VALUE_TYPE_BYTES: u8 = 0x12;
pub(crate) const MAX_IDENTIFIER_COMPONENT_BYTES: usize = 64 * 1024;
const MAX_IDENTIFIER_COMPONENTS: usize = 32;
pub(crate) const MAX_DATA_ROW_ID_BYTES: usize = 256 * 1024;
const MAX_STRUCTURED_KEY_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum KeyNamespace {
    Data = 1,
    SecondaryIndex = 2,
    CompositeIndex = 3,
    FullText = 4,
    CountSummary = 5,
    Catalog = 6,
    VectorIndex = 7,
}

impl KeyNamespace {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Data),
            2 => Some(Self::SecondaryIndex),
            3 => Some(Self::CompositeIndex),
            4 => Some(Self::FullText),
            5 => Some(Self::CountSummary),
            6 => Some(Self::Catalog),
            7 => Some(Self::VectorIndex),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DataRoute {
    Unsharded,
    Shard(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum KeyCodecError {
    InvalidMagic,
    UnsupportedVersion(u8),
    UnknownNamespace(u8),
    UnexpectedNamespace(KeyNamespace),
    UnknownDataRoute(u8),
    Truncated(&'static str),
    EmptyTable,
    IdentifierComponentTooLarge {
        len: usize,
        max: usize,
    },
    TooManyIdentifierComponents {
        count: usize,
        max: usize,
    },
    DataRowIdTooLarge {
        len: usize,
        max: usize,
    },
    KeyTooLarge {
        len: usize,
        max: usize,
    },
    DataRequiresSpecializedCodec,
    OrderedComponentRequiresSpecializedCodec,
    UnknownOrderedValueType(u8),
    MalformedOrderedBytes,
    TrailingBytes,
    #[cfg(test)]
    WrongIdentifierShape,
}

impl fmt::Display for KeyCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => write!(formatter, "invalid structured key magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported structured key version {version}")
            }
            Self::UnknownNamespace(namespace) => {
                write!(formatter, "unknown structured key namespace {namespace}")
            }
            Self::UnexpectedNamespace(namespace) => {
                write!(
                    formatter,
                    "unexpected structured key namespace {namespace:?}"
                )
            }
            Self::UnknownDataRoute(route) => write!(formatter, "unknown data route {route}"),
            Self::Truncated(field) => write!(formatter, "truncated structured key {field}"),
            Self::EmptyTable => write!(formatter, "structured data key table is empty"),
            Self::IdentifierComponentTooLarge { len, max } => write!(
                formatter,
                "structured key identifier component is too large: {len} > {max}"
            ),
            Self::TooManyIdentifierComponents { count, max } => write!(
                formatter,
                "structured key has too many identifier components: {count} > {max}"
            ),
            Self::DataRowIdTooLarge { len, max } => {
                write!(
                    formatter,
                    "structured data row id is too large: {len} > {max}"
                )
            }
            Self::KeyTooLarge { len, max } => {
                write!(formatter, "structured key is too large: {len} > {max}")
            }
            Self::DataRequiresSpecializedCodec => {
                write!(formatter, "data keys require the specialized data codec")
            }
            Self::OrderedComponentRequiresSpecializedCodec => write!(
                formatter,
                "ordered components require a namespace-specific codec"
            ),
            Self::UnknownOrderedValueType(value_type) => {
                write!(formatter, "unknown ordered value type {value_type}")
            }
            Self::MalformedOrderedBytes => write!(formatter, "malformed ordered byte component"),
            Self::TrailingBytes => write!(formatter, "structured key has trailing bytes"),
            #[cfg(test)]
            Self::WrongIdentifierShape => write!(formatter, "unexpected identifier key shape"),
        }
    }
}

impl std::error::Error for KeyCodecError {}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg(test)]
pub(crate) struct StructuredIdentifierKey<'a> {
    namespace: KeyNamespace,
    components: Vec<&'a [u8]>,
}

#[cfg(test)]
impl<'a> StructuredIdentifierKey<'a> {
    pub(crate) fn namespace(&self) -> KeyNamespace {
        self.namespace
    }

    pub(crate) fn components(&self) -> &[&'a [u8]] {
        &self.components
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParsedDataKey<'a> {
    route: DataRoute,
    table: &'a [u8],
    row_id: Vec<u8>,
}

impl<'a> ParsedDataKey<'a> {
    pub(crate) fn route(&self) -> DataRoute {
        self.route
    }

    pub(crate) fn table(&self) -> &'a [u8] {
        self.table
    }

    #[cfg(test)]
    pub(crate) fn row_id(&self) -> &[u8] {
        &self.row_id
    }
}

pub(crate) fn data_namespace_prefix() -> Vec<u8> {
    encoded_header(KeyNamespace::Data, STRUCTURED_KEY_MAGIC.len() + 2)
}

fn encoded_header(namespace: KeyNamespace, capacity: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(capacity);
    key.extend_from_slice(STRUCTURED_KEY_MAGIC);
    key.push(STRUCTURED_KEY_VERSION);
    key.push(namespace as u8);
    key
}

fn parse_header(input: &[u8]) -> Result<(KeyNamespace, &[u8]), KeyCodecError> {
    if input.len() > MAX_STRUCTURED_KEY_BYTES {
        return Err(KeyCodecError::KeyTooLarge {
            len: input.len(),
            max: MAX_STRUCTURED_KEY_BYTES,
        });
    }
    let remaining = input
        .strip_prefix(STRUCTURED_KEY_MAGIC)
        .ok_or(KeyCodecError::InvalidMagic)?;
    let (&version, remaining) = remaining
        .split_first()
        .ok_or(KeyCodecError::Truncated("version"))?;
    if version != STRUCTURED_KEY_VERSION {
        return Err(KeyCodecError::UnsupportedVersion(version));
    }
    let (&namespace, remaining) = remaining
        .split_first()
        .ok_or(KeyCodecError::Truncated("namespace"))?;
    let namespace =
        KeyNamespace::from_byte(namespace).ok_or(KeyCodecError::UnknownNamespace(namespace))?;
    Ok((namespace, remaining))
}

fn validate_identifier(component: &[u8]) -> Result<u32, KeyCodecError> {
    if component.len() > MAX_IDENTIFIER_COMPONENT_BYTES {
        return Err(KeyCodecError::IdentifierComponentTooLarge {
            len: component.len(),
            max: MAX_IDENTIFIER_COMPONENT_BYTES,
        });
    }
    u32::try_from(component.len()).map_err(|_| KeyCodecError::IdentifierComponentTooLarge {
        len: component.len(),
        max: MAX_IDENTIFIER_COMPONENT_BYTES,
    })
}

fn validate_generic_identifier(component: &[u8]) -> Result<(), KeyCodecError> {
    validate_identifier(component)?;
    if decode_ordered_i64_component(component).is_some()
        || decode_ordered_bytes_component(component).is_some()
    {
        return Err(KeyCodecError::OrderedComponentRequiresSpecializedCodec);
    }
    Ok(())
}

fn append_identifier(key: &mut Vec<u8>, component: &[u8]) -> Result<(), KeyCodecError> {
    let len = validate_identifier(component)?;
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(component);
    Ok(())
}

/// Encodes identifier-only tuple prefixes. Ordered values must use a namespace-specific codec.
pub(crate) fn encode_identifier_key(
    namespace: KeyNamespace,
    components: &[&[u8]],
) -> Result<Vec<u8>, KeyCodecError> {
    if namespace == KeyNamespace::Data {
        return Err(KeyCodecError::DataRequiresSpecializedCodec);
    }
    if components.len() > MAX_IDENTIFIER_COMPONENTS {
        return Err(KeyCodecError::TooManyIdentifierComponents {
            count: components.len(),
            max: MAX_IDENTIFIER_COMPONENTS,
        });
    }

    let mut capacity = STRUCTURED_KEY_MAGIC.len() + 2;
    for component in components {
        validate_generic_identifier(component)?;
        capacity = capacity
            .checked_add(4)
            .and_then(|capacity| capacity.checked_add(component.len()))
            .ok_or(KeyCodecError::KeyTooLarge {
                len: usize::MAX,
                max: MAX_STRUCTURED_KEY_BYTES,
            })?;
    }
    if capacity > MAX_STRUCTURED_KEY_BYTES {
        return Err(KeyCodecError::KeyTooLarge {
            len: capacity,
            max: MAX_STRUCTURED_KEY_BYTES,
        });
    }

    let mut key = encoded_header(namespace, capacity);
    for component in components {
        append_identifier(&mut key, component)?;
    }
    Ok(key)
}

#[cfg(test)]
pub(crate) fn encode_identifier_prefix(
    namespace: KeyNamespace,
    components: &[&[u8]],
) -> Result<Vec<u8>, KeyCodecError> {
    encode_identifier_key(namespace, components)
}

#[cfg(test)]
pub(crate) fn parse_identifier_key(
    input: &[u8],
) -> Result<StructuredIdentifierKey<'_>, KeyCodecError> {
    let (namespace, mut remaining) = parse_header(input)?;
    if namespace == KeyNamespace::Data {
        return Err(KeyCodecError::DataRequiresSpecializedCodec);
    }

    let mut components = Vec::new();
    while !remaining.is_empty() {
        if components.len() == MAX_IDENTIFIER_COMPONENTS {
            return Err(KeyCodecError::TooManyIdentifierComponents {
                count: components.len() + 1,
                max: MAX_IDENTIFIER_COMPONENTS,
            });
        }
        let length_bytes = remaining
            .get(..4)
            .ok_or(KeyCodecError::Truncated("identifier length"))?;
        let component_len =
            u32::from_be_bytes(length_bytes.try_into().expect("four bytes")) as usize;
        if component_len > MAX_IDENTIFIER_COMPONENT_BYTES {
            return Err(KeyCodecError::IdentifierComponentTooLarge {
                len: component_len,
                max: MAX_IDENTIFIER_COMPONENT_BYTES,
            });
        }
        remaining = &remaining[4..];
        let component = remaining
            .get(..component_len)
            .ok_or(KeyCodecError::Truncated("identifier"))?;
        validate_generic_identifier(component)?;
        components.push(component);
        remaining = &remaining[component_len..];
    }

    Ok(StructuredIdentifierKey {
        namespace,
        components,
    })
}

#[cfg(test)]
pub(crate) fn parse_identifier_key_exact(
    input: &[u8],
    namespace: KeyNamespace,
    component_count: usize,
) -> Result<StructuredIdentifierKey<'_>, KeyCodecError> {
    let key = parse_identifier_key(input)?;
    if key.namespace != namespace || key.components.len() != component_count {
        return Err(KeyCodecError::WrongIdentifierShape);
    }
    Ok(key)
}

fn data_prefix_capacity(route: DataRoute, table_len: usize) -> Result<usize, KeyCodecError> {
    let route_len = match route {
        DataRoute::Unsharded => 1,
        DataRoute::Shard(_) => 1 + 8,
    };
    let capacity = STRUCTURED_KEY_MAGIC
        .len()
        .checked_add(2 + route_len + 4)
        .and_then(|capacity| capacity.checked_add(table_len))
        .ok_or(KeyCodecError::KeyTooLarge {
            len: usize::MAX,
            max: MAX_STRUCTURED_KEY_BYTES,
        })?;
    if capacity > MAX_STRUCTURED_KEY_BYTES {
        return Err(KeyCodecError::KeyTooLarge {
            len: capacity,
            max: MAX_STRUCTURED_KEY_BYTES,
        });
    }
    Ok(capacity)
}

/// The prefix shared by every Data key of one route, table-independent.
///
/// Used to seek past a whole route region when enumerating which routes are
/// physically present, so a per-table cleanup costs O(routes) seeks instead
/// of a scan over the entire Data namespace.
pub(crate) fn encode_data_route_prefix(route: DataRoute) -> Vec<u8> {
    let capacity = STRUCTURED_KEY_MAGIC.len() + 2 + 1 + 8;
    let mut key = encoded_header(KeyNamespace::Data, capacity);
    match route {
        DataRoute::Unsharded => key.push(DATA_ROUTE_UNSHARDED),
        DataRoute::Shard(shard_id) => {
            key.push(DATA_ROUTE_SHARD);
            key.extend_from_slice(&shard_id.to_be_bytes());
        }
    }
    key
}

pub(crate) fn encode_data_prefix(route: DataRoute, table: &[u8]) -> Result<Vec<u8>, KeyCodecError> {
    if table.is_empty() {
        return Err(KeyCodecError::EmptyTable);
    }
    validate_identifier(table)?;
    let capacity = data_prefix_capacity(route, table.len())?;
    let mut key = encoded_header(KeyNamespace::Data, capacity);
    match route {
        DataRoute::Unsharded => key.push(DATA_ROUTE_UNSHARDED),
        DataRoute::Shard(shard_id) => {
            key.push(DATA_ROUTE_SHARD);
            key.extend_from_slice(&shard_id.to_be_bytes());
        }
    }
    append_identifier(&mut key, table)?;
    Ok(key)
}

fn ordered_bytes_encoded_len(bytes: &[u8]) -> Result<usize, KeyCodecError> {
    bytes
        .len()
        .checked_add(bytes.iter().filter(|&&byte| byte == 0).count())
        .and_then(|len| len.checked_add(3))
        .ok_or(KeyCodecError::KeyTooLarge {
            len: usize::MAX,
            max: MAX_STRUCTURED_KEY_BYTES,
        })
}

pub(crate) fn encode_data_key(
    route: DataRoute,
    table: &[u8],
    row_id: &[u8],
) -> Result<Vec<u8>, KeyCodecError> {
    if row_id.len() > MAX_DATA_ROW_ID_BYTES {
        return Err(KeyCodecError::DataRowIdTooLarge {
            len: row_id.len(),
            max: MAX_DATA_ROW_ID_BYTES,
        });
    }
    let mut key = encode_data_prefix(route, table)?;
    let component_len = ordered_bytes_encoded_len(row_id)?;
    let encoded_len = key
        .len()
        .checked_add(component_len)
        .ok_or(KeyCodecError::KeyTooLarge {
            len: usize::MAX,
            max: MAX_STRUCTURED_KEY_BYTES,
        })?;
    if encoded_len > MAX_STRUCTURED_KEY_BYTES {
        return Err(KeyCodecError::KeyTooLarge {
            len: encoded_len,
            max: MAX_STRUCTURED_KEY_BYTES,
        });
    }
    key.reserve(component_len);
    append_ordered_bytes_component(&mut key, row_id);
    Ok(key)
}

pub(crate) fn parse_data_key_exact(input: &[u8]) -> Result<ParsedDataKey<'_>, KeyCodecError> {
    let (namespace, mut remaining) = parse_header(input)?;
    if namespace != KeyNamespace::Data {
        return Err(KeyCodecError::UnexpectedNamespace(namespace));
    }

    let (&route_tag, route_remaining) = remaining
        .split_first()
        .ok_or(KeyCodecError::Truncated("data route"))?;
    remaining = route_remaining;
    let route = match route_tag {
        DATA_ROUTE_UNSHARDED => DataRoute::Unsharded,
        DATA_ROUTE_SHARD => {
            let shard_bytes = remaining
                .get(..8)
                .ok_or(KeyCodecError::Truncated("shard id"))?;
            remaining = &remaining[8..];
            DataRoute::Shard(u64::from_be_bytes(
                shard_bytes.try_into().expect("eight bytes"),
            ))
        }
        route => return Err(KeyCodecError::UnknownDataRoute(route)),
    };

    let table_len_bytes = remaining
        .get(..4)
        .ok_or(KeyCodecError::Truncated("table length"))?;
    let table_len = u32::from_be_bytes(table_len_bytes.try_into().expect("four bytes")) as usize;
    if table_len == 0 {
        return Err(KeyCodecError::EmptyTable);
    }
    if table_len > MAX_IDENTIFIER_COMPONENT_BYTES {
        return Err(KeyCodecError::IdentifierComponentTooLarge {
            len: table_len,
            max: MAX_IDENTIFIER_COMPONENT_BYTES,
        });
    }
    remaining = &remaining[4..];
    let table = remaining
        .get(..table_len)
        .ok_or(KeyCodecError::Truncated("table"))?;
    remaining = &remaining[table_len..];

    let (row_id, consumed) = decode_ordered_bytes_prefix(remaining)?;
    if row_id.len() > MAX_DATA_ROW_ID_BYTES {
        return Err(KeyCodecError::DataRowIdTooLarge {
            len: row_id.len(),
            max: MAX_DATA_ROW_ID_BYTES,
        });
    }
    if consumed != remaining.len() {
        return Err(KeyCodecError::TrailingBytes);
    }

    Ok(ParsedDataKey {
        route,
        table,
        row_id,
    })
}

/// The exclusive upper bound of the key range covered by `prefix`.
///
/// `None` means the prefix is all-`0xff` and therefore has no finite bound;
/// callers scan to the end of the keyspace instead.
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

#[cfg(test)]
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

fn append_ordered_bytes_component(out: &mut Vec<u8>, bytes: &[u8]) {
    out.push(ORDERED_VALUE_TYPE_BYTES);
    for &byte in bytes {
        if byte == 0 {
            out.push(0);
            out.push(0xff);
        } else {
            out.push(byte);
        }
    }
    out.push(0);
    out.push(0);
}

#[cfg(test)]
pub(crate) fn encode_ordered_bytes_component(bytes: &[u8]) -> Vec<u8> {
    let mut component = Vec::with_capacity(ordered_bytes_encoded_len(bytes).unwrap_or(0));
    append_ordered_bytes_component(&mut component, bytes);
    component
}

fn decode_ordered_bytes_prefix(input: &[u8]) -> Result<(Vec<u8>, usize), KeyCodecError> {
    let (&value_type, payload) = input
        .split_first()
        .ok_or(KeyCodecError::Truncated("ordered byte type"))?;
    if value_type != ORDERED_VALUE_TYPE_BYTES {
        return Err(KeyCodecError::UnknownOrderedValueType(value_type));
    }

    let mut decoded = Vec::with_capacity(payload.len().min(MAX_DATA_ROW_ID_BYTES));
    let mut idx = 0;
    while idx < payload.len() {
        let byte = payload[idx];
        if byte != 0 {
            if decoded.len() == MAX_DATA_ROW_ID_BYTES {
                return Err(KeyCodecError::DataRowIdTooLarge {
                    len: decoded.len() + 1,
                    max: MAX_DATA_ROW_ID_BYTES,
                });
            }
            decoded.push(byte);
            idx += 1;
            continue;
        }

        let marker = *payload
            .get(idx + 1)
            .ok_or(KeyCodecError::MalformedOrderedBytes)?;
        match marker {
            0 => return Ok((decoded, idx + 3)),
            0xff => {
                if decoded.len() == MAX_DATA_ROW_ID_BYTES {
                    return Err(KeyCodecError::DataRowIdTooLarge {
                        len: decoded.len() + 1,
                        max: MAX_DATA_ROW_ID_BYTES,
                    });
                }
                decoded.push(0);
                idx += 2;
            }
            _ => return Err(KeyCodecError::MalformedOrderedBytes),
        }
    }
    Err(KeyCodecError::MalformedOrderedBytes)
}

pub(crate) fn decode_ordered_bytes_component(component: &[u8]) -> Option<Vec<u8>> {
    let (decoded, consumed) = decode_ordered_bytes_prefix(component).ok()?;
    (consumed == component.len()).then_some(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_codec_roundtrips_binary_components() {
        let table = b"tenant:archive,orders|2026";
        let column = b"quoted:col,with|delims\0\xff";
        let key = encode_identifier_prefix(KeyNamespace::SecondaryIndex, &[table, column])
            .expect("identifier key should encode");
        let parsed = parse_identifier_key_exact(&key, KeyNamespace::SecondaryIndex, 2)
            .expect("identifier key should parse");

        assert_eq!(parsed.namespace(), KeyNamespace::SecondaryIndex);
        assert_eq!(parsed.components(), &[&table[..], &column[..]]);
        assert!(matches!(
            encode_identifier_prefix(KeyNamespace::Data, &[table]),
            Err(KeyCodecError::DataRequiresSpecializedCodec)
        ));

        let ordered = encode_ordered_bytes_component(b"open");
        assert!(matches!(
            encode_identifier_key(KeyNamespace::SecondaryIndex, &[&ordered]),
            Err(KeyCodecError::OrderedComponentRequiresSpecializedCodec)
        ));
    }

    #[test]
    fn data_key_roundtrips_routes_delimiters_and_binary_bytes() {
        let table = b"tenant:archive,orders|2026\0\xff";
        let row_id = b"row\0id:,|\xff";

        for route in [
            DataRoute::Unsharded,
            DataRoute::Shard(0),
            DataRoute::Shard(u64::MAX),
        ] {
            let key = encode_data_key(route, table, row_id).expect("data key should encode");
            let parsed = parse_data_key_exact(&key).expect("data key should parse");
            assert_eq!(parsed.route(), route);
            assert_eq!(parsed.table(), table);
            assert_eq!(parsed.row_id(), row_id);
        }
    }

    #[test]
    fn complete_data_keys_preserve_variable_length_row_id_order() {
        let values: [&[u8]; 9] = [
            b"", b"a", b"a\0", b"a\0b", b"a\x01", b"aa", b"b", b"b\0", b"ba",
        ];
        let mut encoded = values
            .iter()
            .map(|row_id| {
                (
                    encode_data_key(DataRoute::Unsharded, b"orders", row_id)
                        .expect("data key should encode"),
                    *row_id,
                )
            })
            .collect::<Vec<_>>();
        encoded.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(
            encoded
                .iter()
                .map(|(_, row_id)| *row_id)
                .collect::<Vec<_>>(),
            values
        );
    }

    #[test]
    fn data_prefixes_are_isolated_by_route_and_exact_table() {
        let tenant = encode_data_prefix(DataRoute::Unsharded, b"tenant").unwrap();
        let tenant_archive = encode_data_prefix(DataRoute::Unsharded, b"tenant:archive").unwrap();
        let shard_one = encode_data_prefix(DataRoute::Shard(1), b"tenant").unwrap();
        let shard_two = encode_data_prefix(DataRoute::Shard(2), b"tenant").unwrap();

        let tenant_key = encode_data_key(DataRoute::Unsharded, b"tenant", b"one").unwrap();
        let archive_key = encode_data_key(DataRoute::Unsharded, b"tenant:archive", b"one").unwrap();
        let shard_one_key = encode_data_key(DataRoute::Shard(1), b"tenant", b"one").unwrap();

        assert!(tenant_key.starts_with(&tenant));
        assert!(!archive_key.starts_with(&tenant));
        assert!(archive_key.starts_with(&tenant_archive));
        assert!(shard_one_key.starts_with(&shard_one));
        assert!(!shard_one_key.starts_with(&shard_two));
        assert!(!shard_one_key.starts_with(&tenant));

        let tenant_end = prefix_end(&tenant).expect("prefix should have an end");
        assert!(tenant_key >= tenant && tenant_key < tenant_end);
        assert!(archive_key >= tenant_end);
    }

    #[test]
    fn data_parser_rejects_unknown_header_route_and_namespace() {
        let key = encode_data_key(DataRoute::Shard(7), b"orders", b"row").unwrap();
        let version_offset = STRUCTURED_KEY_MAGIC.len();
        let namespace_offset = version_offset + 1;
        let route_offset = namespace_offset + 1;

        let mut unknown_version = key.clone();
        unknown_version[version_offset] = STRUCTURED_KEY_VERSION + 1;
        assert!(matches!(
            parse_data_key_exact(&unknown_version),
            Err(KeyCodecError::UnsupportedVersion(_))
        ));

        let mut unknown_namespace = key.clone();
        unknown_namespace[namespace_offset] = 0xff;
        assert!(matches!(
            parse_data_key_exact(&unknown_namespace),
            Err(KeyCodecError::UnknownNamespace(0xff))
        ));

        let mut wrong_namespace = key.clone();
        wrong_namespace[namespace_offset] = KeyNamespace::FullText as u8;
        assert!(matches!(
            parse_data_key_exact(&wrong_namespace),
            Err(KeyCodecError::UnexpectedNamespace(KeyNamespace::FullText))
        ));

        let mut unknown_route = key;
        unknown_route[route_offset] = 0xff;
        assert!(matches!(
            parse_data_key_exact(&unknown_route),
            Err(KeyCodecError::UnknownDataRoute(0xff))
        ));
    }

    #[test]
    fn data_parser_rejects_every_truncation_and_trailing_bytes() {
        let key = encode_data_key(DataRoute::Shard(42), b"orders", b"a\0b").unwrap();
        for end in 0..key.len() {
            assert!(
                parse_data_key_exact(&key[..end]).is_err(),
                "truncation at {end} parsed successfully"
            );
        }

        let mut trailing = key;
        trailing.push(0x7f);
        assert!(matches!(
            parse_data_key_exact(&trailing),
            Err(KeyCodecError::TrailingBytes)
        ));
    }

    #[test]
    fn data_parser_rejects_malformed_ordered_bytes() {
        let prefix = encode_data_prefix(DataRoute::Unsharded, b"orders").unwrap();

        for malformed in [
            vec![],
            vec![ORDERED_VALUE_TYPE_INTEGER],
            vec![ORDERED_VALUE_TYPE_BYTES],
            vec![ORDERED_VALUE_TYPE_BYTES, 0],
            vec![ORDERED_VALUE_TYPE_BYTES, 0, 1],
            vec![ORDERED_VALUE_TYPE_BYTES, b'a', 0, 0, b'b'],
        ] {
            let mut key = prefix.clone();
            key.extend_from_slice(&malformed);
            assert!(parse_data_key_exact(&key).is_err());
        }
    }

    #[test]
    fn codec_rejects_empty_and_oversized_fields() {
        assert!(matches!(
            encode_data_prefix(DataRoute::Unsharded, b""),
            Err(KeyCodecError::EmptyTable)
        ));

        let oversized_table = vec![b't'; MAX_IDENTIFIER_COMPONENT_BYTES + 1];
        assert!(matches!(
            encode_data_prefix(DataRoute::Unsharded, &oversized_table),
            Err(KeyCodecError::IdentifierComponentTooLarge { .. })
        ));

        let oversized_row_id = vec![b'r'; MAX_DATA_ROW_ID_BYTES + 1];
        assert!(matches!(
            encode_data_key(DataRoute::Unsharded, b"orders", &oversized_row_id),
            Err(KeyCodecError::DataRowIdTooLarge { .. })
        ));

        let oversized_key = vec![0; MAX_STRUCTURED_KEY_BYTES + 1];
        assert!(matches!(
            parse_data_key_exact(&oversized_key),
            Err(KeyCodecError::KeyTooLarge { .. })
        ));
    }

    #[test]
    fn arbitrary_malformed_inputs_never_panic() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for len in 0..512 {
            let mut input = vec![0; len];
            for byte in &mut input {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state as u8;
            }
            let _ = parse_data_key_exact(&input);
            let _ = parse_identifier_key(&input);
        }
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
        encoded.sort_by_key(|left| left.0);

        assert_eq!(
            encoded.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
            vec![i64::MIN, -1, 0, 1, i64::MAX]
        );
        for (component, value) in encoded {
            assert_eq!(decode_ordered_i64_component(&component), Some(value));
        }
    }

    #[test]
    fn ordered_bytes_component_roundtrips_and_rejects_trailing_data() {
        let value = b"a\0b\xff";
        let component = encode_ordered_bytes_component(value);
        assert_eq!(
            decode_ordered_bytes_component(&component),
            Some(value.to_vec())
        );

        let mut trailing = component;
        trailing.push(b'x');
        assert!(decode_ordered_bytes_component(&trailing).is_none());
    }
}
