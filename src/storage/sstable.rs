use crate::catalog::TableSchema;
use crate::common::{Result, Value};
use crate::monitor;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use moka::sync::Cache;
use serde::de::{MapAccess, Visitor};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Instant, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

use crc32fast::Hasher as Crc32Hasher;
use fastbloom::BloomFilter;

// --- SSTable ---
// Format:
// [Data Block 1] [Data Block 2] ... [Index Block] [Filter Block] [Meta Block] [Footer]
// Data Block: legacy [Count: 4b] [Entry 1] ... [CRC32: 4b],
// or compressed [Magic: 4b] [Count: 4b] [LZ4 payload] [CRC32: 4b]
// Entry: [KeyLen: 4b] [Key] [ValLen: 4b] [Val]
// Index Block: [Entry 1] ... (Key -> Offset)
// Filter Block: [FilterBytes]
// Footer: [IndexOffset: 8b] [FilterOffset: 8b] [MetaOffset: 8b] [Magic: 4b]

const SST_MAGIC: u32 = 0xCAFEBABE;
const COMPRESSED_BLOCK_MAGIC: &[u8; 4] = b"FDBL";
const COMPRESSED_BLOCK_HEADER_LEN: usize = 8;
const SSTABLE_INDEX_MAGIC: &[u8; 4] = b"FIDX";
const SSTABLE_INDEX_VERSION_FLAT_VEC: u32 = 1;
const SSTABLE_INDEX_CACHE_MAGIC: &[u8; 4] = b"FICX";
const SSTABLE_INDEX_CACHE_VERSION: u32 = 3;
const SSTABLE_REVERSE_SEEK_MAGIC: &[u8; 4] = b"FRSK";
const SSTABLE_REVERSE_SEEK_VERSION: u32 = 1;
const SSTABLE_META_MAGIC: &[u8; 4] = b"FSMT";
const SSTABLE_META_HEADER_LEN: usize = 8;
const SSTABLE_META_VERSION_BLOCK_PROPERTIES: u32 = 2;
const SSTABLE_META_VERSION_BLOCK_TABLE_PREFIXES: u32 = 3;
const SSTABLE_META_VERSION_BLOCK_SQL_INDEX_PREFIXES: u32 = 4;
const SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS_V5: u32 = 5;
const SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS: u32 = 6;
const SSTABLE_FILTER_VERSION_PREFIX: u32 = 2;
const SSTABLE_FILTER_VERSION_USER_KEY: u32 = 3;
const SSTABLE_FILTER_VERSION_SQL_INDEX_PREFIX: u32 = 4;
const SSTABLE_PREFIX_EXTRACTOR_TABLE_USER_KEY: u32 = 1;
const SSTABLE_USER_KEY_EXTRACTOR_MVCC_USER_KEY: u32 = 1;
const SSTABLE_SQL_INDEX_PREFIX_EXTRACTOR: u32 = 1;
const SSTABLE_SQL_ZONE_MAP_VALUE_ENCODING_VERSION: u8 =
    super::SQL_BLOCK_ZONE_MAP_VALUE_ENCODING_VERSION;

pub type BlockCacheKey = (u64, u64);
pub type BlockCacheValue = Arc<[u8]>;
pub type BlockCache = Cache<BlockCacheKey, BlockCacheValue>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SsTablePrefixFilterProbe {
    MayMatch,
    NoMatch,
    FailOpen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SsTableReverseFrontierKind {
    BlockProperty,
    FileFallback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsTableReverseFrontier {
    pub user_key: Vec<u8>,
    pub kind: SsTableReverseFrontierKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SsTableReadOptions {
    pub fill_cache: bool,
}

impl SsTableReadOptions {
    pub const fn fill_cache() -> Self {
        Self { fill_cache: true }
    }

    pub const fn no_fill_cache() -> Self {
        Self { fill_cache: false }
    }
}

impl Default for SsTableReadOptions {
    fn default() -> Self {
        Self::fill_cache()
    }
}

fn block_entry_buffer() -> VecDeque<(Vec<u8>, Vec<u8>)> {
    VecDeque::with_capacity(1)
}

fn block_entry_reserve_count(count: u32, block_len: usize) -> usize {
    (count as usize).min(block_len / 8)
}

#[derive(Debug, Clone, Copy)]
struct BlockEntrySpan {
    key_start: usize,
    key_end: usize,
    value_start: usize,
    value_end: usize,
}

#[derive(Debug, Default, Clone, Copy)]
struct ReverseBlockScanStats {
    decoded_entries: u64,
    yielded_entries: u64,
    span_scan_blocks: u64,
    span_scan_entries: u64,
    span_materialized_entries: u64,
    sidecar_index_entries: u64,
    sidecar_materialized_entries: u64,
    sidecar_offset_probes: u64,
}

fn key_user_part(key: &[u8], suffix_len: usize) -> &[u8] {
    key.len()
        .checked_sub(suffix_len)
        .map(|len| &key[..len])
        .unwrap_or(key)
}

fn legacy_block_len(entry_bytes_len: usize) -> usize {
    4 + entry_bytes_len
}

fn compressed_block_payload_capacity(compressed_len: usize) -> usize {
    COMPRESSED_BLOCK_HEADER_LEN + compressed_len
}

fn find_byte_from(bytes: &[u8], start: usize, needle: u8) -> Option<usize> {
    bytes
        .get(start..)?
        .iter()
        .position(|byte| *byte == needle)
        .map(|pos| start + pos)
}

fn namespace_table_prefix<'a>(key: &'a [u8], namespace: &[u8]) -> Option<&'a [u8]> {
    if !key.starts_with(namespace) {
        return None;
    }
    let table_end = find_byte_from(key, namespace.len(), b':')?;
    Some(&key[..=table_end])
}

fn table_user_key_prefix(key: &[u8]) -> Option<&[u8]> {
    if let Some(prefix) = namespace_table_prefix(key, b"data:") {
        return Some(prefix);
    }
    if let Some(prefix) = namespace_table_prefix(key, b"index:") {
        return Some(prefix);
    }
    if let Some(prefix) = namespace_table_prefix(key, b"fts:") {
        return Some(prefix);
    }

    if !key.starts_with(b"shard:") {
        return None;
    }
    let shard_end = find_byte_from(key, b"shard:".len(), b':')?;
    let namespace_start = shard_end + 1;
    if key.get(namespace_start..)?.starts_with(b"data:") {
        let table_end = find_byte_from(key, namespace_start + b"data:".len(), b':')?;
        return Some(&key[..=table_end]);
    }
    if key.get(namespace_start..)?.starts_with(b"index:") {
        let table_end = find_byte_from(key, namespace_start + b"index:".len(), b':')?;
        return Some(&key[..=table_end]);
    }
    if key.get(namespace_start..)?.starts_with(b"fts:") {
        let table_end = find_byte_from(key, namespace_start + b"fts:".len(), b':')?;
        return Some(&key[..=table_end]);
    }
    None
}

fn sql_index_namespace_start(key: &[u8]) -> Option<usize> {
    if key.starts_with(b"index:") {
        return Some(0);
    }

    if !key.starts_with(b"shard:") {
        return None;
    }
    let shard_end = find_byte_from(key, b"shard:".len(), b':')?;
    let namespace_start = shard_end + 1;
    if key.get(namespace_start..)?.starts_with(b"index:") {
        Some(namespace_start)
    } else {
        None
    }
}

fn sql_index_scan_prefix(key: &[u8]) -> Option<&[u8]> {
    let namespace_start = sql_index_namespace_start(key)?;
    let table_start = namespace_start + b"index:".len();
    let table_end = find_byte_from(key, table_start, b':')?;
    let columns_start = table_end + 1;
    let columns_end = find_byte_from(key, columns_start, b':')?;
    let value_start = columns_end + 1;

    if key[columns_start..columns_end].contains(&b',') {
        let component_end = find_byte_from(key, value_start, b'|')?;
        return Some(&key[..=component_end]);
    }

    Some(&key[..value_start])
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != 0xff {
            end[index] += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

fn take_cursor_bytes<'a>(cursor: &mut std::io::Cursor<&'a [u8]>, len: usize) -> Option<&'a [u8]> {
    let start = cursor.position() as usize;
    let end = start.checked_add(len)?;
    let bytes = cursor.get_ref().get(start..end)?;
    cursor.set_position(end as u64);
    Some(bytes)
}

fn read_cursor_u32(cursor: &mut std::io::Cursor<&[u8]>) -> Option<u32> {
    let bytes = take_cursor_bytes(cursor, 4)?;
    Some(u32::from_le_bytes(bytes.try_into().ok()?))
}

fn block_table_prefixes(buf: &[u8], count: u32, suffix_len: Option<usize>) -> Vec<Vec<u8>> {
    let Some(suffix_len) = suffix_len else {
        return Vec::new();
    };

    let mut prefixes = Vec::new();
    let mut cursor = std::io::Cursor::new(buf);
    for _ in 0..count {
        let Some(k_len) = read_cursor_u32(&mut cursor).map(|len| len as usize) else {
            break;
        };
        let Some(key) = take_cursor_bytes(&mut cursor, k_len) else {
            break;
        };
        let Some(v_len) = read_cursor_u32(&mut cursor).map(|len| len as usize) else {
            break;
        };
        if take_cursor_bytes(&mut cursor, v_len).is_none() {
            break;
        }

        if let Some(user_key_len) = key.len().checked_sub(suffix_len) {
            if let Some(prefix) = table_user_key_prefix(&key[..user_key_len]) {
                prefixes.push(prefix.to_vec());
            }
        }
    }

    prefixes.sort();
    prefixes.dedup();
    prefixes
}

fn block_table_prefix_ranges(
    buf: &[u8],
    count: u32,
    suffix_len: Option<usize>,
) -> (bool, Vec<SsTableBlockTablePrefixRange>) {
    let Some(suffix_len) = suffix_len else {
        return (false, Vec::new());
    };

    let mut ranges: BTreeMap<Vec<u8>, (Vec<u8>, Vec<u8>)> = BTreeMap::new();
    let mut cursor = std::io::Cursor::new(buf);
    for _ in 0..count {
        let Some(k_len) = read_cursor_u32(&mut cursor).map(|len| len as usize) else {
            return (false, Vec::new());
        };
        let Some(key) = take_cursor_bytes(&mut cursor, k_len) else {
            return (false, Vec::new());
        };
        let Some(v_len) = read_cursor_u32(&mut cursor).map(|len| len as usize) else {
            return (false, Vec::new());
        };
        if take_cursor_bytes(&mut cursor, v_len).is_none() {
            return (false, Vec::new());
        }

        let Some(user_key_len) = key.len().checked_sub(suffix_len) else {
            return (false, Vec::new());
        };
        let user_key = &key[..user_key_len];
        let Some(prefix) = table_user_key_prefix(user_key) else {
            continue;
        };
        ranges
            .entry(prefix.to_vec())
            .and_modify(|(first, last)| {
                if user_key < first.as_slice() {
                    *first = user_key.to_vec();
                }
                if user_key > last.as_slice() {
                    *last = user_key.to_vec();
                }
            })
            .or_insert_with(|| (user_key.to_vec(), user_key.to_vec()));
    }
    if cursor.position() as usize != buf.len() {
        return (false, Vec::new());
    }

    (
        true,
        ranges
            .into_iter()
            .map(
                |(table_prefix, (first_user_key, last_user_key))| SsTableBlockTablePrefixRange {
                    table_prefix,
                    first_user_key,
                    last_user_key,
                },
            )
            .collect(),
    )
}

fn block_sql_index_prefixes(
    buf: &[u8],
    count: u32,
    suffix_len: Option<usize>,
) -> (bool, Vec<Vec<u8>>) {
    let Some(suffix_len) = suffix_len else {
        return (false, Vec::new());
    };

    let mut prefixes = Vec::new();
    let mut cursor = std::io::Cursor::new(buf);
    for _ in 0..count {
        let Some(k_len) = read_cursor_u32(&mut cursor).map(|len| len as usize) else {
            return (false, Vec::new());
        };
        let Some(key) = take_cursor_bytes(&mut cursor, k_len) else {
            return (false, Vec::new());
        };
        let Some(v_len) = read_cursor_u32(&mut cursor).map(|len| len as usize) else {
            return (false, Vec::new());
        };
        if take_cursor_bytes(&mut cursor, v_len).is_none() {
            return (false, Vec::new());
        }

        let Some(user_key_len) = key.len().checked_sub(suffix_len) else {
            return (false, Vec::new());
        };
        if let Some(prefix) = sql_index_scan_prefix(&key[..user_key_len]) {
            prefixes.push(prefix.to_vec());
        }
    }
    if cursor.position() as usize != buf.len() {
        return (false, Vec::new());
    }

    prefixes.sort();
    prefixes.dedup();
    (true, prefixes)
}

fn data_table_prefix_and_name(key: &[u8]) -> Option<(&[u8], &str)> {
    if key.starts_with(b"data:") {
        let table_start = b"data:".len();
        let table_end = find_byte_from(key, table_start, b':')?;
        let table_name = std::str::from_utf8(&key[table_start..table_end]).ok()?;
        if table_name.is_empty() {
            return None;
        }
        return Some((&key[..=table_end], table_name));
    }

    if !key.starts_with(b"shard:") {
        return None;
    }
    let shard_end = find_byte_from(key, b"shard:".len(), b':')?;
    let namespace_start = shard_end + 1;
    if !key.get(namespace_start..)?.starts_with(b"data:") {
        return None;
    }
    let table_start = namespace_start + b"data:".len();
    let table_end = find_byte_from(key, table_start, b':')?;
    let table_name = std::str::from_utf8(&key[table_start..table_end]).ok()?;
    if table_name.is_empty() {
        return None;
    }
    Some((&key[..=table_end], table_name))
}

fn stable_schema_fingerprint(schema: &TableSchema) -> u64 {
    super::sql_block_zone_map_schema_fingerprint(schema)
}

fn sql_zone_map_type_tag(data_type: &str) -> Option<u8> {
    super::sql_block_zone_map_type_tag(data_type)
}

fn sql_zone_map_scalar(value: &Value, type_tag: u8) -> Option<Option<i64>> {
    super::sql_block_zone_map_scalar(value, type_tag)
}

#[derive(Clone, Debug)]
struct SqlZoneMapAccumulator {
    table_prefix: Vec<u8>,
    schema_fingerprint: u64,
    column_index: u32,
    column_name: String,
    type_tag: u8,
    min_scalar: i64,
    max_scalar: i64,
    row_count: u32,
    null_count: u32,
    non_null_count: u32,
    put_count: u32,
    tombstone_count: u32,
    bounds_valid: bool,
}

impl SqlZoneMapAccumulator {
    fn new(
        table_prefix: Vec<u8>,
        schema_fingerprint: u64,
        column_index: u32,
        column_name: String,
        type_tag: u8,
    ) -> Self {
        Self {
            table_prefix,
            schema_fingerprint,
            column_index,
            column_name,
            type_tag,
            min_scalar: 0,
            max_scalar: 0,
            row_count: 0,
            null_count: 0,
            non_null_count: 0,
            put_count: 0,
            tombstone_count: 0,
            bounds_valid: false,
        }
    }

    fn observe_put(&mut self, value: &Value) -> bool {
        self.row_count = self.row_count.saturating_add(1);
        self.put_count = self.put_count.saturating_add(1);
        match sql_zone_map_scalar(value, self.type_tag) {
            Some(Some(scalar)) => {
                self.non_null_count = self.non_null_count.saturating_add(1);
                if self.bounds_valid {
                    self.min_scalar = self.min_scalar.min(scalar);
                    self.max_scalar = self.max_scalar.max(scalar);
                } else {
                    self.min_scalar = scalar;
                    self.max_scalar = scalar;
                    self.bounds_valid = true;
                }
                true
            }
            Some(None) => {
                self.null_count = self.null_count.saturating_add(1);
                true
            }
            None => false,
        }
    }

    fn observe_tombstone(&mut self) {
        self.row_count = self.row_count.saturating_add(1);
        self.tombstone_count = self.tombstone_count.saturating_add(1);
    }

    fn finish(self) -> SsTableSqlZoneMap {
        SsTableSqlZoneMap {
            table_prefix: self.table_prefix,
            schema_fingerprint: self.schema_fingerprint,
            column_index: self.column_index,
            column_name: self.column_name,
            type_tag: self.type_tag,
            value_encoding_version: SSTABLE_SQL_ZONE_MAP_VALUE_ENCODING_VERSION,
            min_scalar: self.min_scalar,
            max_scalar: self.max_scalar,
            row_count: self.row_count,
            null_count: self.null_count,
            non_null_count: self.non_null_count,
            put_count: self.put_count,
            tombstone_count: self.tombstone_count,
            bounds_valid: self.bounds_valid,
        }
    }
}

fn block_sql_zone_maps(
    buf: &[u8],
    count: u32,
    suffix_len: Option<usize>,
    schemas: &BTreeMap<String, TableSchema>,
) -> (bool, Vec<SsTableSqlZoneMap>) {
    let Some(suffix_len) = suffix_len else {
        {
            return (false, Vec::new());
        }
    };
    if schemas.is_empty() {
        {
            return (false, Vec::new());
        }
    }
    if schemas.keys().any(|table_name| table_name.contains(':')) {
        {
            return (false, Vec::new());
        }
    }

    let mut maps: BTreeMap<(Vec<u8>, u32), SqlZoneMapAccumulator> = BTreeMap::new();
    let mut cursor = std::io::Cursor::new(buf);
    for _ in 0..count {
        let Some(k_len) = read_cursor_u32(&mut cursor).map(|len| len as usize) else {
            {
                return (false, Vec::new());
            }
        };
        let Some(key) = take_cursor_bytes(&mut cursor, k_len) else {
            {
                return (false, Vec::new());
            }
        };
        let Some(v_len) = read_cursor_u32(&mut cursor).map(|len| len as usize) else {
            {
                return (false, Vec::new());
            }
        };
        let Some(value) = take_cursor_bytes(&mut cursor, v_len) else {
            {
                return (false, Vec::new());
            }
        };

        let Some(user_key_len) = key.len().checked_sub(suffix_len) else {
            {
                return (false, Vec::new());
            }
        };
        let user_key = &key[..user_key_len];
        let Some((table_prefix, table_name)) = data_table_prefix_and_name(user_key) else {
            continue;
        };
        let Some(schema) = schemas.get(table_name) else {
            {
                return (false, Vec::new());
            }
        };
        let Some((&flag, row_payload)) = value.split_first() else {
            {
                return (false, Vec::new());
            }
        };
        let is_put = match flag {
            0 => false,
            1 => true,
            _ => return (false, Vec::new()),
        };
        let schema_fingerprint = stable_schema_fingerprint(schema);

        for (column_index, column) in schema.columns.iter().enumerate() {
            let Some(type_tag) = sql_zone_map_type_tag(&column.data_type) else {
                continue;
            };
            let key = (table_prefix.to_vec(), column_index as u32);
            let accumulator = maps.entry(key).or_insert_with(|| {
                SqlZoneMapAccumulator::new(
                    table_prefix.to_vec(),
                    schema_fingerprint,
                    column_index as u32,
                    column.name.clone(),
                    type_tag,
                )
            });
            if is_put {
                let value = match crate::common::encoding::RowDecoder::decode_column(
                    row_payload,
                    column_index,
                ) {
                    Ok(Some(value)) => value,
                    Ok(None) | Err(_) => return (false, Vec::new()),
                };
                if !accumulator.observe_put(&value) {
                    {
                        return (false, Vec::new());
                    }
                }
            } else {
                accumulator.observe_tombstone();
            }
        }
    }
    if cursor.position() as usize != buf.len() {
        {
            return (false, Vec::new());
        }
    }

    (
        true,
        maps.into_values()
            .map(SqlZoneMapAccumulator::finish)
            .collect(),
    )
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SsTableMeta {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub block_properties: Vec<SsTableBlockProperties>,
    pub format_version: u32,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct SsTableBlockProperties {
    pub offset: u64,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub entry_count: u32,
    pub table_prefixes: Vec<Vec<u8>>,
    pub table_prefix_ranges_complete: bool,
    pub table_prefix_ranges: Vec<SsTableBlockTablePrefixRange>,
    pub sql_index_prefixes_complete: bool,
    pub sql_index_prefixes: Vec<Vec<u8>>,
    pub sql_zone_maps_complete: bool,
    pub sql_zone_maps: Vec<SsTableSqlZoneMap>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct SsTableBlockTablePrefixRange {
    pub table_prefix: Vec<u8>,
    pub first_user_key: Vec<u8>,
    pub last_user_key: Vec<u8>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct SsTableSqlZoneMap {
    pub table_prefix: Vec<u8>,
    pub schema_fingerprint: u64,
    pub column_index: u32,
    pub column_name: String,
    pub type_tag: u8,
    pub value_encoding_version: u8,
    pub min_scalar: i64,
    pub max_scalar: i64,
    pub row_count: u32,
    pub null_count: u32,
    pub non_null_count: u32,
    pub put_count: u32,
    pub tombstone_count: u32,
    pub bounds_valid: bool,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
struct BlockSqlZoneMapV5 {
    table_prefix: Vec<u8>,
    schema_fingerprint: u64,
    column_index: u32,
    column_name: String,
    type_tag: u8,
    value_encoding_version: u8,
    min_scalar: i64,
    max_scalar: i64,
    row_count: u32,
    null_count: u32,
    non_null_count: u32,
    put_count: u32,
    tombstone_count: u32,
    bounds_valid: bool,
}

impl From<BlockSqlZoneMapV5> for SsTableSqlZoneMap {
    fn from(wire: BlockSqlZoneMapV5) -> Self {
        Self {
            table_prefix: wire.table_prefix,
            schema_fingerprint: wire.schema_fingerprint,
            column_index: wire.column_index,
            column_name: wire.column_name,
            type_tag: wire.type_tag,
            value_encoding_version: wire.value_encoding_version,
            min_scalar: wire.min_scalar,
            max_scalar: wire.max_scalar,
            row_count: wire.row_count,
            null_count: wire.null_count,
            non_null_count: wire.non_null_count,
            put_count: wire.put_count,
            tombstone_count: wire.tombstone_count,
            bounds_valid: wire.bounds_valid,
        }
    }
}

impl From<SsTableSqlZoneMap> for BlockSqlZoneMapV5 {
    fn from(zone_map: SsTableSqlZoneMap) -> Self {
        Self {
            table_prefix: zone_map.table_prefix,
            schema_fingerprint: zone_map.schema_fingerprint,
            column_index: zone_map.column_index,
            column_name: zone_map.column_name,
            type_tag: zone_map.type_tag,
            value_encoding_version: zone_map.value_encoding_version,
            min_scalar: zone_map.min_scalar,
            max_scalar: zone_map.max_scalar,
            row_count: zone_map.row_count,
            null_count: zone_map.null_count,
            non_null_count: zone_map.non_null_count,
            put_count: zone_map.put_count,
            tombstone_count: zone_map.tombstone_count,
            bounds_valid: zone_map.bounds_valid,
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
struct BlockTablePrefixRangeV6 {
    table_prefix: Vec<u8>,
    first_user_key: Vec<u8>,
    last_user_key: Vec<u8>,
}

impl From<BlockTablePrefixRangeV6> for SsTableBlockTablePrefixRange {
    fn from(wire: BlockTablePrefixRangeV6) -> Self {
        Self {
            table_prefix: wire.table_prefix,
            first_user_key: wire.first_user_key,
            last_user_key: wire.last_user_key,
        }
    }
}

impl From<SsTableBlockTablePrefixRange> for BlockTablePrefixRangeV6 {
    fn from(range: SsTableBlockTablePrefixRange) -> Self {
        Self {
            table_prefix: range.table_prefix,
            first_user_key: range.first_user_key,
            last_user_key: range.last_user_key,
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
struct BlockSqlZoneMapsSsTableBlockPropertiesV5 {
    offset: u64,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    entry_count: u32,
    table_prefixes: Vec<Vec<u8>>,
    sql_index_prefixes_complete: bool,
    sql_index_prefixes: Vec<Vec<u8>>,
    sql_zone_maps_complete: bool,
    sql_zone_maps: Vec<BlockSqlZoneMapV5>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
struct BlockSqlZoneMapsSsTableBlockPropertiesV6 {
    offset: u64,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    entry_count: u32,
    table_prefixes: Vec<Vec<u8>>,
    table_prefix_ranges_complete: bool,
    table_prefix_ranges: Vec<BlockTablePrefixRangeV6>,
    sql_index_prefixes_complete: bool,
    sql_index_prefixes: Vec<Vec<u8>>,
    sql_zone_maps_complete: bool,
    sql_zone_maps: Vec<BlockSqlZoneMapV5>,
}

impl From<BlockSqlZoneMapsSsTableBlockPropertiesV5> for SsTableBlockProperties {
    fn from(wire: BlockSqlZoneMapsSsTableBlockPropertiesV5) -> Self {
        Self {
            offset: wire.offset,
            first_key: wire.first_key,
            last_key: wire.last_key,
            entry_count: wire.entry_count,
            table_prefixes: wire.table_prefixes,
            table_prefix_ranges_complete: false,
            table_prefix_ranges: Vec::new(),
            sql_index_prefixes_complete: wire.sql_index_prefixes_complete,
            sql_index_prefixes: wire.sql_index_prefixes,
            sql_zone_maps_complete: wire.sql_zone_maps_complete,
            sql_zone_maps: wire
                .sql_zone_maps
                .into_iter()
                .map(SsTableSqlZoneMap::from)
                .collect(),
        }
    }
}

impl From<BlockSqlZoneMapsSsTableBlockPropertiesV6> for SsTableBlockProperties {
    fn from(wire: BlockSqlZoneMapsSsTableBlockPropertiesV6) -> Self {
        Self {
            offset: wire.offset,
            first_key: wire.first_key,
            last_key: wire.last_key,
            entry_count: wire.entry_count,
            table_prefixes: wire.table_prefixes,
            table_prefix_ranges_complete: wire.table_prefix_ranges_complete,
            table_prefix_ranges: wire
                .table_prefix_ranges
                .into_iter()
                .map(SsTableBlockTablePrefixRange::from)
                .collect(),
            sql_index_prefixes_complete: wire.sql_index_prefixes_complete,
            sql_index_prefixes: wire.sql_index_prefixes,
            sql_zone_maps_complete: wire.sql_zone_maps_complete,
            sql_zone_maps: wire
                .sql_zone_maps
                .into_iter()
                .map(SsTableSqlZoneMap::from)
                .collect(),
        }
    }
}

impl From<SsTableBlockProperties> for BlockSqlZoneMapsSsTableBlockPropertiesV6 {
    fn from(property: SsTableBlockProperties) -> Self {
        Self {
            offset: property.offset,
            first_key: property.first_key,
            last_key: property.last_key,
            entry_count: property.entry_count,
            table_prefixes: property.table_prefixes,
            table_prefix_ranges_complete: property.table_prefix_ranges_complete,
            table_prefix_ranges: property
                .table_prefix_ranges
                .into_iter()
                .map(BlockTablePrefixRangeV6::from)
                .collect(),
            sql_index_prefixes_complete: property.sql_index_prefixes_complete,
            sql_index_prefixes: property.sql_index_prefixes,
            sql_zone_maps_complete: property.sql_zone_maps_complete,
            sql_zone_maps: property
                .sql_zone_maps
                .into_iter()
                .map(BlockSqlZoneMapV5::from)
                .collect(),
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
struct BlockSqlIndexPrefixesSsTableBlockPropertiesV4 {
    offset: u64,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    entry_count: u32,
    table_prefixes: Vec<Vec<u8>>,
    sql_index_prefixes_complete: bool,
    sql_index_prefixes: Vec<Vec<u8>>,
}

impl From<BlockSqlIndexPrefixesSsTableBlockPropertiesV4> for SsTableBlockProperties {
    fn from(legacy: BlockSqlIndexPrefixesSsTableBlockPropertiesV4) -> Self {
        Self {
            offset: legacy.offset,
            first_key: legacy.first_key,
            last_key: legacy.last_key,
            entry_count: legacy.entry_count,
            table_prefixes: legacy.table_prefixes,
            table_prefix_ranges_complete: false,
            table_prefix_ranges: Vec::new(),
            sql_index_prefixes_complete: legacy.sql_index_prefixes_complete,
            sql_index_prefixes: legacy.sql_index_prefixes,
            sql_zone_maps_complete: false,
            sql_zone_maps: Vec::new(),
        }
    }
}

impl From<SsTableBlockProperties> for BlockSqlIndexPrefixesSsTableBlockPropertiesV4 {
    fn from(property: SsTableBlockProperties) -> Self {
        Self {
            offset: property.offset,
            first_key: property.first_key,
            last_key: property.last_key,
            entry_count: property.entry_count,
            table_prefixes: property.table_prefixes,
            sql_index_prefixes_complete: property.sql_index_prefixes_complete,
            sql_index_prefixes: property.sql_index_prefixes,
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
struct BlockTablePrefixesSsTableBlockProperties {
    offset: u64,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    entry_count: u32,
    table_prefixes: Vec<Vec<u8>>,
}

impl From<BlockTablePrefixesSsTableBlockProperties> for SsTableBlockProperties {
    fn from(legacy: BlockTablePrefixesSsTableBlockProperties) -> Self {
        Self {
            offset: legacy.offset,
            first_key: legacy.first_key,
            last_key: legacy.last_key,
            entry_count: legacy.entry_count,
            table_prefixes: legacy.table_prefixes,
            table_prefix_ranges_complete: false,
            table_prefix_ranges: Vec::new(),
            sql_index_prefixes_complete: false,
            sql_index_prefixes: Vec::new(),
            sql_zone_maps_complete: false,
            sql_zone_maps: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SsTableOpenDescriptor {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub format_version: u32,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
struct LegacySsTableBlockProperties {
    offset: u64,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    entry_count: u32,
}

impl From<LegacySsTableBlockProperties> for SsTableBlockProperties {
    fn from(legacy: LegacySsTableBlockProperties) -> Self {
        Self {
            offset: legacy.offset,
            first_key: legacy.first_key,
            last_key: legacy.last_key,
            entry_count: legacy.entry_count,
            table_prefixes: Vec::new(),
            table_prefix_ranges_complete: false,
            table_prefix_ranges: Vec::new(),
            sql_index_prefixes_complete: false,
            sql_index_prefixes: Vec::new(),
            sql_zone_maps_complete: false,
            sql_zone_maps: Vec::new(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LegacySsTableMeta {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LegacyBlockPropertiesSsTableMeta {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    block_properties: Vec<LegacySsTableBlockProperties>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct BlockPropertiesSsTableMetaV2 {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    block_properties: Vec<LegacySsTableBlockProperties>,
    format_version: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct BlockTablePrefixesSsTableMetaV3 {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    block_properties: Vec<BlockTablePrefixesSsTableBlockProperties>,
    format_version: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct BlockSqlIndexPrefixesSsTableMetaV4 {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    block_properties: Vec<BlockSqlIndexPrefixesSsTableBlockPropertiesV4>,
    format_version: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct BlockSqlZoneMapsSsTableMetaV5 {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    block_properties: Vec<BlockSqlZoneMapsSsTableBlockPropertiesV5>,
    format_version: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct BlockSqlZoneMapsSsTableMetaV6 {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    block_properties: Vec<BlockSqlZoneMapsSsTableBlockPropertiesV6>,
    format_version: u32,
}

struct SsTableIndexVectors {
    keys: Vec<Vec<u8>>,
    offsets: Vec<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SsTableIndexCacheFingerprint {
    file_len: u64,
    modified_unix_secs: u64,
    modified_subsec_nanos: u32,
    index_offset: u64,
    filter_offset: u64,
    meta_offset: u64,
    index_len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SsTableReverseSeekBlockIndex {
    block_offset: u64,
    decoded_len: u32,
    entry_count: u32,
    decoded_crc32: u32,
    entry_offsets: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SsTableReverseSeekSidecar {
    blocks: Vec<SsTableReverseSeekBlockIndex>,
}

impl SsTableIndexVectors {
    fn push<E>(&mut self, key: Vec<u8>, offset: u64) -> std::result::Result<(), E>
    where
        E: serde::de::Error,
    {
        if self
            .keys
            .last()
            .is_some_and(|previous| previous.as_slice() >= key.as_slice())
        {
            return Err(E::custom("SSTable index keys are not strictly increasing"));
        }
        self.keys.push(key);
        self.offsets.push(offset);
        Ok(())
    }
}

impl SsTableReverseSeekSidecar {
    fn block_for_offset(&self, offset: u64) -> Option<&SsTableReverseSeekBlockIndex> {
        self.blocks
            .binary_search_by_key(&offset, |block| block.block_offset)
            .ok()
            .and_then(|idx| self.blocks.get(idx))
    }
}

impl<'de> serde::Deserialize<'de> for SsTableIndexVectors {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IndexVectorsVisitor;

        impl<'de> Visitor<'de> for IndexVectorsVisitor {
            type Value = SsTableIndexVectors;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an SSTable index map")
            }

            fn visit_map<M>(self, mut access: M) -> std::result::Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let capacity = access.size_hint().unwrap_or(0);
                let mut index = SsTableIndexVectors {
                    keys: Vec::with_capacity(capacity),
                    offsets: Vec::with_capacity(capacity),
                };

                while let Some((key, offset)) = access.next_entry::<Vec<u8>, u64>()? {
                    index.push(key, offset)?;
                }

                Ok(index)
            }
        }

        deserializer.deserialize_map(IndexVectorsVisitor)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LegacySsTableFilterBlockV2 {
    format_version: u32,
    whole_key_filter: BloomFilter,
    prefix_filter: Option<SsTablePrefixFilter>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LegacySsTableFilterBlockV3 {
    format_version: u32,
    whole_key_filter: BloomFilter,
    prefix_filter: Option<SsTablePrefixFilter>,
    user_key_filter: Option<SsTableUserKeyFilter>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SsTableFilterBlock {
    format_version: u32,
    whole_key_filter: BloomFilter,
    prefix_filter: Option<SsTablePrefixFilter>,
    user_key_filter: Option<SsTableUserKeyFilter>,
    sql_index_prefix_filter: Option<SsTablePrefixFilter>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SsTablePrefixFilter {
    extractor_id: u32,
    filter: BloomFilter,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SsTableUserKeyFilter {
    extractor_id: u32,
    suffix_len: usize,
    filter: BloomFilter,
}

pub struct SsTable {
    pub id: u64,
    pub path: PathBuf,
    pub filter: BloomFilter,
    prefix_filter: Option<SsTablePrefixFilter>,
    user_key_filter: Option<SsTableUserKeyFilter>,
    sql_index_prefix_filter: Option<SsTablePrefixFilter>,
    pub block_cache: Arc<BlockCache>,
    pub file_len: u64,
    meta_offset: u64,
    meta_len: u64,
    pub index_keys: Arc<Vec<Vec<u8>>>,
    pub index_offsets: Arc<Vec<u64>>,
    block_properties: Arc<OnceLock<Arc<Vec<SsTableBlockProperties>>>>,
    reverse_seek_fingerprint: Option<SsTableIndexCacheFingerprint>,
    reverse_seek_sidecar: Arc<OnceLock<Option<Arc<SsTableReverseSeekSidecar>>>>,
    pub meta: SsTableMeta,
}

impl SsTable {
    pub async fn open(path: PathBuf, id: u64, block_cache: Arc<BlockCache>) -> Result<Self> {
        Self::open_with_descriptor(path, id, block_cache, None).await
    }

    pub async fn open_with_descriptor(
        path: PathBuf,
        id: u64,
        block_cache: Arc<BlockCache>,
        descriptor: Option<SsTableOpenDescriptor>,
    ) -> Result<Self> {
        let open_started = Instant::now();
        let mut open_stats = monitor::SstableOpenStats::default();
        let mut file = tokio::fs::File::open(&path).await?;
        let file_metadata = file.metadata().await?;
        let len = file_metadata.len();

        if len < 28 {
            // Footer size (8+8+8+4) = 28 bytes now
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "File too small",
            )));
        }

        // Read Footer
        file.seek(SeekFrom::End(-28)).await?;
        let mut footer = [0u8; 28];
        file.read_exact(&mut footer).await?;

        let magic = u32::from_le_bytes(footer[24..28].try_into().unwrap());
        if magic != SST_MAGIC {
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid Magic",
            )));
        }

        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let filter_offset = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let meta_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap());

        let footer_offset = len - 28;
        if index_offset > filter_offset
            || filter_offset > meta_offset
            || meta_offset > footer_offset
        {
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid SSTable footer offsets",
            )));
        }

        let index_len = filter_offset - index_offset;
        let index_cache_fingerprint = Self::index_cache_fingerprint(
            &file_metadata,
            index_offset,
            filter_offset,
            meta_offset,
            index_len,
        );
        let index_cache_path = Self::index_cache_path(&path);
        let mut index_from_cache = None;

        if let Some(fingerprint) = index_cache_fingerprint {
            let phase_started = Instant::now();
            match tokio::fs::read(&index_cache_path).await {
                Ok(index_cache_data) => {
                    open_stats.index_bytes += index_cache_data.len() as u64;
                    open_stats.index_read_us += phase_started.elapsed().as_micros() as u64;
                    let phase_started = Instant::now();
                    match Self::decode_index_cache(&index_cache_data, fingerprint) {
                        Ok(Some(index)) => {
                            monitor::inc_sstable_index_cache_hit();
                            open_stats.index_decode_us +=
                                phase_started.elapsed().as_micros() as u64;
                            index_from_cache = Some(index);
                        }
                        Ok(None) => {
                            monitor::inc_sstable_index_cache_stale();
                        }
                        Err(_) => {
                            monitor::inc_sstable_index_cache_invalid();
                        }
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    monitor::inc_sstable_index_cache_miss();
                }
                Err(_) => {
                    monitor::inc_sstable_index_cache_invalid();
                }
            }
        } else {
            monitor::inc_sstable_index_cache_miss();
        }

        let (index_keys, index_offsets) = if let Some(index) = index_from_cache {
            index
        } else {
            // Read Index
            file.seek(SeekFrom::Start(index_offset)).await?;
            let mut index_data = vec![0u8; index_len as usize];
            let phase_started = Instant::now();
            file.read_exact(&mut index_data).await?;
            open_stats.index_bytes += index_len;
            open_stats.index_read_us += phase_started.elapsed().as_micros() as u64;

            let phase_started = Instant::now();
            let index = Self::decode_index_block(&index_data)?;
            Self::validate_index_vectors(&index.0, &index.1, index_offset, "SSTable index block")?;
            open_stats.index_decode_us += phase_started.elapsed().as_micros() as u64;

            if let Some(fingerprint) = index_cache_fingerprint {
                if let Err(error) =
                    Self::persist_index_cache(&index_cache_path, fingerprint, &index.0, &index.1)
                        .await
                {
                    monitor::inc_sstable_index_cache_write_error();
                    eprintln!(
                        "Warning: failed to persist SSTable index cache {}: {}",
                        index_cache_path.display(),
                        error
                    );
                } else {
                    monitor::inc_sstable_index_cache_write();
                }
            }

            index
        };
        open_stats.index_entries = index_keys.len() as u64;

        // Read Filter
        file.seek(SeekFrom::Start(filter_offset)).await?;
        let filter_len = meta_offset - filter_offset;
        let mut filter_data = vec![0u8; filter_len as usize];
        let phase_started = Instant::now();
        file.read_exact(&mut filter_data).await?;
        open_stats.filter_bytes = filter_len;
        open_stats.filter_read_us = phase_started.elapsed().as_micros() as u64;

        let phase_started = Instant::now();
        let (filter, prefix_filter, user_key_filter, sql_index_prefix_filter) =
            Self::decode_filter_block(&filter_data)?;
        open_stats.filter_decode_us = phase_started.elapsed().as_micros() as u64;

        let meta_len = len - 28 - meta_offset;
        let mut block_properties = Vec::new();
        let meta = if let Some(descriptor) = descriptor {
            SsTableMeta {
                first_key: descriptor.first_key,
                last_key: descriptor.last_key,
                block_properties: Vec::new(),
                format_version: descriptor.format_version,
            }
        } else {
            // Read Meta
            file.seek(SeekFrom::Start(meta_offset)).await?;
            let mut meta_data = vec![0u8; meta_len as usize];
            let phase_started = Instant::now();
            file.read_exact(&mut meta_data).await?;
            open_stats.meta_bytes = meta_len;
            open_stats.meta_read_us = phase_started.elapsed().as_micros() as u64;

            let phase_started = Instant::now();
            let mut meta = Self::decode_meta(&meta_data)?;
            open_stats.meta_decode_us = phase_started.elapsed().as_micros() as u64;
            block_properties = std::mem::take(&mut meta.block_properties);
            meta
        };

        let block_property_count = block_properties.len() as u64;
        let block_properties = Arc::new(block_properties);
        let block_properties_cell = Arc::new(OnceLock::new());
        if block_property_count > 0 {
            let _ = block_properties_cell.set(block_properties);
        }
        open_stats.block_property_count = block_property_count;
        open_stats.total_us = open_started.elapsed().as_micros() as u64;
        monitor::record_sstable_open(open_stats);

        Ok(Self {
            id,
            path,
            filter,
            prefix_filter,
            user_key_filter,
            sql_index_prefix_filter,
            block_cache,
            file_len: index_offset, // Data ends at index_offset
            meta_offset,
            meta_len,
            index_keys: Arc::new(index_keys),
            index_offsets: Arc::new(index_offsets),
            block_properties: block_properties_cell,
            reverse_seek_fingerprint: index_cache_fingerprint,
            reverse_seek_sidecar: Arc::new(OnceLock::new()),
            meta,
        })
    }

    fn decode_index_block(index_data: &[u8]) -> Result<(Vec<Vec<u8>>, Vec<u64>)> {
        if let Some(payload) = Self::versioned_flat_index_payload(index_data) {
            let entries: Vec<(Vec<u8>, u64)> = bincode::deserialize(payload).map_err(|e| {
                crate::common::FusionError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e,
                ))
            })?;
            return Self::index_entries_to_vectors(entries);
        }

        if let Ok(index) = bincode::deserialize::<SsTableIndexVectors>(index_data) {
            return Ok((index.keys, index.offsets));
        }

        if let Ok(entries) = bincode::deserialize::<Vec<(Vec<u8>, u64)>>(index_data) {
            return Self::index_entries_to_vectors(entries);
        }

        let index: BTreeMap<Vec<u8>, u64> = bincode::deserialize(index_data).map_err(|e| {
            crate::common::FusionError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;
        Self::index_entries_to_vectors(index.into_iter().collect())
    }

    fn encode_index_block(index: BTreeMap<Vec<u8>, u64>) -> Vec<u8> {
        let entries = index.into_iter().collect::<Vec<_>>();
        let payload = bincode::serialize(&entries).unwrap();
        let mut encoded = Vec::with_capacity(SSTABLE_INDEX_MAGIC.len() + 4 + payload.len());
        encoded.extend_from_slice(SSTABLE_INDEX_MAGIC);
        encoded.extend_from_slice(&SSTABLE_INDEX_VERSION_FLAT_VEC.to_le_bytes());
        encoded.extend_from_slice(&payload);
        encoded
    }

    fn versioned_flat_index_payload(index_data: &[u8]) -> Option<&[u8]> {
        if index_data.len() < SSTABLE_INDEX_MAGIC.len() + 4 {
            return None;
        }
        if &index_data[..SSTABLE_INDEX_MAGIC.len()] != SSTABLE_INDEX_MAGIC {
            return None;
        }
        let version_start = SSTABLE_INDEX_MAGIC.len();
        let version_end = version_start + 4;
        let version = u32::from_le_bytes(index_data[version_start..version_end].try_into().ok()?);
        if version != SSTABLE_INDEX_VERSION_FLAT_VEC {
            return None;
        }
        Some(&index_data[version_end..])
    }

    fn index_cache_path(path: &Path) -> PathBuf {
        let Some(file_name) = path.file_name() else {
            return path.with_extension("idxcache");
        };
        let mut cache_file_name = file_name.to_os_string();
        cache_file_name.push(".idxcache");
        path.with_file_name(cache_file_name)
    }

    fn reverse_seek_path(path: &Path) -> PathBuf {
        let Some(file_name) = path.file_name() else {
            return path.with_extension("rseek");
        };
        let mut sidecar_file_name = file_name.to_os_string();
        sidecar_file_name.push(".rseek");
        path.with_file_name(sidecar_file_name)
    }

    fn index_cache_fingerprint(
        metadata: &std::fs::Metadata,
        index_offset: u64,
        filter_offset: u64,
        meta_offset: u64,
        index_len: u64,
    ) -> Option<SsTableIndexCacheFingerprint> {
        let modified = metadata.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
        Some(SsTableIndexCacheFingerprint {
            file_len: metadata.len(),
            modified_unix_secs: modified.as_secs(),
            modified_subsec_nanos: modified.subsec_nanos(),
            index_offset,
            filter_offset,
            meta_offset,
            index_len,
        })
    }

    fn encode_index_cache(
        fingerprint: SsTableIndexCacheFingerprint,
        index_keys: &[Vec<u8>],
        index_offsets: &[u64],
    ) -> Vec<u8> {
        debug_assert_eq!(index_keys.len(), index_offsets.len());
        let key_bytes = index_keys.iter().map(Vec::len).sum::<usize>();
        let entry_bytes = index_keys.len() * (4 + 8);
        let mut payload = Vec::with_capacity(8 + key_bytes + entry_bytes);
        payload.extend_from_slice(&(index_keys.len() as u64).to_le_bytes());
        for (key, offset) in index_keys.iter().zip(index_offsets.iter()) {
            payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
            payload.extend_from_slice(key);
            payload.extend_from_slice(&offset.to_le_bytes());
        }

        let payload_crc = Self::crc32(&payload);
        let mut encoded = Vec::with_capacity(4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + payload.len());
        encoded.extend_from_slice(SSTABLE_INDEX_CACHE_MAGIC);
        encoded.extend_from_slice(&SSTABLE_INDEX_CACHE_VERSION.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.file_len.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.modified_unix_secs.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.modified_subsec_nanos.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.index_offset.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.filter_offset.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.meta_offset.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.index_len.to_le_bytes());
        encoded.extend_from_slice(&payload_crc.to_le_bytes());
        encoded.extend_from_slice(&payload);
        encoded
    }

    fn decode_index_cache(
        cache_data: &[u8],
        expected_fingerprint: SsTableIndexCacheFingerprint,
    ) -> Result<Option<(Vec<Vec<u8>>, Vec<u64>)>> {
        let mut cursor = 0usize;
        if Self::take_index_cache_bytes(cache_data, &mut cursor, 4)? != SSTABLE_INDEX_CACHE_MAGIC {
            return Ok(None);
        }

        let version = Self::read_index_cache_u32(cache_data, &mut cursor)?;
        if version != SSTABLE_INDEX_CACHE_VERSION {
            return Ok(None);
        }

        let fingerprint = SsTableIndexCacheFingerprint {
            file_len: Self::read_index_cache_u64(cache_data, &mut cursor)?,
            modified_unix_secs: Self::read_index_cache_u64(cache_data, &mut cursor)?,
            modified_subsec_nanos: Self::read_index_cache_u32(cache_data, &mut cursor)?,
            index_offset: Self::read_index_cache_u64(cache_data, &mut cursor)?,
            filter_offset: Self::read_index_cache_u64(cache_data, &mut cursor)?,
            meta_offset: Self::read_index_cache_u64(cache_data, &mut cursor)?,
            index_len: Self::read_index_cache_u64(cache_data, &mut cursor)?,
        };
        if fingerprint != expected_fingerprint {
            return Ok(None);
        }

        let expected_payload_crc = Self::read_index_cache_u32(cache_data, &mut cursor)?;
        let payload = cache_data.get(cursor..).ok_or_else(|| {
            crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "SSTable index cache payload is truncated",
            ))
        })?;
        if Self::crc32(payload) != expected_payload_crc {
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "SSTable index cache payload checksum mismatch",
            )));
        }

        let entry_count = Self::read_index_cache_u64(cache_data, &mut cursor)? as usize;
        let remaining = cache_data.len().saturating_sub(cursor);
        if entry_count > remaining / 12 {
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "SSTable index cache entry count exceeds payload size",
            )));
        }
        let mut index_keys = Vec::with_capacity(entry_count);
        let mut index_offsets = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            let key_len = Self::read_index_cache_u32(cache_data, &mut cursor)? as usize;
            let key = Self::take_index_cache_bytes(cache_data, &mut cursor, key_len)?.to_vec();
            let offset = Self::read_index_cache_u64(cache_data, &mut cursor)?;
            index_keys.push(key);
            index_offsets.push(offset);
        }
        Self::validate_index_vectors(
            &index_keys,
            &index_offsets,
            expected_fingerprint.index_offset,
            "SSTable index cache",
        )?;

        if cursor != cache_data.len() {
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "SSTable index cache has trailing bytes",
            )));
        }

        Ok(Some((index_keys, index_offsets)))
    }

    fn take_index_cache_bytes<'a>(
        cache_data: &'a [u8],
        cursor: &mut usize,
        len: usize,
    ) -> Result<&'a [u8]> {
        let end = cursor.checked_add(len).ok_or_else(|| {
            crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "SSTable index cache cursor overflow",
            ))
        })?;
        let bytes = cache_data.get(*cursor..end).ok_or_else(|| {
            crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "SSTable index cache is truncated",
            ))
        })?;
        *cursor = end;
        Ok(bytes)
    }

    fn read_index_cache_u32(cache_data: &[u8], cursor: &mut usize) -> Result<u32> {
        let bytes = Self::take_index_cache_bytes(cache_data, cursor, 4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_index_cache_u64(cache_data: &[u8], cursor: &mut usize) -> Result<u64> {
        let bytes = Self::take_index_cache_bytes(cache_data, cursor, 8)?;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn crc32(bytes: &[u8]) -> u32 {
        let mut hasher = Crc32Hasher::new();
        hasher.update(bytes);
        hasher.finalize()
    }

    fn read_block_u32_at(block_data: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = cursor.checked_add(4)?;
        let bytes = block_data.get(*cursor..end)?;
        *cursor = end;
        Some(u32::from_le_bytes(bytes.try_into().ok()?))
    }

    fn decoded_block_entry_spans(block_data: &[u8]) -> Result<Vec<BlockEntrySpan>> {
        let mut cursor = 0usize;
        let count = Self::read_block_u32_at(block_data, &mut cursor)
            .ok_or_else(|| Self::decode_meta_error("SSTable block is missing its entry count"))?;
        let mut spans = Vec::with_capacity(block_entry_reserve_count(count, block_data.len()));

        for entry_index in 0..count {
            let key_len = Self::read_block_u32_at(block_data, &mut cursor).ok_or_else(|| {
                Self::decode_meta_error(format!(
                    "SSTable block entry {entry_index} is missing its key length"
                ))
            })?;
            let key_len = key_len as usize;
            let key_end = cursor.checked_add(key_len).ok_or_else(|| {
                Self::decode_meta_error(format!(
                    "SSTable block entry {entry_index} key length overflows"
                ))
            })?;
            if key_end > block_data.len() {
                return Err(Self::decode_meta_error(format!(
                    "SSTable block entry {entry_index} key is truncated"
                )));
            }
            let key_start = cursor;
            cursor = key_end;

            let value_len = Self::read_block_u32_at(block_data, &mut cursor).ok_or_else(|| {
                Self::decode_meta_error(format!(
                    "SSTable block entry {entry_index} is missing its value length"
                ))
            })?;
            let value_len = value_len as usize;
            let value_end = cursor.checked_add(value_len).ok_or_else(|| {
                Self::decode_meta_error(format!(
                    "SSTable block entry {entry_index} value length overflows"
                ))
            })?;
            if value_end > block_data.len() {
                return Err(Self::decode_meta_error(format!(
                    "SSTable block entry {entry_index} value is truncated"
                )));
            }
            let value_start = cursor;
            cursor = value_end;

            spans.push(BlockEntrySpan {
                key_start,
                key_end,
                value_start,
                value_end,
            });
        }

        if cursor != block_data.len() {
            return Err(Self::decode_meta_error(format!(
                "SSTable block has {} trailing bytes after {count} entries",
                block_data.len() - cursor
            )));
        }

        Ok(spans)
    }

    fn span_user_key_before_bound(
        block_data: &[u8],
        span: &BlockEntrySpan,
        suffix_len: usize,
        bound: &[u8],
    ) -> bool {
        let key = &block_data[span.key_start..span.key_end];
        key_user_part(key, suffix_len) < bound
    }

    fn append_reverse_block_entries_in_bounds(
        block_data: &[u8],
        user_key_lower_bound: Option<&[u8]>,
        user_key_upper_bound: Option<&[u8]>,
        suffix_len: usize,
        out: &mut VecDeque<(Vec<u8>, Vec<u8>)>,
    ) -> Result<ReverseBlockScanStats> {
        let spans = Self::decoded_block_entry_spans(block_data)?;
        let mut stats = ReverseBlockScanStats {
            span_scan_blocks: 1,
            span_scan_entries: spans.len() as u64,
            ..Default::default()
        };
        let lower_idx = user_key_lower_bound
            .map(|bound| {
                spans.partition_point(|span| {
                    Self::span_user_key_before_bound(block_data, span, suffix_len, bound)
                })
            })
            .unwrap_or(0);
        let upper_idx = user_key_upper_bound
            .map(|bound| {
                spans.partition_point(|span| {
                    Self::span_user_key_before_bound(block_data, span, suffix_len, bound)
                })
            })
            .unwrap_or(spans.len());

        if lower_idx >= upper_idx {
            return Ok(stats);
        }

        let bounded_spans = &spans[lower_idx..upper_idx];
        out.reserve(bounded_spans.len());
        for span in bounded_spans.iter().rev() {
            let key = block_data[span.key_start..span.key_end].to_vec();
            let value = block_data[span.value_start..span.value_end].to_vec();
            stats.decoded_entries += 1;
            stats.yielded_entries += 1;
            stats.span_materialized_entries += 1;
            out.push_back((key, value));
        }
        Ok(stats)
    }

    fn entry_at_offset(block_data: &[u8], offset: u32) -> Option<(&[u8], &[u8])> {
        let mut cursor = offset as usize;
        let key_len = Self::read_block_u32_at(block_data, &mut cursor)? as usize;
        let key_end = cursor.checked_add(key_len)?;
        let key = block_data.get(cursor..key_end)?;
        cursor = key_end;
        let value_len = Self::read_block_u32_at(block_data, &mut cursor)? as usize;
        let value_end = cursor.checked_add(value_len)?;
        let value = block_data.get(cursor..value_end)?;
        Some((key, value))
    }

    fn sidecar_offset_before_bound(
        block_data: &[u8],
        offset: u32,
        suffix_len: usize,
        bound: &[u8],
    ) -> bool {
        let Some((key, _)) = Self::entry_at_offset(block_data, offset) else {
            return false;
        };
        key_user_part(key, suffix_len) < bound
    }

    fn append_reverse_block_entries_with_seek_index(
        block_data: &[u8],
        seek_block: &SsTableReverseSeekBlockIndex,
        user_key_lower_bound: Option<&[u8]>,
        user_key_upper_bound: Option<&[u8]>,
        suffix_len: usize,
        out: &mut VecDeque<(Vec<u8>, Vec<u8>)>,
    ) -> Option<ReverseBlockScanStats> {
        if seek_block.decoded_len as usize != block_data.len()
            || seek_block.entry_count as usize != seek_block.entry_offsets.len()
            || seek_block.decoded_crc32 != Self::crc32(block_data)
        {
            return None;
        }

        let mut sidecar_offset_probes = 0u64;
        let lower_idx = if let Some(bound) = user_key_lower_bound {
            seek_block.entry_offsets.partition_point(|offset| {
                sidecar_offset_probes += 1;
                Self::sidecar_offset_before_bound(block_data, *offset, suffix_len, bound)
            })
        } else {
            0
        };
        let upper_idx = if let Some(bound) = user_key_upper_bound {
            seek_block.entry_offsets.partition_point(|offset| {
                sidecar_offset_probes += 1;
                Self::sidecar_offset_before_bound(block_data, *offset, suffix_len, bound)
            })
        } else {
            seek_block.entry_offsets.len()
        };
        let mut stats = ReverseBlockScanStats {
            sidecar_index_entries: seek_block.entry_count as u64,
            sidecar_offset_probes,
            ..Default::default()
        };
        if lower_idx >= upper_idx {
            return Some(stats);
        }

        let offsets = &seek_block.entry_offsets[lower_idx..upper_idx];
        out.reserve(offsets.len());
        for offset in offsets.iter().rev() {
            let (key, value) = Self::entry_at_offset(block_data, *offset)?;
            stats.decoded_entries += 1;
            stats.yielded_entries += 1;
            stats.sidecar_materialized_entries += 1;
            out.push_back((key.to_vec(), value.to_vec()));
        }
        Some(stats)
    }

    async fn reverse_seek_sidecar_for_iterator(
        path: &Path,
        fingerprint: Option<SsTableIndexCacheFingerprint>,
        index_offsets: &[u64],
        sidecar_cell: &Arc<OnceLock<Option<Arc<SsTableReverseSeekSidecar>>>>,
    ) -> Option<Arc<SsTableReverseSeekSidecar>> {
        if let Some(cached) = sidecar_cell.get() {
            return cached.clone();
        }

        let loaded = if let Some(fingerprint) = fingerprint {
            let sidecar_path = Self::reverse_seek_path(path);
            match tokio::fs::read(&sidecar_path).await {
                Ok(sidecar_data) => {
                    match Self::decode_reverse_seek_sidecar(
                        &sidecar_data,
                        fingerprint,
                        index_offsets,
                    ) {
                        Ok(Some(sidecar)) => {
                            monitor::inc_sstable_reverse_seek_sidecar_hit();
                            Some(Arc::new(sidecar))
                        }
                        Ok(None) => {
                            monitor::inc_sstable_reverse_seek_sidecar_stale();
                            None
                        }
                        Err(_) => {
                            monitor::inc_sstable_reverse_seek_sidecar_invalid();
                            None
                        }
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    monitor::inc_sstable_reverse_seek_sidecar_miss();
                    None
                }
                Err(_) => {
                    monitor::inc_sstable_reverse_seek_sidecar_invalid();
                    None
                }
            }
        } else {
            monitor::inc_sstable_reverse_seek_sidecar_miss();
            None
        };

        let _ = sidecar_cell.set(loaded.clone());
        loaded
    }

    async fn persist_index_cache(
        cache_path: &Path,
        fingerprint: SsTableIndexCacheFingerprint,
        index_keys: &[Vec<u8>],
        index_offsets: &[u64],
    ) -> std::io::Result<()> {
        if let Some(parent) = cache_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file_name = cache_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("sstable.idxcache");
        let tmp_path = cache_path.with_file_name(format!("{file_name}.tmp"));
        let bytes = Self::encode_index_cache(fingerprint, index_keys, index_offsets);
        let mut tmp_file = tokio::fs::File::create(&tmp_path).await?;
        tmp_file.write_all(&bytes).await?;
        tmp_file.sync_data().await?;
        drop(tmp_file);
        tokio::fs::rename(tmp_path, cache_path).await
    }

    pub async fn remove_index_cache_file_for_path(path: &Path) {
        let _ = tokio::fs::remove_file(Self::index_cache_path(path)).await;
    }

    fn reverse_seek_block_index_from_entries(
        block_offset: u64,
        count: u32,
        entries: &[u8],
    ) -> Option<SsTableReverseSeekBlockIndex> {
        let mut cursor = 0usize;
        let mut entry_offsets = Vec::with_capacity(block_entry_reserve_count(
            count,
            legacy_block_len(entries.len()),
        ));
        for _ in 0..count {
            let entry_offset = 4usize.checked_add(cursor)?;
            let key_len = Self::read_block_u32_at(entries, &mut cursor)? as usize;
            cursor = cursor.checked_add(key_len)?;
            if cursor > entries.len() {
                return None;
            }
            let value_len = Self::read_block_u32_at(entries, &mut cursor)? as usize;
            cursor = cursor.checked_add(value_len)?;
            if cursor > entries.len() {
                return None;
            }
            entry_offsets.push(u32::try_from(entry_offset).ok()?);
        }
        if cursor != entries.len() {
            return None;
        }

        let mut decoded = Vec::with_capacity(legacy_block_len(entries.len()));
        decoded.extend_from_slice(&count.to_le_bytes());
        decoded.extend_from_slice(entries);
        Some(SsTableReverseSeekBlockIndex {
            block_offset,
            decoded_len: u32::try_from(decoded.len()).ok()?,
            entry_count: count,
            decoded_crc32: Self::crc32(&decoded),
            entry_offsets,
        })
    }

    fn encode_reverse_seek_sidecar(
        fingerprint: SsTableIndexCacheFingerprint,
        sidecar: &SsTableReverseSeekSidecar,
    ) -> Vec<u8> {
        let offset_count = sidecar
            .blocks
            .iter()
            .map(|block| block.entry_offsets.len())
            .sum::<usize>();
        let mut payload = Vec::with_capacity(8 + sidecar.blocks.len() * 28 + offset_count * 4);
        payload.extend_from_slice(&(sidecar.blocks.len() as u64).to_le_bytes());
        for block in &sidecar.blocks {
            payload.extend_from_slice(&block.block_offset.to_le_bytes());
            payload.extend_from_slice(&block.decoded_len.to_le_bytes());
            payload.extend_from_slice(&block.entry_count.to_le_bytes());
            payload.extend_from_slice(&block.decoded_crc32.to_le_bytes());
            payload.extend_from_slice(&(block.entry_offsets.len() as u32).to_le_bytes());
            for offset in &block.entry_offsets {
                payload.extend_from_slice(&offset.to_le_bytes());
            }
        }

        let payload_crc = Self::crc32(&payload);
        let mut encoded = Vec::with_capacity(4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + payload.len());
        encoded.extend_from_slice(SSTABLE_REVERSE_SEEK_MAGIC);
        encoded.extend_from_slice(&SSTABLE_REVERSE_SEEK_VERSION.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.file_len.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.modified_unix_secs.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.modified_subsec_nanos.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.index_offset.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.filter_offset.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.meta_offset.to_le_bytes());
        encoded.extend_from_slice(&fingerprint.index_len.to_le_bytes());
        encoded.extend_from_slice(&payload_crc.to_le_bytes());
        encoded.extend_from_slice(&payload);
        encoded
    }

    fn decode_reverse_seek_sidecar(
        sidecar_data: &[u8],
        expected_fingerprint: SsTableIndexCacheFingerprint,
        index_offsets: &[u64],
    ) -> Result<Option<SsTableReverseSeekSidecar>> {
        let mut cursor = 0usize;
        if Self::take_index_cache_bytes(sidecar_data, &mut cursor, 4)? != SSTABLE_REVERSE_SEEK_MAGIC
        {
            return Ok(None);
        }

        let version = Self::read_index_cache_u32(sidecar_data, &mut cursor)?;
        if version != SSTABLE_REVERSE_SEEK_VERSION {
            return Ok(None);
        }

        let fingerprint = SsTableIndexCacheFingerprint {
            file_len: Self::read_index_cache_u64(sidecar_data, &mut cursor)?,
            modified_unix_secs: Self::read_index_cache_u64(sidecar_data, &mut cursor)?,
            modified_subsec_nanos: Self::read_index_cache_u32(sidecar_data, &mut cursor)?,
            index_offset: Self::read_index_cache_u64(sidecar_data, &mut cursor)?,
            filter_offset: Self::read_index_cache_u64(sidecar_data, &mut cursor)?,
            meta_offset: Self::read_index_cache_u64(sidecar_data, &mut cursor)?,
            index_len: Self::read_index_cache_u64(sidecar_data, &mut cursor)?,
        };
        if fingerprint != expected_fingerprint {
            return Ok(None);
        }

        let expected_payload_crc = Self::read_index_cache_u32(sidecar_data, &mut cursor)?;
        let payload = sidecar_data.get(cursor..).ok_or_else(|| {
            crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "SSTable reverse seek sidecar payload is truncated",
            ))
        })?;
        if Self::crc32(payload) != expected_payload_crc {
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "SSTable reverse seek sidecar payload checksum mismatch",
            )));
        }

        let block_count = Self::read_index_cache_u64(sidecar_data, &mut cursor)? as usize;
        if block_count != index_offsets.len() {
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "SSTable reverse seek sidecar block count does not match index",
            )));
        }

        let mut blocks: Vec<SsTableReverseSeekBlockIndex> = Vec::with_capacity(block_count);
        for (block_idx, expected_offset) in index_offsets.iter().enumerate() {
            let block_offset = Self::read_index_cache_u64(sidecar_data, &mut cursor)?;
            if block_offset != *expected_offset {
                return Err(crate::common::FusionError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "SSTable reverse seek sidecar block offset mismatch",
                )));
            }
            let decoded_len = Self::read_index_cache_u32(sidecar_data, &mut cursor)?;
            let entry_count = Self::read_index_cache_u32(sidecar_data, &mut cursor)?;
            let decoded_crc32 = Self::read_index_cache_u32(sidecar_data, &mut cursor)?;
            let offset_count = Self::read_index_cache_u32(sidecar_data, &mut cursor)? as usize;
            if offset_count != entry_count as usize {
                return Err(crate::common::FusionError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "SSTable reverse seek sidecar entry count mismatch",
                )));
            }
            let mut entry_offsets = Vec::with_capacity(offset_count);
            for offset_idx in 0..offset_count {
                let offset = Self::read_index_cache_u32(sidecar_data, &mut cursor)?;
                if offset < 4 || offset as u64 >= decoded_len as u64 {
                    return Err(crate::common::FusionError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "SSTable reverse seek sidecar entry offset out of bounds",
                    )));
                }
                if offset_idx > 0 && entry_offsets[offset_idx - 1] >= offset {
                    return Err(crate::common::FusionError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "SSTable reverse seek sidecar entry offsets are not increasing",
                    )));
                }
                entry_offsets.push(offset);
            }
            if block_idx > 0 && blocks[block_idx - 1].block_offset >= block_offset {
                return Err(crate::common::FusionError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "SSTable reverse seek sidecar block offsets are not increasing",
                )));
            }
            blocks.push(SsTableReverseSeekBlockIndex {
                block_offset,
                decoded_len,
                entry_count,
                decoded_crc32,
                entry_offsets,
            });
        }

        if cursor != sidecar_data.len() {
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "SSTable reverse seek sidecar has trailing bytes",
            )));
        }

        Ok(Some(SsTableReverseSeekSidecar { blocks }))
    }

    async fn persist_reverse_seek_sidecar(
        sidecar_path: &Path,
        fingerprint: SsTableIndexCacheFingerprint,
        sidecar: &SsTableReverseSeekSidecar,
    ) -> std::io::Result<()> {
        if let Some(parent) = sidecar_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file_name = sidecar_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("sstable.rseek");
        let tmp_path = sidecar_path.with_file_name(format!("{file_name}.tmp"));
        let bytes = Self::encode_reverse_seek_sidecar(fingerprint, sidecar);
        let mut tmp_file = tokio::fs::File::create(&tmp_path).await?;
        tmp_file.write_all(&bytes).await?;
        tmp_file.sync_data().await?;
        drop(tmp_file);
        tokio::fs::rename(tmp_path, sidecar_path).await
    }

    pub async fn remove_reverse_seek_file_for_path(path: &Path) {
        let _ = tokio::fs::remove_file(Self::reverse_seek_path(path)).await;
    }

    fn index_entries_to_vectors(entries: Vec<(Vec<u8>, u64)>) -> Result<(Vec<Vec<u8>>, Vec<u64>)> {
        let mut index_keys: Vec<Vec<u8>> = Vec::with_capacity(entries.len());
        let mut index_offsets: Vec<u64> = Vec::with_capacity(entries.len());

        for (key, offset) in entries {
            if index_keys
                .last()
                .is_some_and(|previous| previous.as_slice() >= key.as_slice())
            {
                return Err(crate::common::FusionError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "SSTable index keys are not strictly increasing",
                )));
            }
            index_keys.push(key);
            index_offsets.push(offset);
        }

        Ok((index_keys, index_offsets))
    }

    fn validate_index_vectors(
        index_keys: &[Vec<u8>],
        index_offsets: &[u64],
        data_len: u64,
        source: &str,
    ) -> Result<()> {
        if index_keys.len() != index_offsets.len() {
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{source} keys and offsets length mismatch"),
            )));
        }

        for idx in 0..index_keys.len() {
            if idx > 0 {
                if index_keys[idx - 1].as_slice() >= index_keys[idx].as_slice() {
                    return Err(crate::common::FusionError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("{source} keys are not strictly increasing"),
                    )));
                }
                if index_offsets[idx - 1] >= index_offsets[idx] {
                    return Err(crate::common::FusionError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("{source} offsets are not strictly increasing"),
                    )));
                }
            }
            if index_offsets[idx] >= data_len {
                return Err(crate::common::FusionError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{source} offset points outside SSTable data blocks"),
                )));
            }
        }

        Ok(())
    }

    pub fn index_offset_for(&self, key: &[u8]) -> Option<u64> {
        self.index_keys
            .binary_search_by(|indexed_key| indexed_key.as_slice().cmp(key))
            .ok()
            .map(|idx| self.index_offsets[idx])
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Bloom Filter Check
        if !self.filter.contains(key) {
            return Ok(None);
        }

        if let Some((k, v)) = self.find_ge(key).await? {
            if k == key {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// Best-effort total entry count for filter sizing during compaction:
    /// sum of per-block counts when block properties are loaded, else a
    /// block-count heuristic.
    pub fn estimated_entry_count(&self) -> usize {
        let props = self.current_block_properties();
        let from_props: usize = props.iter().map(|p| p.entry_count as usize).sum();
        if from_props > 0 {
            return from_props;
        }
        self.index_offsets.len().saturating_mul(512)
    }

    pub fn current_block_properties(&self) -> Arc<Vec<SsTableBlockProperties>> {
        self.block_properties
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(Vec::new()))
    }

    pub fn validated_block_properties_for_zone_maps(
        &self,
    ) -> Option<Arc<Vec<SsTableBlockProperties>>> {
        let block_properties = self.current_block_properties();
        Self::block_properties_match_offsets(&self.index_offsets, block_properties.as_ref())
            .then_some(block_properties)
    }

    pub fn block_property_user_key_interval(
        property: &SsTableBlockProperties,
        suffix_len: usize,
    ) -> Option<(Vec<u8>, Vec<u8>)> {
        if property.first_key.len() < suffix_len || property.last_key.len() < suffix_len {
            return None;
        }
        let first_user_key = key_user_part(&property.first_key, suffix_len);
        let last_user_key = key_user_part(&property.last_key, suffix_len);
        if first_user_key > last_user_key {
            return None;
        }
        Some((first_user_key.to_vec(), last_user_key.to_vec()))
    }

    pub fn block_property_table_prefix_interval(
        property: &SsTableBlockProperties,
        table_prefix: &[u8],
    ) -> Option<Option<(Vec<u8>, Vec<u8>)>> {
        if !property.table_prefix_ranges_complete {
            return None;
        }
        for range in &property.table_prefix_ranges {
            if range.table_prefix.as_slice() != table_prefix {
                continue;
            }
            if range.first_user_key > range.last_user_key {
                return None;
            }
            return Some(Some((
                range.first_user_key.clone(),
                range.last_user_key.clone(),
            )));
        }
        Some(None)
    }

    pub async fn preload_block_properties(&self) {
        if self.block_properties.get().is_some() {
            return;
        }
        let Ok(properties) = self.load_block_properties_from_meta().await else {
            return;
        };
        let properties = Arc::new(properties);
        let _ = self.block_properties.set(properties);
    }

    async fn load_block_properties_from_meta(&self) -> Result<Vec<SsTableBlockProperties>> {
        let mut file = tokio::fs::File::open(&self.path).await?;
        file.seek(SeekFrom::Start(self.meta_offset)).await?;
        let mut meta_data = vec![0u8; self.meta_len as usize];
        file.read_exact(&mut meta_data).await?;
        let mut meta = Self::decode_meta(&meta_data)?;
        Ok(std::mem::take(&mut meta.block_properties))
    }

    pub fn prefix_may_match(&self, prefix: &[u8]) -> bool {
        !matches!(
            self.probe_user_key_prefix_filter(prefix),
            SsTablePrefixFilterProbe::NoMatch
        )
    }

    pub fn probe_user_key_prefix_filter(&self, prefix: &[u8]) -> SsTablePrefixFilterProbe {
        let Some(prefix_filter) = self.prefix_filter.as_ref() else {
            return SsTablePrefixFilterProbe::FailOpen;
        };
        if prefix_filter.extractor_id != SSTABLE_PREFIX_EXTRACTOR_TABLE_USER_KEY {
            return SsTablePrefixFilterProbe::FailOpen;
        }
        let Some(extracted_prefix) = table_user_key_prefix(prefix) else {
            return SsTablePrefixFilterProbe::FailOpen;
        };
        if prefix_filter.filter.contains(extracted_prefix) {
            SsTablePrefixFilterProbe::MayMatch
        } else {
            SsTablePrefixFilterProbe::NoMatch
        }
    }

    pub fn sql_index_prefix_for_range(start: &[u8], end: &[u8]) -> Option<Vec<u8>> {
        let prefix = sql_index_scan_prefix(start)?;
        let prefix_end = prefix_end(prefix)?;
        if end <= prefix_end.as_slice() {
            Some(prefix.to_vec())
        } else {
            None
        }
    }

    pub fn probe_sql_index_prefix_filter(&self, prefix: &[u8]) -> SsTablePrefixFilterProbe {
        let Some(prefix_filter) = self.sql_index_prefix_filter.as_ref() else {
            return SsTablePrefixFilterProbe::FailOpen;
        };
        if prefix_filter.extractor_id != SSTABLE_SQL_INDEX_PREFIX_EXTRACTOR {
            return SsTablePrefixFilterProbe::FailOpen;
        }
        let Some(extracted_prefix) = sql_index_scan_prefix(prefix) else {
            return SsTablePrefixFilterProbe::FailOpen;
        };
        if extracted_prefix != prefix {
            return SsTablePrefixFilterProbe::FailOpen;
        }
        if prefix_filter.filter.contains(extracted_prefix) {
            SsTablePrefixFilterProbe::MayMatch
        } else {
            SsTablePrefixFilterProbe::NoMatch
        }
    }

    pub fn probe_user_key_filter(
        &self,
        user_key: &[u8],
        expected_suffix_len: usize,
    ) -> SsTablePrefixFilterProbe {
        let Some(user_key_filter) = self.user_key_filter.as_ref() else {
            return SsTablePrefixFilterProbe::FailOpen;
        };
        if user_key_filter.extractor_id != SSTABLE_USER_KEY_EXTRACTOR_MVCC_USER_KEY
            || expected_suffix_len == 0
            || user_key_filter.suffix_len != expected_suffix_len
        {
            return SsTablePrefixFilterProbe::FailOpen;
        }
        if user_key_filter.filter.contains(user_key) {
            SsTablePrefixFilterProbe::MayMatch
        } else {
            SsTablePrefixFilterProbe::NoMatch
        }
    }

    pub async fn find_ge(&self, search_key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.index_offsets.is_empty() {
            return Ok(None);
        }

        let block_properties = self.current_block_properties();
        let start_idx = Self::first_block_idx_for_lower_bound(
            &self.index_keys,
            &self.index_offsets,
            block_properties.as_ref(),
            search_key,
        );
        let mut file = None;

        for i in start_idx..self.index_offsets.len() {
            let offset = self.index_offsets[i];
            let block_data = Self::read_block_at_with_reusable_file(
                &self.path,
                &self.block_cache,
                self.id,
                &self.index_offsets,
                self.file_len,
                offset,
                SsTableReadOptions::default(),
                Some(&mut file),
            )
            .await?;

            let spans = Self::decoded_block_entry_spans(block_data.as_ref())?;
            for span in spans {
                let key = &block_data[span.key_start..span.key_end];
                if key >= search_key {
                    return Ok(Some((
                        key.to_vec(),
                        block_data[span.value_start..span.value_end].to_vec(),
                    )));
                }
            }
        }

        Ok(None)
    }

    fn decode_meta_exact<T>(meta_data: &[u8]) -> bincode::Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let mut cursor = std::io::Cursor::new(meta_data);
        let decoded = bincode::deserialize_from(&mut cursor)?;
        if cursor.position() as usize == meta_data.len() {
            Ok(decoded)
        } else {
            Err(Box::new(bincode::ErrorKind::Custom(
                "trailing bytes in SSTable metadata".to_string(),
            )))
        }
    }

    fn encode_versioned_meta<T>(format_version: u32, meta: &T) -> bincode::Result<Vec<u8>>
    where
        T: serde::Serialize,
    {
        let payload = bincode::serialize(meta)?;
        let mut bytes = Vec::with_capacity(SSTABLE_META_HEADER_LEN + payload.len());
        bytes.extend_from_slice(SSTABLE_META_MAGIC);
        bytes.extend_from_slice(&format_version.to_le_bytes());
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    fn versioned_meta_payload(meta_data: &[u8]) -> Option<(u32, &[u8])> {
        if meta_data.len() < SSTABLE_META_HEADER_LEN || !meta_data.starts_with(SSTABLE_META_MAGIC) {
            return None;
        }
        let mut version = [0u8; 4];
        version.copy_from_slice(&meta_data[4..SSTABLE_META_HEADER_LEN]);
        Some((
            u32::from_le_bytes(version),
            &meta_data[SSTABLE_META_HEADER_LEN..],
        ))
    }

    fn unframed_meta_version(meta_data: &[u8]) -> Option<u32> {
        let version_offset = meta_data.len().checked_sub(std::mem::size_of::<u32>())?;
        let version: [u8; 4] = meta_data.get(version_offset..)?.try_into().ok()?;
        Some(u32::from_le_bytes(version))
    }

    fn decode_meta_error(error: impl Into<String>) -> crate::common::FusionError {
        crate::common::FusionError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error.into(),
        ))
    }

    fn decode_meta_bincode_error(error: bincode::Error) -> crate::common::FusionError {
        crate::common::FusionError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }

    fn decode_meta(meta_data: &[u8]) -> Result<SsTableMeta> {
        if let Some((format_version, payload)) = Self::versioned_meta_payload(meta_data) {
            if format_version == SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS {
                let meta = Self::decode_meta_exact::<BlockSqlZoneMapsSsTableMetaV6>(payload)
                    .map_err(Self::decode_meta_bincode_error)?;
                if meta.format_version != format_version {
                    return Err(Self::decode_meta_error(format!(
                        "framed SSTable metadata header version {format_version} does not match payload version {}",
                        meta.format_version
                    )));
                }
                return Ok(SsTableMeta {
                    first_key: meta.first_key,
                    last_key: meta.last_key,
                    block_properties: meta
                        .block_properties
                        .into_iter()
                        .map(SsTableBlockProperties::from)
                        .collect(),
                    format_version: meta.format_version,
                });
            }
            if format_version == SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS_V5 {
                let meta = Self::decode_meta_exact::<BlockSqlZoneMapsSsTableMetaV5>(payload)
                    .map_err(Self::decode_meta_bincode_error)?;
                if meta.format_version != format_version {
                    return Err(Self::decode_meta_error(format!(
                        "framed SSTable metadata header version {format_version} does not match payload version {}",
                        meta.format_version
                    )));
                }
                return Ok(SsTableMeta {
                    first_key: meta.first_key,
                    last_key: meta.last_key,
                    block_properties: meta
                        .block_properties
                        .into_iter()
                        .map(SsTableBlockProperties::from)
                        .collect(),
                    format_version: meta.format_version,
                });
            }
            return Err(Self::decode_meta_error(format!(
                "unsupported framed SSTable metadata version {format_version}"
            )));
        }
        let unframed_version = Self::unframed_meta_version(meta_data);
        if unframed_version == Some(SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS) {
            if let Ok(meta) = Self::decode_meta_exact::<BlockSqlZoneMapsSsTableMetaV6>(meta_data) {
                return Ok(SsTableMeta {
                    first_key: meta.first_key,
                    last_key: meta.last_key,
                    block_properties: meta
                        .block_properties
                        .into_iter()
                        .map(SsTableBlockProperties::from)
                        .collect(),
                    format_version: meta.format_version,
                });
            }
        }
        if unframed_version == Some(SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS_V5) {
            if let Ok(meta) = Self::decode_meta_exact::<BlockSqlZoneMapsSsTableMetaV5>(meta_data) {
                return Ok(SsTableMeta {
                    first_key: meta.first_key,
                    last_key: meta.last_key,
                    block_properties: meta
                        .block_properties
                        .into_iter()
                        .map(SsTableBlockProperties::from)
                        .collect(),
                    format_version: meta.format_version,
                });
            }
        }
        if let Ok(meta) = Self::decode_meta_exact::<BlockSqlIndexPrefixesSsTableMetaV4>(meta_data) {
            let block_properties =
                if meta.format_version == SSTABLE_META_VERSION_BLOCK_SQL_INDEX_PREFIXES {
                    meta.block_properties
                        .into_iter()
                        .map(SsTableBlockProperties::from)
                        .collect()
                } else {
                    Vec::new()
                };
            return Ok(SsTableMeta {
                first_key: meta.first_key,
                last_key: meta.last_key,
                block_properties,
                format_version: meta.format_version,
            });
        }
        if let Ok(meta) = Self::decode_meta_exact::<BlockTablePrefixesSsTableMetaV3>(meta_data) {
            let block_properties =
                if meta.format_version == SSTABLE_META_VERSION_BLOCK_TABLE_PREFIXES {
                    meta.block_properties
                        .into_iter()
                        .map(SsTableBlockProperties::from)
                        .collect()
                } else {
                    Vec::new()
                };
            return Ok(SsTableMeta {
                first_key: meta.first_key,
                last_key: meta.last_key,
                block_properties,
                format_version: meta.format_version,
            });
        }
        if let Ok(meta) = Self::decode_meta_exact::<BlockPropertiesSsTableMetaV2>(meta_data) {
            let block_properties = if meta.format_version == SSTABLE_META_VERSION_BLOCK_PROPERTIES {
                meta.block_properties
                    .into_iter()
                    .map(SsTableBlockProperties::from)
                    .collect()
            } else {
                Vec::new()
            };
            return Ok(SsTableMeta {
                first_key: meta.first_key,
                last_key: meta.last_key,
                block_properties,
                format_version: meta.format_version,
            });
        }
        if let Ok(meta) = Self::decode_meta_exact::<LegacyBlockPropertiesSsTableMeta>(meta_data) {
            return Ok(SsTableMeta {
                first_key: meta.first_key,
                last_key: meta.last_key,
                block_properties: meta
                    .block_properties
                    .into_iter()
                    .map(SsTableBlockProperties::from)
                    .collect(),
                format_version: 1,
            });
        }
        let legacy: LegacySsTableMeta = Self::decode_meta_exact(meta_data).map_err(|e| {
            crate::common::FusionError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;
        Ok(SsTableMeta {
            first_key: legacy.first_key,
            last_key: legacy.last_key,
            block_properties: Vec::new(),
            format_version: 0,
        })
    }

    fn decode_filter_block(
        filter_data: &[u8],
    ) -> Result<(
        BloomFilter,
        Option<SsTablePrefixFilter>,
        Option<SsTableUserKeyFilter>,
        Option<SsTablePrefixFilter>,
    )> {
        if let Ok(filter_block) = bincode::deserialize::<SsTableFilterBlock>(filter_data) {
            if filter_block.format_version == SSTABLE_FILTER_VERSION_SQL_INDEX_PREFIX {
                return Ok((
                    filter_block.whole_key_filter,
                    filter_block.prefix_filter,
                    filter_block.user_key_filter,
                    filter_block.sql_index_prefix_filter,
                ));
            }
            return Ok((filter_block.whole_key_filter, None, None, None));
        }

        if let Ok(filter_block) = bincode::deserialize::<LegacySsTableFilterBlockV3>(filter_data) {
            if filter_block.format_version == SSTABLE_FILTER_VERSION_USER_KEY {
                return Ok((
                    filter_block.whole_key_filter,
                    filter_block.prefix_filter,
                    filter_block.user_key_filter,
                    None,
                ));
            }
            if filter_block.format_version == SSTABLE_FILTER_VERSION_PREFIX {
                return Ok((
                    filter_block.whole_key_filter,
                    filter_block.prefix_filter,
                    None,
                    None,
                ));
            }
            return Ok((filter_block.whole_key_filter, None, None, None));
        }

        if let Ok(filter_block) = bincode::deserialize::<LegacySsTableFilterBlockV2>(filter_data) {
            if filter_block.format_version == SSTABLE_FILTER_VERSION_PREFIX {
                return Ok((
                    filter_block.whole_key_filter,
                    filter_block.prefix_filter,
                    None,
                    None,
                ));
            }
            return Ok((filter_block.whole_key_filter, None, None, None));
        }

        let filter: BloomFilter = bincode::deserialize(filter_data).map_err(|e| {
            crate::common::FusionError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;
        Ok((filter, None, None, None))
    }

    fn first_block_idx_for_lower_bound(
        index_keys: &[Vec<u8>],
        index_offsets: &[u64],
        block_properties: &[SsTableBlockProperties],
        key: &[u8],
    ) -> usize {
        if block_properties.len() == index_offsets.len()
            && block_properties
                .iter()
                .zip(index_offsets)
                .all(|(property, offset)| property.offset == *offset)
        {
            return block_properties.partition_point(|property| property.last_key.as_slice() < key);
        }

        match index_keys.binary_search_by(|indexed_key| indexed_key.as_slice().cmp(key)) {
            Ok(idx) => idx,
            Err(idx) => idx.saturating_sub(1),
        }
    }

    /// Verify CRC32 of a block. Returns the data portion (without trailing CRC).
    fn verify_block_crc(block_data: &[u8]) -> Result<&[u8]> {
        if block_data.len() < 4 {
            return Ok(block_data); // Too small for CRC, legacy block
        }
        let (data, crc_bytes) = block_data.split_at(block_data.len() - 4);
        let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap_or([0; 4]));
        // If stored_crc is 0, treat as legacy block without checksum
        if stored_crc == 0 {
            return Ok(block_data);
        }
        let mut hasher = Crc32Hasher::new();
        hasher.update(data);
        let computed = hasher.finalize();
        if computed != stored_crc {
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "SSTable block CRC mismatch: expected {:08x}, got {:08x}",
                    stored_crc, computed
                ),
            )));
        }
        Ok(data)
    }

    fn encode_block_payload(count: u32, entries: &[u8]) -> Vec<u8> {
        let compressed = compress_prepend_size(entries);
        let compressed_len = compressed_block_payload_capacity(compressed.len());
        let legacy_len = legacy_block_len(entries.len());

        if compressed_len < legacy_len {
            let mut encoded = Vec::with_capacity(compressed_len);
            encoded.extend_from_slice(COMPRESSED_BLOCK_MAGIC);
            encoded.extend_from_slice(&count.to_le_bytes());
            encoded.extend_from_slice(&compressed);
            encoded
        } else {
            let mut encoded = Vec::with_capacity(legacy_len);
            encoded.extend_from_slice(&count.to_le_bytes());
            encoded.extend_from_slice(entries);
            encoded
        }
    }

    fn decode_block_payload(block_data: &[u8]) -> Result<Vec<u8>> {
        let data = Self::verify_block_crc(block_data)?;
        if data.len() < COMPRESSED_BLOCK_HEADER_LEN
            || &data[..COMPRESSED_BLOCK_MAGIC.len()] != COMPRESSED_BLOCK_MAGIC
        {
            return Ok(data.to_vec());
        }

        let count = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let entries =
            decompress_size_prepended(&data[COMPRESSED_BLOCK_HEADER_LEN..]).map_err(|error| {
                crate::common::FusionError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("SSTable block decompression failed: {}", error),
                ))
            })?;

        let mut decoded = Vec::with_capacity(legacy_block_len(entries.len()));
        decoded.extend_from_slice(&count.to_le_bytes());
        decoded.extend_from_slice(&entries);
        Ok(decoded)
    }

    async fn read_block_at(
        path: &PathBuf,
        block_cache: &Arc<BlockCache>,
        sst_id: u64,
        index_offsets: &[u64],
        file_len: u64,
        offset: u64,
        read_options: SsTableReadOptions,
    ) -> Result<BlockCacheValue> {
        Self::read_block_at_with_reusable_file(
            path,
            block_cache,
            sst_id,
            index_offsets,
            file_len,
            offset,
            read_options,
            None,
        )
        .await
    }

    async fn read_block_bytes(file: &mut File, offset: u64, len: usize) -> Result<Vec<u8>> {
        file.seek(SeekFrom::Start(offset)).await?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf).await?;
        monitor::inc_sstable_block_read_bytes(len as u64);
        Ok(buf)
    }

    async fn read_block_at_with_reusable_file(
        path: &PathBuf,
        block_cache: &Arc<BlockCache>,
        sst_id: u64,
        index_offsets: &[u64],
        file_len: u64,
        offset: u64,
        read_options: SsTableReadOptions,
        mut reusable_file: Option<&mut Option<File>>,
    ) -> Result<BlockCacheValue> {
        if let Some(data) = block_cache.get(&(sst_id, offset)) {
            monitor::inc_block_cache_hit();
            return Ok(data);
        }
        monitor::inc_block_cache_miss();

        let mut next_offset = file_len;

        if let Ok(idx) = index_offsets.binary_search(&offset) {
            if idx + 1 < index_offsets.len() {
                next_offset = index_offsets[idx + 1];
            }
        }

        let len = (next_offset - offset) as usize;
        let buf = if let Some(file_slot) = reusable_file.as_deref_mut() {
            if file_slot.is_none() {
                let file = File::open(path).await?;
                monitor::inc_sstable_block_file_open();
                *file_slot = Some(file);
            }
            let file = file_slot.as_mut().expect("reusable file initialized");
            Self::read_block_bytes(file, offset, len).await?
        } else {
            let mut file = File::open(path).await?;
            monitor::inc_sstable_block_file_open();
            Self::read_block_bytes(&mut file, offset, len).await?
        };

        let decoded: BlockCacheValue = Self::decode_block_payload(&buf)?.into();
        if read_options.fill_cache {
            monitor::inc_block_cache_insert(decoded.len() as u64);
            block_cache.insert((sst_id, offset), decoded.clone());
        } else {
            monitor::inc_block_cache_fill_skip();
        }
        Ok(decoded)
    }

    pub async fn read_block(&self, offset: u64) -> Result<BlockCacheValue> {
        self.read_block_with_options(offset, SsTableReadOptions::default())
            .await
    }

    pub async fn read_block_with_options(
        &self,
        offset: u64,
        read_options: SsTableReadOptions,
    ) -> Result<BlockCacheValue> {
        Self::read_block_at(
            &self.path,
            &self.block_cache,
            self.id,
            &self.index_offsets,
            self.file_len,
            offset,
            read_options,
        )
        .await
    }

    pub async fn new_iterator(&self, start_key: Option<&[u8]>) -> Result<SsTableIterator> {
        self.new_range_iterator(start_key, None).await
    }

    pub async fn new_range_iterator(
        &self,
        start_key: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> Result<SsTableIterator> {
        self.new_range_iterator_with_options(start_key, upper_bound, SsTableReadOptions::default())
            .await
    }

    pub async fn new_iterator_with_options(
        &self,
        start_key: Option<&[u8]>,
        read_options: SsTableReadOptions,
    ) -> Result<SsTableIterator> {
        self.new_range_iterator_with_options(start_key, None, read_options)
            .await
    }

    pub async fn new_range_iterator_with_options(
        &self,
        start_key: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
        read_options: SsTableReadOptions,
    ) -> Result<SsTableIterator> {
        self.new_iterator_with_upper_bound(
            start_key,
            upper_bound.map(|key| SsTableIteratorUpperBound::Raw(key.to_vec())),
            read_options,
            None,
        )
        .await
    }

    pub async fn new_user_key_range_iterator(
        &self,
        start_key: Option<&[u8]>,
        user_key_upper_bound: Option<&[u8]>,
        suffix_len: usize,
    ) -> Result<SsTableIterator> {
        self.new_user_key_range_iterator_with_options(
            start_key,
            user_key_upper_bound,
            suffix_len,
            SsTableReadOptions::default(),
        )
        .await
    }

    pub async fn new_user_key_range_iterator_with_options(
        &self,
        start_key: Option<&[u8]>,
        user_key_upper_bound: Option<&[u8]>,
        suffix_len: usize,
        read_options: SsTableReadOptions,
    ) -> Result<SsTableIterator> {
        self.new_user_key_range_iterator_with_options_and_block_skips(
            start_key,
            user_key_upper_bound,
            suffix_len,
            read_options,
            None,
        )
        .await
    }

    pub(crate) async fn new_user_key_range_iterator_with_options_and_block_skips(
        &self,
        start_key: Option<&[u8]>,
        user_key_upper_bound: Option<&[u8]>,
        suffix_len: usize,
        read_options: SsTableReadOptions,
        approved_block_skip_offsets: Option<Arc<BTreeSet<u64>>>,
    ) -> Result<SsTableIterator> {
        self.new_iterator_with_upper_bound(
            start_key,
            user_key_upper_bound.map(|key| SsTableIteratorUpperBound::UserKey {
                bound: key.to_vec(),
                suffix_len,
            }),
            read_options,
            approved_block_skip_offsets,
        )
        .await
    }

    pub async fn new_user_key_range_reverse_iterator(
        &self,
        user_key_lower_bound: Option<&[u8]>,
        user_key_upper_bound: Option<&[u8]>,
        suffix_len: usize,
    ) -> Result<SsTableReverseIterator> {
        self.new_user_key_range_reverse_iterator_with_options(
            user_key_lower_bound,
            user_key_upper_bound,
            suffix_len,
            SsTableReadOptions::default(),
        )
        .await
    }

    pub async fn new_user_key_range_reverse_iterator_with_options(
        &self,
        user_key_lower_bound: Option<&[u8]>,
        user_key_upper_bound: Option<&[u8]>,
        suffix_len: usize,
        read_options: SsTableReadOptions,
    ) -> Result<SsTableReverseIterator> {
        let block_properties = self.current_block_properties();
        let current_block_idx = if user_key_lower_bound
            .zip(user_key_upper_bound)
            .is_some_and(|(lower, upper)| lower >= upper)
        {
            None
        } else {
            Self::last_block_idx_before_user_upper_bound(
                &self.index_keys,
                &self.index_offsets,
                block_properties.as_ref(),
                user_key_upper_bound,
                suffix_len,
            )
        };
        let block_table_prefix_filter = SsTableReverseIterator::block_table_prefix_filter(
            user_key_lower_bound,
            user_key_upper_bound,
        );
        let block_sql_index_prefix_filter = SsTableReverseIterator::block_sql_index_prefix_filter(
            user_key_lower_bound,
            user_key_upper_bound,
        );

        Ok(SsTableReverseIterator {
            path: self.path.clone(),
            block_cache: self.block_cache.clone(),
            sst_id: self.id,
            index_keys: self.index_keys.clone(),
            index_offsets: self.index_offsets.clone(),
            block_properties,
            reverse_seek_fingerprint: self.reverse_seek_fingerprint,
            reverse_seek_sidecar: self.reverse_seek_sidecar.clone(),
            file_len: self.file_len,
            current_block_idx,
            current_block_entries: block_entry_buffer(),
            user_key_lower_bound: user_key_lower_bound.map(|key| key.to_vec()),
            user_key_upper_bound: user_key_upper_bound.map(|key| key.to_vec()),
            suffix_len,
            block_table_prefix_filter,
            block_sql_index_prefix_filter,
            read_options,
            file: None,
        })
    }

    async fn new_iterator_with_upper_bound(
        &self,
        start_key: Option<&[u8]>,
        upper_bound: Option<SsTableIteratorUpperBound>,
        read_options: SsTableReadOptions,
        approved_block_skip_offsets: Option<Arc<BTreeSet<u64>>>,
    ) -> Result<SsTableIterator> {
        let block_properties = self.current_block_properties();
        let start_idx = if let Some(key) = start_key {
            Self::first_block_idx_for_lower_bound(
                &self.index_keys,
                &self.index_offsets,
                block_properties.as_ref(),
                key,
            )
        } else {
            0
        };
        let block_table_prefix_filter =
            SsTableIterator::block_table_prefix_filter(start_key, upper_bound.as_ref());
        let block_sql_index_prefix_filter =
            SsTableIterator::block_sql_index_prefix_filter(start_key, upper_bound.as_ref());

        Ok(SsTableIterator {
            path: self.path.clone(),
            block_cache: self.block_cache.clone(),
            sst_id: self.id,
            index_keys: self.index_keys.clone(),
            index_offsets: self.index_offsets.clone(),
            block_properties,
            file_len: self.file_len,
            current_block_idx: start_idx,
            current_block_entries: block_entry_buffer(),
            lower_bound: start_key.map(|key| key.to_vec()),
            upper_bound,
            block_table_prefix_filter,
            block_sql_index_prefix_filter,
            approved_block_skip_offsets,
            read_options,
            file: None,
        })
    }

    fn block_properties_match_offsets(
        index_offsets: &[u64],
        block_properties: &[SsTableBlockProperties],
    ) -> bool {
        block_properties.len() == index_offsets.len()
            && block_properties
                .iter()
                .zip(index_offsets)
                .all(|(property, offset)| property.offset == *offset)
    }

    fn last_block_idx_before_user_upper_bound(
        index_keys: &[Vec<u8>],
        index_offsets: &[u64],
        block_properties: &[SsTableBlockProperties],
        user_key_upper_bound: Option<&[u8]>,
        suffix_len: usize,
    ) -> Option<usize> {
        if index_offsets.is_empty() {
            return None;
        }
        let Some(upper_bound) = user_key_upper_bound else {
            return Some(index_offsets.len() - 1);
        };

        let end_idx = if Self::block_properties_match_offsets(index_offsets, block_properties) {
            block_properties.partition_point(|property| {
                key_user_part(&property.first_key, suffix_len) < upper_bound
            })
        } else {
            index_keys.partition_point(|key| key_user_part(key, suffix_len) < upper_bound)
        };
        end_idx.checked_sub(1)
    }

    pub fn reverse_frontier_for_range(
        &self,
        user_key_lower_bound: &[u8],
        user_key_upper_bound: &[u8],
        suffix_len: usize,
    ) -> Option<SsTableReverseFrontier> {
        if user_key_lower_bound >= user_key_upper_bound || self.index_offsets.is_empty() {
            return None;
        }

        let table_min_user_key = key_user_part(&self.meta.first_key, suffix_len);
        let table_max_user_key = key_user_part(&self.meta.last_key, suffix_len);
        if table_max_user_key < user_key_lower_bound || table_min_user_key >= user_key_upper_bound {
            return None;
        }

        let block_properties = self.current_block_properties();
        let block_idx = Self::last_block_idx_before_user_upper_bound(
            &self.index_keys,
            &self.index_offsets,
            block_properties.as_ref(),
            Some(user_key_upper_bound),
            suffix_len,
        )?;

        if Self::block_properties_match_offsets(&self.index_offsets, block_properties.as_ref()) {
            if let Some(property) = block_properties.get(block_idx) {
                if key_user_part(&property.last_key, suffix_len) < user_key_lower_bound {
                    return None;
                }
                let block_last_user_key = key_user_part(&property.last_key, suffix_len);
                return Some(SsTableReverseFrontier {
                    user_key: if block_last_user_key < user_key_upper_bound {
                        block_last_user_key.to_vec()
                    } else {
                        user_key_upper_bound.to_vec()
                    },
                    kind: SsTableReverseFrontierKind::BlockProperty,
                });
            }
        }

        Some(SsTableReverseFrontier {
            user_key: if table_max_user_key < user_key_upper_bound {
                table_max_user_key.to_vec()
            } else {
                user_key_upper_bound.to_vec()
            },
            kind: SsTableReverseFrontierKind::FileFallback,
        })
    }

    pub fn reverse_frontier_user_key_for_range(
        &self,
        user_key_lower_bound: &[u8],
        user_key_upper_bound: &[u8],
        suffix_len: usize,
    ) -> Option<Vec<u8>> {
        self.reverse_frontier_for_range(user_key_lower_bound, user_key_upper_bound, suffix_len)
            .map(|frontier| frontier.user_key)
    }
}

// Implement Clone manually if needed or derive?
// BloomFilter might not implement Clone.
// Let's assume we don't clone SsTable often.

pub struct SsTableIterator {
    path: PathBuf,
    block_cache: Arc<BlockCache>,
    sst_id: u64,
    index_keys: Arc<Vec<Vec<u8>>>,
    index_offsets: Arc<Vec<u64>>,
    block_properties: Arc<Vec<SsTableBlockProperties>>,
    file_len: u64,
    current_block_idx: usize,
    current_block_entries: VecDeque<(Vec<u8>, Vec<u8>)>,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<SsTableIteratorUpperBound>,
    block_table_prefix_filter: Option<Vec<u8>>,
    block_sql_index_prefix_filter: Option<Vec<u8>>,
    approved_block_skip_offsets: Option<Arc<BTreeSet<u64>>>,
    read_options: SsTableReadOptions,
    file: Option<File>,
}

enum SsTableIteratorUpperBound {
    Raw(Vec<u8>),
    UserKey { bound: Vec<u8>, suffix_len: usize },
}

impl SsTableIterator {
    fn block_table_prefix_filter(
        start_key: Option<&[u8]>,
        upper_bound: Option<&SsTableIteratorUpperBound>,
    ) -> Option<Vec<u8>> {
        let start_key = start_key?;
        let Some(SsTableIteratorUpperBound::UserKey { bound, suffix_len }) = upper_bound else {
            return None;
        };
        let user_key_len = start_key.len().checked_sub(*suffix_len)?;
        let user_key = &start_key[..user_key_len];
        if table_user_key_prefix(user_key)? != user_key {
            return None;
        }
        if prefix_end(user_key)?.as_slice() != bound.as_slice() {
            return None;
        }
        Some(user_key.to_vec())
    }

    fn block_sql_index_prefix_filter(
        start_key: Option<&[u8]>,
        upper_bound: Option<&SsTableIteratorUpperBound>,
    ) -> Option<Vec<u8>> {
        let start_key = start_key?;
        let Some(SsTableIteratorUpperBound::UserKey { bound, suffix_len }) = upper_bound else {
            return None;
        };
        let user_key_len = start_key.len().checked_sub(*suffix_len)?;
        SsTable::sql_index_prefix_for_range(&start_key[..user_key_len], bound)
    }

    fn block_table_prefix_probe(
        &self,
        block_idx: usize,
        offset: u64,
    ) -> Option<SsTablePrefixFilterProbe> {
        let target = self.block_table_prefix_filter.as_ref()?;
        if self.block_properties.len() != self.index_offsets.len() {
            return Some(SsTablePrefixFilterProbe::FailOpen);
        }
        let Some(property) = self.block_properties.get(block_idx) else {
            return Some(SsTablePrefixFilterProbe::FailOpen);
        };
        if property.offset != offset || property.table_prefixes.is_empty() {
            return Some(SsTablePrefixFilterProbe::FailOpen);
        }
        if property
            .table_prefixes
            .iter()
            .any(|prefix| prefix.as_slice() == target.as_slice())
        {
            Some(SsTablePrefixFilterProbe::MayMatch)
        } else {
            Some(SsTablePrefixFilterProbe::NoMatch)
        }
    }

    fn block_sql_index_prefix_probe(
        &self,
        block_idx: usize,
        offset: u64,
    ) -> Option<SsTablePrefixFilterProbe> {
        let target = self.block_sql_index_prefix_filter.as_ref()?;
        if self.block_properties.len() != self.index_offsets.len() {
            return Some(SsTablePrefixFilterProbe::FailOpen);
        }
        let Some(property) = self.block_properties.get(block_idx) else {
            return Some(SsTablePrefixFilterProbe::FailOpen);
        };
        if property.offset != offset || !property.sql_index_prefixes_complete {
            return Some(SsTablePrefixFilterProbe::FailOpen);
        }
        if property
            .sql_index_prefixes
            .iter()
            .any(|prefix| prefix.as_slice() == target.as_slice())
        {
            Some(SsTablePrefixFilterProbe::MayMatch)
        } else {
            Some(SsTablePrefixFilterProbe::NoMatch)
        }
    }

    fn key_at_or_after_upper_bound(&self, key: &[u8]) -> bool {
        match self.upper_bound.as_ref() {
            Some(SsTableIteratorUpperBound::Raw(bound)) => key >= bound.as_slice(),
            Some(SsTableIteratorUpperBound::UserKey { bound, suffix_len }) => {
                key_user_part(key, *suffix_len) >= bound.as_slice()
            }
            None => false,
        }
    }

    fn block_skip_approved(&self, offset: u64) -> bool {
        self.approved_block_skip_offsets
            .as_ref()
            .is_some_and(|offsets| offsets.contains(&offset))
    }

    pub async fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            if let Some(entry) = self.current_block_entries.pop_front() {
                return Ok(Some(entry));
            }

            // Load next block
            if self.current_block_idx >= self.index_offsets.len() {
                return Ok(None);
            }
            if self.key_at_or_after_upper_bound(&self.index_keys[self.current_block_idx]) {
                return Ok(None);
            }

            let block_idx = self.current_block_idx;
            let offset = self.index_offsets[block_idx];
            self.current_block_idx += 1;

            if let Some(probe) = self.block_table_prefix_probe(block_idx, offset) {
                monitor::inc_sstable_block_prefix_filter_check();
                match probe {
                    SsTablePrefixFilterProbe::MayMatch => {
                        monitor::inc_sstable_block_prefix_filter_positive();
                    }
                    SsTablePrefixFilterProbe::NoMatch => {
                        monitor::inc_sstable_block_prefix_filter_skip();
                        continue;
                    }
                    SsTablePrefixFilterProbe::FailOpen => {
                        monitor::inc_sstable_block_prefix_filter_fail_open();
                    }
                }
            }
            if let Some(probe) = self.block_sql_index_prefix_probe(block_idx, offset) {
                monitor::inc_sstable_block_index_prefix_filter_check();
                match probe {
                    SsTablePrefixFilterProbe::MayMatch => {
                        monitor::inc_sstable_block_index_prefix_filter_positive();
                    }
                    SsTablePrefixFilterProbe::NoMatch => {
                        monitor::inc_sstable_block_index_prefix_filter_skip();
                        continue;
                    }
                    SsTablePrefixFilterProbe::FailOpen => {
                        monitor::inc_sstable_block_index_prefix_filter_fail_open();
                    }
                }
            }

            if self.block_skip_approved(offset) {
                continue;
            }

            let block_data = SsTable::read_block_at_with_reusable_file(
                &self.path,
                &self.block_cache,
                self.sst_id,
                &self.index_offsets,
                self.file_len,
                offset,
                self.read_options,
                Some(&mut self.file),
            )
            .await?;

            let spans = SsTable::decoded_block_entry_spans(block_data.as_ref())?;
            self.current_block_entries.reserve(spans.len());

            for span in spans {
                let key = &block_data[span.key_start..span.key_end];

                if self.key_at_or_after_upper_bound(key) {
                    break;
                }

                if self
                    .lower_bound
                    .as_ref()
                    .map_or(true, |lower_bound| key >= lower_bound.as_slice())
                {
                    self.current_block_entries.push_back((
                        key.to_vec(),
                        block_data[span.value_start..span.value_end].to_vec(),
                    ));
                }
            }
        }
    }
}

pub struct SsTableReverseIterator {
    path: PathBuf,
    block_cache: Arc<BlockCache>,
    sst_id: u64,
    index_keys: Arc<Vec<Vec<u8>>>,
    index_offsets: Arc<Vec<u64>>,
    block_properties: Arc<Vec<SsTableBlockProperties>>,
    reverse_seek_fingerprint: Option<SsTableIndexCacheFingerprint>,
    reverse_seek_sidecar: Arc<OnceLock<Option<Arc<SsTableReverseSeekSidecar>>>>,
    file_len: u64,
    current_block_idx: Option<usize>,
    current_block_entries: VecDeque<(Vec<u8>, Vec<u8>)>,
    user_key_lower_bound: Option<Vec<u8>>,
    user_key_upper_bound: Option<Vec<u8>>,
    suffix_len: usize,
    block_table_prefix_filter: Option<Vec<u8>>,
    block_sql_index_prefix_filter: Option<Vec<u8>>,
    read_options: SsTableReadOptions,
    file: Option<File>,
}

impl SsTableReverseIterator {
    fn block_table_prefix_filter(
        user_key_lower_bound: Option<&[u8]>,
        user_key_upper_bound: Option<&[u8]>,
    ) -> Option<Vec<u8>> {
        let lower = user_key_lower_bound?;
        let upper = user_key_upper_bound?;
        if table_user_key_prefix(lower)? != lower {
            return None;
        }
        if prefix_end(lower)?.as_slice() != upper {
            return None;
        }
        Some(lower.to_vec())
    }

    fn block_sql_index_prefix_filter(
        user_key_lower_bound: Option<&[u8]>,
        user_key_upper_bound: Option<&[u8]>,
    ) -> Option<Vec<u8>> {
        SsTable::sql_index_prefix_for_range(user_key_lower_bound?, user_key_upper_bound?)
    }

    fn block_table_prefix_probe(
        &self,
        block_idx: usize,
        offset: u64,
    ) -> Option<SsTablePrefixFilterProbe> {
        let target = self.block_table_prefix_filter.as_ref()?;
        if self.block_properties.len() != self.index_offsets.len() {
            return Some(SsTablePrefixFilterProbe::FailOpen);
        }
        let Some(property) = self.block_properties.get(block_idx) else {
            return Some(SsTablePrefixFilterProbe::FailOpen);
        };
        if property.offset != offset || property.table_prefixes.is_empty() {
            return Some(SsTablePrefixFilterProbe::FailOpen);
        }
        if property
            .table_prefixes
            .iter()
            .any(|prefix| prefix.as_slice() == target.as_slice())
        {
            Some(SsTablePrefixFilterProbe::MayMatch)
        } else {
            Some(SsTablePrefixFilterProbe::NoMatch)
        }
    }

    fn block_sql_index_prefix_probe(
        &self,
        block_idx: usize,
        offset: u64,
    ) -> Option<SsTablePrefixFilterProbe> {
        let target = self.block_sql_index_prefix_filter.as_ref()?;
        if self.block_properties.len() != self.index_offsets.len() {
            return Some(SsTablePrefixFilterProbe::FailOpen);
        }
        let Some(property) = self.block_properties.get(block_idx) else {
            return Some(SsTablePrefixFilterProbe::FailOpen);
        };
        if property.offset != offset || !property.sql_index_prefixes_complete {
            return Some(SsTablePrefixFilterProbe::FailOpen);
        }
        if property
            .sql_index_prefixes
            .iter()
            .any(|prefix| prefix.as_slice() == target.as_slice())
        {
            Some(SsTablePrefixFilterProbe::MayMatch)
        } else {
            Some(SsTablePrefixFilterProbe::NoMatch)
        }
    }

    fn key_at_or_after_upper_bound(&self, key: &[u8]) -> bool {
        self.user_key_upper_bound
            .as_ref()
            .is_some_and(|bound| key_user_part(key, self.suffix_len) >= bound.as_slice())
    }

    fn block_before_lower_bound(&self, block_idx: usize, offset: u64) -> bool {
        let Some(lower_bound) = self.user_key_lower_bound.as_ref() else {
            return false;
        };
        if !SsTable::block_properties_match_offsets(&self.index_offsets, &self.block_properties) {
            return false;
        }
        let Some(property) = self.block_properties.get(block_idx) else {
            return false;
        };
        property.offset == offset
            && key_user_part(&property.last_key, self.suffix_len) < lower_bound.as_slice()
    }

    pub async fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            if let Some(entry) = self.current_block_entries.pop_front() {
                return Ok(Some(entry));
            }

            let Some(block_idx) = self.current_block_idx else {
                return Ok(None);
            };
            let offset = self.index_offsets[block_idx];
            self.current_block_idx = block_idx.checked_sub(1);

            if self.block_before_lower_bound(block_idx, offset) {
                self.current_block_idx = None;
                return Ok(None);
            }

            if self.key_at_or_after_upper_bound(&self.index_keys[block_idx]) {
                continue;
            }

            if let Some(probe) = self.block_table_prefix_probe(block_idx, offset) {
                monitor::inc_sstable_block_prefix_filter_check();
                match probe {
                    SsTablePrefixFilterProbe::MayMatch => {
                        monitor::inc_sstable_block_prefix_filter_positive();
                    }
                    SsTablePrefixFilterProbe::NoMatch => {
                        monitor::inc_sstable_block_prefix_filter_skip();
                        continue;
                    }
                    SsTablePrefixFilterProbe::FailOpen => {
                        monitor::inc_sstable_block_prefix_filter_fail_open();
                    }
                }
            }
            if let Some(probe) = self.block_sql_index_prefix_probe(block_idx, offset) {
                monitor::inc_sstable_block_index_prefix_filter_check();
                match probe {
                    SsTablePrefixFilterProbe::MayMatch => {
                        monitor::inc_sstable_block_index_prefix_filter_positive();
                    }
                    SsTablePrefixFilterProbe::NoMatch => {
                        monitor::inc_sstable_block_index_prefix_filter_skip();
                        continue;
                    }
                    SsTablePrefixFilterProbe::FailOpen => {
                        monitor::inc_sstable_block_index_prefix_filter_fail_open();
                    }
                }
            }

            let block_data = SsTable::read_block_at_with_reusable_file(
                &self.path,
                &self.block_cache,
                self.sst_id,
                &self.index_offsets,
                self.file_len,
                offset,
                self.read_options,
                Some(&mut self.file),
            )
            .await?;
            monitor::inc_sstable_reverse_block_read();

            let sidecar = SsTable::reverse_seek_sidecar_for_iterator(
                &self.path,
                self.reverse_seek_fingerprint,
                &self.index_offsets,
                &self.reverse_seek_sidecar,
            )
            .await;
            let stats = if let Some(sidecar) = sidecar {
                if let Some(seek_block) = sidecar.block_for_offset(offset) {
                    if let Some(stats) = SsTable::append_reverse_block_entries_with_seek_index(
                        block_data.as_ref(),
                        seek_block,
                        self.user_key_lower_bound.as_deref(),
                        self.user_key_upper_bound.as_deref(),
                        self.suffix_len,
                        &mut self.current_block_entries,
                    ) {
                        monitor::inc_sstable_reverse_seek_sidecar_use();
                        stats
                    } else {
                        monitor::inc_sstable_reverse_seek_sidecar_fail_open();
                        SsTable::append_reverse_block_entries_in_bounds(
                            block_data.as_ref(),
                            self.user_key_lower_bound.as_deref(),
                            self.user_key_upper_bound.as_deref(),
                            self.suffix_len,
                            &mut self.current_block_entries,
                        )?
                    }
                } else {
                    monitor::inc_sstable_reverse_seek_sidecar_fail_open();
                    SsTable::append_reverse_block_entries_in_bounds(
                        block_data.as_ref(),
                        self.user_key_lower_bound.as_deref(),
                        self.user_key_upper_bound.as_deref(),
                        self.suffix_len,
                        &mut self.current_block_entries,
                    )?
                }
            } else {
                SsTable::append_reverse_block_entries_in_bounds(
                    block_data.as_ref(),
                    self.user_key_lower_bound.as_deref(),
                    self.user_key_upper_bound.as_deref(),
                    self.suffix_len,
                    &mut self.current_block_entries,
                )?
            };
            monitor::add_sstable_reverse_block_entry_decodes(stats.decoded_entries);
            monitor::add_sstable_reverse_block_entry_yields(stats.yielded_entries);
            monitor::add_sstable_reverse_block_span_scans(stats.span_scan_blocks);
            monitor::add_sstable_reverse_block_span_scan_entries(stats.span_scan_entries);
            monitor::add_sstable_reverse_block_span_materialize_entries(
                stats.span_materialized_entries,
            );
            monitor::add_sstable_reverse_seek_sidecar_index_entries(stats.sidecar_index_entries);
            monitor::add_sstable_reverse_seek_sidecar_entry_materializes(
                stats.sidecar_materialized_entries,
            );
            monitor::add_sstable_reverse_seek_sidecar_offset_probes(stats.sidecar_offset_probes);
        }
    }
}

#[cfg(test)]
mod tests {
    /// BENCHPROD-468: filters must be sized to the real entry count. A
    /// saturated bloom (fixed 100k capacity under a 32MB memtable's ~300k+
    /// entries) degrades to ~100% false positives, turning every absent-key
    /// point probe into real block reads — O(sstable count) per get.
    #[test]
    fn filter_sized_to_entry_count_keeps_false_positives_low() {
        let mut sized = super::SsTableBuilder::new(std::path::PathBuf::from("unused.sst"));
        sized.set_expected_filter_items(300_000);
        let mut saturated = super::SsTableBuilder::new(std::path::PathBuf::from("unused2.sst"));
        // saturated keeps the 100k default

        for i in 0..300_000u64 {
            let key = format!("data:ev:{i:016x}AAAAAAAAA");
            sized.add_key(key.as_bytes());
            saturated.add_key(key.as_bytes());
        }

        let absent_probes = 2_000u64;
        let mut sized_fp = 0u32;
        let mut saturated_fp = 0u32;
        for i in 0..absent_probes {
            let key = format!("data:ev:{:016x}AAAAAAAAA", 10_000_000 + i);
            if sized.filter.contains(key.as_bytes()) {
                sized_fp += 1;
            }
            if saturated.filter.contains(key.as_bytes()) {
                saturated_fp += 1;
            }
        }

        assert!(
            sized_fp < 100,
            "correctly sized filter must stay near its 1% design FP rate, got {sized_fp}/2000"
        );
        // At 3x overload the measured FP rate is already ~37% (vs the 1%
        // design point); production memtables overloaded 5x+ and measured
        // ~100%. Anything above 20% here demonstrates the failure mode.
        assert!(
            saturated_fp > 400,
            "regression guard expects the saturated default to be badly degraded, got {saturated_fp}/2000"
        );
    }

    use crate::catalog::{Column, IndexType, TableSchema};
    use crate::common::{encoding::RowEncoder, Value};
    use crate::storage::{SQL_BLOCK_ZONE_MAP_TYPE_BOOLEAN, SQL_BLOCK_ZONE_MAP_TYPE_TIMESTAMP};

    use super::{
        block_entry_buffer, block_entry_reserve_count, block_table_prefix_ranges, prefix_end,
        BlockSqlIndexPrefixesSsTableBlockPropertiesV4, BlockSqlIndexPrefixesSsTableMetaV4,
        BlockSqlZoneMapV5, BlockSqlZoneMapsSsTableBlockPropertiesV5, BlockSqlZoneMapsSsTableMetaV5,
        BlockTablePrefixesSsTableBlockProperties, BlockTablePrefixesSsTableMetaV3, Crc32Hasher,
        LegacyBlockPropertiesSsTableMeta, LegacySsTableBlockProperties, LegacySsTableMeta, SsTable,
        SsTableBlockProperties, SsTableBuilder, SsTableFilterBlock, SsTableIndexCacheFingerprint,
        SsTableOpenDescriptor, SsTablePrefixFilterProbe, SsTableReadOptions,
        SsTableReverseSeekBlockIndex, SsTableReverseSeekSidecar, SsTableSqlZoneMap,
        COMPRESSED_BLOCK_MAGIC, SSTABLE_INDEX_CACHE_MAGIC, SSTABLE_INDEX_MAGIC, SSTABLE_META_MAGIC,
        SSTABLE_META_VERSION_BLOCK_SQL_INDEX_PREFIXES, SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS,
        SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS_V5, SSTABLE_META_VERSION_BLOCK_TABLE_PREFIXES,
        SSTABLE_REVERSE_SEEK_MAGIC,
    };
    use fastbloom::BloomFilter;
    use moka::sync::Cache;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    #[test]
    fn block_entry_buffer_preallocates_first_entry() {
        assert!(block_entry_buffer().capacity() >= 1);
    }

    #[test]
    fn block_entry_reserve_count_is_bounded_by_block_length() {
        assert_eq!(block_entry_reserve_count(3, 24), 3);
        assert_eq!(block_entry_reserve_count(100, 16), 2);
    }

    #[test]
    fn block_property_user_key_interval_strips_suffix_and_rejects_invalid_bounds() {
        let property = SsTableBlockProperties {
            offset: 0,
            first_key: b"data:t:001\0\0\0\0\0\0\0\x05".to_vec(),
            last_key: b"data:t:009\0\0\0\0\0\0\0\x01".to_vec(),
            entry_count: 2,
            table_prefixes: Vec::new(),
            table_prefix_ranges_complete: false,
            table_prefix_ranges: Vec::new(),
            sql_index_prefixes_complete: false,
            sql_index_prefixes: Vec::new(),
            sql_zone_maps_complete: false,
            sql_zone_maps: Vec::new(),
        };
        assert_eq!(
            SsTable::block_property_user_key_interval(&property, 8),
            Some((b"data:t:001".to_vec(), b"data:t:009".to_vec()))
        );

        let mut short = property.clone();
        short.first_key = b"short".to_vec();
        assert_eq!(SsTable::block_property_user_key_interval(&short, 8), None);

        let mut inverted = property;
        inverted.first_key = b"data:t:009\0\0\0\0\0\0\0\x05".to_vec();
        inverted.last_key = b"data:t:001\0\0\0\0\0\0\0\x01".to_vec();
        assert_eq!(
            SsTable::block_property_user_key_interval(&inverted, 8),
            None
        );
    }

    #[test]
    fn block_table_prefix_ranges_keep_data_interval_separate_from_schema_tail() {
        let mut block = Vec::new();
        append_block_entry(&mut block, b"data:metrics:001\0\0\0\0\0\0\0\x03", b"row1");
        append_block_entry(&mut block, b"data:metrics:009\0\0\0\0\0\0\0\x02", b"row2");
        append_block_entry(&mut block, b"schema:metrics\0\0\0\0\0\0\0\x01", b"schema");

        let (complete, ranges) = block_table_prefix_ranges(&block, 3, Some(8));
        assert!(complete);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].table_prefix, b"data:metrics:".to_vec());
        assert_eq!(ranges[0].first_user_key, b"data:metrics:001".to_vec());
        assert_eq!(ranges[0].last_user_key, b"data:metrics:009".to_vec());
    }

    #[test]
    fn reverse_block_bounds_materialize_only_needed_entries() {
        let mut block = Vec::new();
        block.extend_from_slice(&100u32.to_le_bytes());
        for idx in 0..100 {
            let key = format!("k{idx:03}");
            let value = format!("value-{idx:03}");
            append_block_entry(&mut block, key.as_bytes(), value.as_bytes());
        }

        let mut out = std::collections::VecDeque::new();
        let stats = SsTable::append_reverse_block_entries_in_bounds(
            &block,
            Some(b"k090"),
            Some(b"k095"),
            0,
            &mut out,
        )
        .unwrap();

        assert_eq!(stats.decoded_entries, 5);
        assert_eq!(stats.yielded_entries, 5);
        assert_eq!(stats.span_scan_blocks, 1);
        assert_eq!(stats.span_scan_entries, 100);
        assert_eq!(stats.span_materialized_entries, 5);
        assert_eq!(stats.sidecar_index_entries, 0);
        assert_eq!(stats.sidecar_materialized_entries, 0);
        assert_eq!(stats.sidecar_offset_probes, 0);
        let keys: Vec<Vec<u8>> = out.into_iter().map(|(key, _)| key).collect();
        assert_eq!(
            keys,
            vec![
                b"k094".to_vec(),
                b"k093".to_vec(),
                b"k092".to_vec(),
                b"k091".to_vec(),
                b"k090".to_vec(),
            ]
        );
    }

    #[test]
    fn block_decoder_rejects_declared_entries_that_are_missing() {
        let mut block = Vec::new();
        block.extend_from_slice(&2u32.to_le_bytes());
        append_block_entry(&mut block, b"k001", b"value-1");

        let error = SsTable::decoded_block_entry_spans(&block).unwrap_err();
        assert!(error.to_string().contains("entry 1"));
    }

    #[test]
    fn block_decoder_rejects_trailing_bytes_after_declared_entries() {
        let mut block = Vec::new();
        block.extend_from_slice(&1u32.to_le_bytes());
        append_block_entry(&mut block, b"k001", b"value-1");
        block.extend_from_slice(b"unclaimed");

        let error = SsTable::decoded_block_entry_spans(&block).unwrap_err();
        assert!(error.to_string().contains("trailing bytes"));
    }

    #[tokio::test]
    async fn reverse_iterator_uses_persisted_reverse_seek_sidecar() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_reverse_seek_sidecar_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let sidecar_path = SsTable::reverse_seek_path(&path);
        let mut builder = SsTableBuilder::new(path.clone());
        let mut block = Vec::new();
        for idx in 0..100 {
            let key = format!("k{idx:03}");
            builder.add_key(key.as_bytes());
            append_block_entry(&mut block, key.as_bytes(), key.as_bytes());
        }
        builder
            .flush_block(b"k000".to_vec(), 100, &block)
            .await
            .unwrap();
        builder.finish().await.unwrap();
        assert!(
            sidecar_path.exists(),
            "SSTable finish should persist reverse seek sidecar"
        );

        let metrics = &crate::monitor::GLOBAL_METRICS;
        let hits_before = metrics
            .sstable_reverse_seek_sidecar_hit_count
            .load(std::sync::atomic::Ordering::Relaxed);
        let uses_before = metrics
            .sstable_reverse_seek_sidecar_use_count
            .load(std::sync::atomic::Ordering::Relaxed);
        let fail_open_before = metrics
            .sstable_reverse_seek_sidecar_fail_open_count
            .load(std::sync::atomic::Ordering::Relaxed);
        let span_scans_before = metrics
            .sstable_reverse_block_span_scan_count
            .load(std::sync::atomic::Ordering::Relaxed);
        let span_scan_entries_before = metrics
            .sstable_reverse_block_span_scan_entry_count
            .load(std::sync::atomic::Ordering::Relaxed);
        let offset_probes_before = metrics
            .sstable_reverse_seek_sidecar_offset_probe_count
            .load(std::sync::atomic::Ordering::Relaxed);
        let sidecar_index_entries_before = metrics
            .sstable_reverse_seek_sidecar_index_entry_count
            .load(std::sync::atomic::Ordering::Relaxed);
        let sidecar_materializes_before = metrics
            .sstable_reverse_seek_sidecar_entry_materialize_count
            .load(std::sync::atomic::Ordering::Relaxed);
        let span_materializes_before = metrics
            .sstable_reverse_block_span_materialize_entry_count
            .load(std::sync::atomic::Ordering::Relaxed);

        let table = SsTable::open(path.clone(), 41, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        let mut iter = table
            .new_user_key_range_reverse_iterator(Some(b"k090"), Some(b"k095"), 0)
            .await
            .unwrap();
        let mut keys = Vec::new();
        while let Some((key, _value)) = iter.next().await.unwrap() {
            keys.push(key);
        }

        assert_eq!(
            keys,
            vec![
                b"k094".to_vec(),
                b"k093".to_vec(),
                b"k092".to_vec(),
                b"k091".to_vec(),
                b"k090".to_vec(),
            ]
        );
        assert!(
            metrics
                .sstable_reverse_seek_sidecar_hit_count
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(hits_before)
                >= 1
        );
        assert!(
            metrics
                .sstable_reverse_seek_sidecar_use_count
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(uses_before)
                >= 1
        );
        assert_eq!(
            metrics
                .sstable_reverse_seek_sidecar_fail_open_count
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(fail_open_before),
            0
        );
        assert_eq!(
            metrics
                .sstable_reverse_block_span_scan_count
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(span_scans_before),
            0
        );
        assert_eq!(
            metrics
                .sstable_reverse_block_span_scan_entry_count
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(span_scan_entries_before),
            0
        );
        assert_eq!(
            metrics
                .sstable_reverse_block_span_materialize_entry_count
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(span_materializes_before),
            0
        );
        assert!(
            metrics
                .sstable_reverse_seek_sidecar_index_entry_count
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(sidecar_index_entries_before)
                >= 100
        );
        assert_eq!(
            metrics
                .sstable_reverse_seek_sidecar_entry_materialize_count
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(sidecar_materializes_before),
            5
        );
        assert!(
            metrics
                .sstable_reverse_seek_sidecar_offset_probe_count
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(offset_probes_before)
                > 0
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&sidecar_path);
    }

    fn append_crc(mut payload: Vec<u8>) -> Vec<u8> {
        let mut hasher = Crc32Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();
        payload.extend_from_slice(&crc.to_le_bytes());
        payload
    }

    fn append_block_entry(block: &mut Vec<u8>, key: &[u8], value: &[u8]) {
        block.extend_from_slice(&(key.len() as u32).to_le_bytes());
        block.extend_from_slice(key);
        block.extend_from_slice(&(value.len() as u32).to_le_bytes());
        block.extend_from_slice(value);
    }

    fn zone_map_test_schema() -> TableSchema {
        TableSchema::new(
            "metrics".to_string(),
            vec![
                Column {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    is_primary: true,
                    is_indexed: true,
                    index_type: IndexType::BTree,
                    default_value: None,
                    is_nullable: false,
                    is_unique: true,
                    check_expr: None,
                },
                Column {
                    name: "bucket".to_string(),
                    data_type: "INTEGER".to_string(),
                    is_primary: false,
                    is_indexed: false,
                    index_type: IndexType::None,
                    default_value: None,
                    is_nullable: true,
                    is_unique: false,
                    check_expr: None,
                },
                Column {
                    name: "flag".to_string(),
                    data_type: "BOOLEAN".to_string(),
                    is_primary: false,
                    is_indexed: false,
                    index_type: IndexType::None,
                    default_value: None,
                    is_nullable: true,
                    is_unique: false,
                    check_expr: None,
                },
                Column {
                    name: "ts".to_string(),
                    data_type: "TIMESTAMP".to_string(),
                    is_primary: false,
                    is_indexed: false,
                    index_type: IndexType::None,
                    default_value: None,
                    is_nullable: true,
                    is_unique: false,
                    check_expr: None,
                },
                Column {
                    name: "payload".to_string(),
                    data_type: "TEXT".to_string(),
                    is_primary: false,
                    is_indexed: false,
                    index_type: IndexType::None,
                    default_value: None,
                    is_nullable: true,
                    is_unique: false,
                    check_expr: None,
                },
            ],
        )
    }

    fn encoded_put_row(row: &[Value]) -> Vec<u8> {
        let mut value = Vec::from([1u8]);
        value.extend_from_slice(&RowEncoder::encode(row));
        value
    }

    #[test]
    fn sql_zone_map_schema_fingerprint_is_stable_for_type_case_and_sensitive_to_order() {
        let schema = zone_map_test_schema();
        let mut case_variant = schema.clone();
        case_variant.columns[1].data_type = " integer ".to_string();
        assert_eq!(
            super::stable_schema_fingerprint(&schema),
            super::stable_schema_fingerprint(&case_variant)
        );

        let mut reordered = schema.clone();
        reordered.columns.swap(0, 1);
        assert_ne!(
            super::stable_schema_fingerprint(&schema),
            super::stable_schema_fingerprint(&reordered)
        );
    }

    #[test]
    fn compressed_block_payload_round_trips_to_legacy_layout() {
        let entries = vec![b'x'; 4096];
        let encoded = SsTable::encode_block_payload(3, &entries);

        assert_eq!(
            &encoded[..COMPRESSED_BLOCK_MAGIC.len()],
            COMPRESSED_BLOCK_MAGIC
        );

        let decoded = SsTable::decode_block_payload(&append_crc(encoded)).unwrap();
        assert_eq!(u32::from_le_bytes(decoded[..4].try_into().unwrap()), 3);
        assert_eq!(&decoded[4..], entries.as_slice());
    }

    #[test]
    fn legacy_block_payload_stays_readable() {
        let entries = b"not worth compressing".to_vec();
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&1u32.to_le_bytes());
        legacy.extend_from_slice(&entries);

        let decoded = SsTable::decode_block_payload(&append_crc(legacy.clone())).unwrap();

        assert_eq!(decoded, legacy);
    }

    #[test]
    fn btree_index_block_decodes_as_ordered_runtime_vectors() {
        let mut index: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        index.insert(b"k100".to_vec(), 128);
        index.insert(b"k001".to_vec(), 0);
        index.insert(b"k200".to_vec(), 256);
        let bytes = bincode::serialize(&index).unwrap();

        let (keys, offsets) = SsTable::decode_index_block(&bytes).unwrap();

        assert_eq!(
            keys,
            vec![b"k001".to_vec(), b"k100".to_vec(), b"k200".to_vec()]
        );
        assert_eq!(offsets, vec![0, 128, 256]);
    }

    #[test]
    fn versioned_flat_index_block_decodes_as_ordered_runtime_vectors() {
        let mut index: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        index.insert(b"k100".to_vec(), 128);
        index.insert(b"k001".to_vec(), 0);
        index.insert(b"k200".to_vec(), 256);
        let bytes = SsTable::encode_index_block(index);

        assert_eq!(&bytes[..SSTABLE_INDEX_MAGIC.len()], SSTABLE_INDEX_MAGIC);
        let (keys, offsets) = SsTable::decode_index_block(&bytes).unwrap();

        assert_eq!(
            keys,
            vec![b"k001".to_vec(), b"k100".to_vec(), b"k200".to_vec()]
        );
        assert_eq!(offsets, vec![0, 128, 256]);
    }

    #[test]
    fn index_cache_round_trips_and_rejects_stale_or_truncated_bytes() {
        let fingerprint = SsTableIndexCacheFingerprint {
            file_len: 4096,
            modified_unix_secs: 123,
            modified_subsec_nanos: 456,
            index_offset: 384,
            filter_offset: 512,
            meta_offset: 768,
            index_len: 128,
        };
        let keys = vec![b"k001".to_vec(), b"k100".to_vec(), b"k200".to_vec()];
        let offsets = vec![0, 128, 256];
        let bytes = SsTable::encode_index_cache(fingerprint, &keys, &offsets);

        assert_eq!(
            &bytes[..SSTABLE_INDEX_CACHE_MAGIC.len()],
            SSTABLE_INDEX_CACHE_MAGIC
        );
        let decoded = SsTable::decode_index_cache(&bytes, fingerprint)
            .unwrap()
            .unwrap();
        assert_eq!(decoded.0, keys);
        assert_eq!(decoded.1, offsets);

        let stale = SsTableIndexCacheFingerprint {
            file_len: 4097,
            ..fingerprint
        };
        assert!(SsTable::decode_index_cache(&bytes, stale)
            .unwrap()
            .is_none());

        let mut truncated = bytes;
        truncated.pop();
        assert!(SsTable::decode_index_cache(&truncated, fingerprint).is_err());
    }

    #[test]
    fn index_cache_rejects_payload_checksum_mismatch() {
        let fingerprint = SsTableIndexCacheFingerprint {
            file_len: 4096,
            modified_unix_secs: 123,
            modified_subsec_nanos: 456,
            index_offset: 384,
            filter_offset: 512,
            meta_offset: 768,
            index_len: 128,
        };
        let keys = vec![b"k001".to_vec(), b"k100".to_vec(), b"k200".to_vec()];
        let offsets = vec![0, 128, 256];
        let mut bytes = SsTable::encode_index_cache(fingerprint, &keys, &offsets);
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;

        assert!(SsTable::decode_index_cache(&bytes, fingerprint).is_err());
    }

    #[test]
    fn index_cache_rejects_offsets_outside_data_blocks_or_not_increasing() {
        let fingerprint = SsTableIndexCacheFingerprint {
            file_len: 4096,
            modified_unix_secs: 123,
            modified_subsec_nanos: 456,
            index_offset: 384,
            filter_offset: 512,
            meta_offset: 768,
            index_len: 128,
        };
        let keys = vec![b"k001".to_vec(), b"k100".to_vec(), b"k200".to_vec()];

        let outside_data = SsTable::encode_index_cache(fingerprint, &keys, &[0, 128, 384]);
        assert!(SsTable::decode_index_cache(&outside_data, fingerprint).is_err());

        let not_increasing = SsTable::encode_index_cache(fingerprint, &keys, &[0, 128, 64]);
        assert!(SsTable::decode_index_cache(&not_increasing, fingerprint).is_err());
    }

    #[test]
    fn reverse_seek_sidecar_round_trips_and_rejects_stale_or_corrupt_bytes() {
        let fingerprint = SsTableIndexCacheFingerprint {
            file_len: 4096,
            modified_unix_secs: 123,
            modified_subsec_nanos: 456,
            index_offset: 384,
            filter_offset: 512,
            meta_offset: 768,
            index_len: 128,
        };
        let sidecar = SsTableReverseSeekSidecar {
            blocks: vec![
                SsTableReverseSeekBlockIndex {
                    block_offset: 0,
                    decoded_len: 64,
                    entry_count: 2,
                    decoded_crc32: 0x1234,
                    entry_offsets: vec![4, 32],
                },
                SsTableReverseSeekBlockIndex {
                    block_offset: 128,
                    decoded_len: 96,
                    entry_count: 3,
                    decoded_crc32: 0x5678,
                    entry_offsets: vec![4, 36, 68],
                },
            ],
        };
        let bytes = SsTable::encode_reverse_seek_sidecar(fingerprint, &sidecar);

        assert_eq!(
            &bytes[..SSTABLE_REVERSE_SEEK_MAGIC.len()],
            SSTABLE_REVERSE_SEEK_MAGIC
        );
        let decoded = SsTable::decode_reverse_seek_sidecar(&bytes, fingerprint, &[0, 128])
            .unwrap()
            .unwrap();
        assert_eq!(decoded, sidecar);

        let stale = SsTableIndexCacheFingerprint {
            file_len: 4097,
            ..fingerprint
        };
        assert!(
            SsTable::decode_reverse_seek_sidecar(&bytes, stale, &[0, 128])
                .unwrap()
                .is_none()
        );

        let mut corrupt = bytes.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0x01;
        assert!(SsTable::decode_reverse_seek_sidecar(&corrupt, fingerprint, &[0, 128]).is_err());

        let bad_offsets = SsTableReverseSeekSidecar {
            blocks: vec![SsTableReverseSeekBlockIndex {
                block_offset: 0,
                decoded_len: 64,
                entry_count: 2,
                decoded_crc32: 0x1234,
                entry_offsets: vec![32, 4],
            }],
        };
        let bad_bytes = SsTable::encode_reverse_seek_sidecar(fingerprint, &bad_offsets);
        assert!(SsTable::decode_reverse_seek_sidecar(&bad_bytes, fingerprint, &[0]).is_err());
    }

    #[test]
    fn legacy_meta_decodes_without_block_properties() {
        let legacy = LegacySsTableMeta {
            first_key: b"k001".to_vec(),
            last_key: b"k999".to_vec(),
        };
        let bytes = bincode::serialize(&legacy).unwrap();

        let meta = SsTable::decode_meta(&bytes).unwrap();

        assert_eq!(meta.first_key, b"k001");
        assert_eq!(meta.last_key, b"k999");
        assert!(meta.block_properties.is_empty());
        assert_eq!(meta.format_version, 0);
    }

    #[test]
    fn block_properties_meta_without_version_decodes_as_v1() {
        let legacy_block_properties = vec![LegacySsTableBlockProperties {
            offset: 0,
            first_key: b"k001".to_vec(),
            last_key: b"k099".to_vec(),
            entry_count: 10,
        }];
        let expected_block_properties = legacy_block_properties
            .clone()
            .into_iter()
            .map(SsTableBlockProperties::from)
            .collect::<Vec<_>>();
        let legacy = LegacyBlockPropertiesSsTableMeta {
            first_key: b"k001".to_vec(),
            last_key: b"k999".to_vec(),
            block_properties: legacy_block_properties,
        };
        let bytes = bincode::serialize(&legacy).unwrap();

        let meta = SsTable::decode_meta(&bytes).unwrap();

        assert_eq!(meta.first_key, b"k001");
        assert_eq!(meta.last_key, b"k999");
        assert_eq!(meta.block_properties, expected_block_properties);
        assert_eq!(meta.format_version, 1);
    }

    #[test]
    fn block_table_prefix_meta_decodes_without_sql_index_prefixes() {
        let legacy = BlockTablePrefixesSsTableMetaV3 {
            first_key: b"k001".to_vec(),
            last_key: b"k999".to_vec(),
            block_properties: vec![BlockTablePrefixesSsTableBlockProperties {
                offset: 0,
                first_key: b"k001".to_vec(),
                last_key: b"k099".to_vec(),
                entry_count: 10,
                table_prefixes: vec![b"data:t:".to_vec()],
            }],
            format_version: SSTABLE_META_VERSION_BLOCK_TABLE_PREFIXES,
        };
        let bytes = bincode::serialize(&legacy).unwrap();

        let meta = SsTable::decode_meta(&bytes).unwrap();

        assert_eq!(meta.first_key, b"k001");
        assert_eq!(meta.last_key, b"k999");
        assert_eq!(meta.block_properties.len(), 1);
        assert_eq!(
            meta.block_properties[0].table_prefixes,
            vec![b"data:t:".to_vec()]
        );
        assert!(!meta.block_properties[0].sql_index_prefixes_complete);
        assert!(meta.block_properties[0].sql_index_prefixes.is_empty());
        assert_eq!(
            meta.format_version,
            SSTABLE_META_VERSION_BLOCK_TABLE_PREFIXES
        );
    }

    #[test]
    fn block_sql_index_prefix_meta_decodes_without_sql_zone_maps() {
        let legacy = BlockSqlIndexPrefixesSsTableMetaV4 {
            first_key: b"k001".to_vec(),
            last_key: b"k999".to_vec(),
            block_properties: vec![BlockSqlIndexPrefixesSsTableBlockPropertiesV4 {
                offset: 0,
                first_key: b"k001".to_vec(),
                last_key: b"k099".to_vec(),
                entry_count: 10,
                table_prefixes: vec![b"data:t:".to_vec()],
                sql_index_prefixes_complete: true,
                sql_index_prefixes: vec![b"index:t:a:i1|".to_vec()],
            }],
            format_version: SSTABLE_META_VERSION_BLOCK_SQL_INDEX_PREFIXES,
        };
        let bytes = bincode::serialize(&legacy).unwrap();

        let meta = SsTable::decode_meta(&bytes).unwrap();

        assert_eq!(meta.first_key, b"k001");
        assert_eq!(meta.last_key, b"k999");
        assert_eq!(meta.block_properties.len(), 1);
        assert_eq!(
            meta.block_properties[0].table_prefixes,
            vec![b"data:t:".to_vec()]
        );
        assert!(meta.block_properties[0].sql_index_prefixes_complete);
        assert_eq!(
            meta.block_properties[0].sql_index_prefixes,
            vec![b"index:t:a:i1|".to_vec()]
        );
        assert!(!meta.block_properties[0].sql_zone_maps_complete);
        assert!(meta.block_properties[0].sql_zone_maps.is_empty());
        assert_eq!(
            meta.format_version,
            SSTABLE_META_VERSION_BLOCK_SQL_INDEX_PREFIXES
        );
    }

    #[test]
    fn block_sql_zone_map_v5_meta_decodes_with_zone_maps() {
        let zone_map = SsTableSqlZoneMap {
            table_prefix: b"data:t:".to_vec(),
            schema_fingerprint: 42,
            column_index: 1,
            column_name: "bucket".to_string(),
            type_tag: 1,
            value_encoding_version: 1,
            min_scalar: 7,
            max_scalar: 9,
            row_count: 10,
            null_count: 0,
            non_null_count: 10,
            put_count: 10,
            tombstone_count: 0,
            bounds_valid: true,
        };
        let meta = BlockSqlZoneMapsSsTableMetaV5 {
            first_key: b"k001".to_vec(),
            last_key: b"k999".to_vec(),
            block_properties: vec![BlockSqlZoneMapsSsTableBlockPropertiesV5 {
                offset: 0,
                first_key: b"k001".to_vec(),
                last_key: b"k099".to_vec(),
                entry_count: 10,
                table_prefixes: vec![b"data:t:".to_vec()],
                sql_index_prefixes_complete: true,
                sql_index_prefixes: vec![b"index:t:a:i1|".to_vec()],
                sql_zone_maps_complete: true,
                sql_zone_maps: vec![BlockSqlZoneMapV5::from(zone_map.clone())],
            }],
            format_version: SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS_V5,
        };
        let bytes = bincode::serialize(&meta).unwrap();

        let decoded = SsTable::decode_meta(&bytes).unwrap();

        assert_eq!(
            decoded.format_version,
            SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS_V5
        );
        assert_eq!(decoded.block_properties.len(), 1);
        assert!(decoded.block_properties[0].sql_zone_maps_complete);
        assert_eq!(decoded.block_properties[0].sql_zone_maps, vec![zone_map]);
    }

    #[test]
    fn block_sql_zone_map_framed_v5_meta_decodes_with_zone_maps() {
        let zone_map = SsTableSqlZoneMap {
            table_prefix: b"data:t:".to_vec(),
            schema_fingerprint: 42,
            column_index: 1,
            column_name: "bucket".to_string(),
            type_tag: 1,
            value_encoding_version: 1,
            min_scalar: 7,
            max_scalar: 9,
            row_count: 10,
            null_count: 0,
            non_null_count: 10,
            put_count: 10,
            tombstone_count: 0,
            bounds_valid: true,
        };
        let meta = BlockSqlZoneMapsSsTableMetaV5 {
            first_key: b"k001".to_vec(),
            last_key: b"k999".to_vec(),
            block_properties: vec![BlockSqlZoneMapsSsTableBlockPropertiesV5 {
                offset: 0,
                first_key: b"k001".to_vec(),
                last_key: b"k099".to_vec(),
                entry_count: 10,
                table_prefixes: vec![b"data:t:".to_vec()],
                sql_index_prefixes_complete: true,
                sql_index_prefixes: vec![b"index:t:a:i1|".to_vec()],
                sql_zone_maps_complete: true,
                sql_zone_maps: vec![BlockSqlZoneMapV5::from(zone_map.clone())],
            }],
            format_version: SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS_V5,
        };
        let bytes =
            SsTable::encode_versioned_meta(SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS_V5, &meta)
                .unwrap();

        let decoded = SsTable::decode_meta(&bytes).unwrap();

        assert_eq!(
            decoded.format_version,
            SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS_V5
        );
        assert_eq!(decoded.block_properties.len(), 1);
        assert!(decoded.block_properties[0].sql_zone_maps_complete);
        assert_eq!(decoded.block_properties[0].sql_zone_maps, vec![zone_map]);
    }

    #[test]
    fn sstable_meta_decode_rejects_trailing_bytes() {
        let legacy = BlockSqlIndexPrefixesSsTableMetaV4 {
            first_key: b"k001".to_vec(),
            last_key: b"k999".to_vec(),
            block_properties: vec![BlockSqlIndexPrefixesSsTableBlockPropertiesV4 {
                offset: 0,
                first_key: b"k001".to_vec(),
                last_key: b"k099".to_vec(),
                entry_count: 10,
                table_prefixes: vec![b"data:t:".to_vec()],
                sql_index_prefixes_complete: true,
                sql_index_prefixes: vec![b"index:t:a:i1|".to_vec()],
            }],
            format_version: SSTABLE_META_VERSION_BLOCK_SQL_INDEX_PREFIXES,
        };
        let mut bytes = bincode::serialize(&legacy).unwrap();
        bytes.extend_from_slice(b"trailing");

        assert!(SsTable::decode_meta(&bytes).is_err());

        let framed_meta = BlockSqlZoneMapsSsTableMetaV5 {
            first_key: b"k001".to_vec(),
            last_key: b"k999".to_vec(),
            block_properties: Vec::new(),
            format_version: SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS_V5,
        };
        let mut framed_bytes = SsTable::encode_versioned_meta(
            SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS_V5,
            &framed_meta,
        )
        .unwrap();
        framed_bytes.extend_from_slice(b"trailing");

        assert!(SsTable::decode_meta(&framed_bytes).is_err());
    }

    #[test]
    fn sstable_meta_decode_rejects_unknown_framed_version() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SSTABLE_META_MAGIC);
        bytes.extend_from_slice(&999u32.to_le_bytes());

        assert!(SsTable::decode_meta(&bytes).is_err());
    }

    #[test]
    fn legacy_filter_block_decodes_without_prefix_filter() {
        let mut filter = BloomFilter::with_false_pos(0.01).expected_items(16);
        filter.insert(b"full-key");
        let bytes = bincode::serialize(&filter).unwrap();

        let (decoded, prefix_filter, user_key_filter, sql_index_prefix_filter) =
            SsTable::decode_filter_block(&bytes).unwrap();

        assert!(decoded.contains(b"full-key"));
        assert!(prefix_filter.is_none());
        assert!(user_key_filter.is_none());
        assert!(sql_index_prefix_filter.is_none());
    }

    #[test]
    fn unknown_filter_wrapper_version_decodes_without_prefix_filter() {
        let mut filter = BloomFilter::with_false_pos(0.01).expected_items(16);
        filter.insert(b"full-key");
        let filter_block = SsTableFilterBlock {
            format_version: u32::MAX,
            whole_key_filter: filter,
            prefix_filter: None,
            user_key_filter: None,
            sql_index_prefix_filter: None,
        };
        let bytes = bincode::serialize(&filter_block).unwrap();

        let (decoded, prefix_filter, user_key_filter, sql_index_prefix_filter) =
            SsTable::decode_filter_block(&bytes).unwrap();

        assert!(decoded.contains(b"full-key"));
        assert!(prefix_filter.is_none());
        assert!(user_key_filter.is_none());
        assert!(sql_index_prefix_filter.is_none());
    }

    #[tokio::test]
    async fn block_cache_hit_reuses_shared_block_bytes() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_block_cache_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        let key = b"k001";
        let value = b"cached-value";
        let mut block_buffer = Vec::new();
        block_buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
        block_buffer.extend_from_slice(key);
        block_buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
        block_buffer.extend_from_slice(value);

        builder.add_key(key);
        builder
            .flush_block(key.to_vec(), 1, &block_buffer)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 1, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        let first = table.read_block(0).await.unwrap();
        let second = table.read_block(0).await.unwrap();

        assert_eq!(first.as_ref(), second.as_ref());
        assert!(
            Arc::ptr_eq(&first, &second),
            "block cache hit should clone Arc metadata, not block bytes"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn no_fill_iterator_reads_blocks_without_populating_block_cache() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_no_fill_cache_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        for key in [b"k000".as_slice(), b"k100".as_slice(), b"k200".as_slice()] {
            let mut block = Vec::new();
            append_block_entry(&mut block, key, key);
            builder.add_key(key);
            builder.flush_block(key.to_vec(), 1, &block).await.unwrap();
        }
        builder.finish().await.unwrap();

        let cache = Arc::new(Cache::new(16));
        let table = SsTable::open(path.clone(), 2, cache).await.unwrap();
        let hot_offset = table.index_offset_for(b"k000").unwrap();
        let scan_offsets = [
            table.index_offset_for(b"k100").unwrap(),
            table.index_offset_for(b"k200").unwrap(),
        ];
        table.read_block(hot_offset).await.unwrap();
        assert!(table.block_cache.get(&(2, hot_offset)).is_some());

        let mut iter = table
            .new_iterator_with_options(Some(b"k100"), SsTableReadOptions::no_fill_cache())
            .await
            .unwrap();
        let mut keys = Vec::new();
        while let Some((key, _value)) = iter.next().await.unwrap() {
            keys.push(key);
        }

        assert_eq!(keys, vec![b"k100".to_vec(), b"k200".to_vec()]);
        assert!(table.block_cache.get(&(2, hot_offset)).is_some());
        for offset in scan_offsets {
            assert!(
                table.block_cache.get(&(2, offset)).is_none(),
                "no-fill scan should not populate cold scan blocks"
            );
        }

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn no_fill_reverse_iterator_reads_blocks_without_populating_block_cache() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_reverse_no_fill_cache_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        for key in [b"k000".as_slice(), b"k100".as_slice(), b"k200".as_slice()] {
            let mut block = Vec::new();
            append_block_entry(&mut block, key, key);
            builder.add_key(key);
            builder.flush_block(key.to_vec(), 1, &block).await.unwrap();
        }
        builder.finish().await.unwrap();

        let cache = Arc::new(Cache::new(16));
        let table = SsTable::open(path.clone(), 22, cache).await.unwrap();
        let hot_offset = table.index_offset_for(b"k000").unwrap();
        let scan_offsets = [
            table.index_offset_for(b"k100").unwrap(),
            table.index_offset_for(b"k200").unwrap(),
        ];
        table.read_block(hot_offset).await.unwrap();
        assert!(table.block_cache.get(&(22, hot_offset)).is_some());

        let mut iter = table
            .new_user_key_range_reverse_iterator_with_options(
                None,
                None,
                0,
                SsTableReadOptions::no_fill_cache(),
            )
            .await
            .unwrap();
        let mut keys = Vec::new();
        while let Some((key, _value)) = iter.next().await.unwrap() {
            keys.push(key);
        }

        assert_eq!(
            keys,
            vec![b"k200".to_vec(), b"k100".to_vec(), b"k000".to_vec()]
        );
        assert!(table.block_cache.get(&(22, hot_offset)).is_some());
        for offset in scan_offsets {
            assert!(
                table.block_cache.get(&(22, offset)).is_none(),
                "reverse no-fill scan should not populate cold scan blocks"
            );
        }

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn no_fill_read_block_option_skips_insert_but_still_uses_existing_hit() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_no_fill_direct_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        let key = b"k001";
        let mut block = Vec::new();
        append_block_entry(&mut block, key, b"value");
        builder.add_key(key);
        builder.flush_block(key.to_vec(), 1, &block).await.unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 3, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        let offset = table.index_offset_for(key.as_slice()).unwrap();

        let block = table
            .read_block_with_options(offset, SsTableReadOptions::no_fill_cache())
            .await
            .unwrap();
        assert!(block
            .as_ref()
            .windows(key.len())
            .any(|window| window == key));
        assert!(table.block_cache.get(&(3, offset)).is_none());

        table.read_block(offset).await.unwrap();
        assert!(table.block_cache.get(&(3, offset)).is_some());
        table
            .read_block_with_options(offset, SsTableReadOptions::no_fill_cache())
            .await
            .unwrap();
        assert!(table.block_cache.get(&(3, offset)).is_some());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn forward_iterator_skips_only_fusion_approved_block_offsets() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_approved_block_skip_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());

        let mut first_block = Vec::new();
        for key in [b"k001".as_slice(), b"k002".as_slice()] {
            builder.add_key(key);
            append_block_entry(&mut first_block, key, key);
        }
        builder
            .flush_block(b"k001".to_vec(), 2, &first_block)
            .await
            .unwrap();

        let mut second_block = Vec::new();
        for key in [b"k100".as_slice(), b"k101".as_slice()] {
            builder.add_key(key);
            append_block_entry(&mut second_block, key, key);
        }
        builder
            .flush_block(b"k100".to_vec(), 2, &second_block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 43, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        let block_properties = table
            .validated_block_properties_for_zone_maps()
            .expect("fresh v4/v5 metadata should align block offsets");
        assert_eq!(block_properties.len(), 2);

        let mut skip_offsets = BTreeSet::new();
        skip_offsets.insert(block_properties[0].offset);
        let mut iter = table
            .new_user_key_range_iterator_with_options_and_block_skips(
                None,
                Some(b"k999"),
                0,
                SsTableReadOptions::fill_cache(),
                Some(Arc::new(skip_offsets)),
            )
            .await
            .unwrap();

        let mut keys = Vec::new();
        while let Some((key, _value)) = iter.next().await.unwrap() {
            keys.push(key);
        }
        assert_eq!(keys, vec![b"k100".to_vec(), b"k101".to_vec()]);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sstable_compresses_repetitive_blocks_and_reads_entries() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_compress_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        let mut block_buffer = Vec::new();
        let mut first_key = None;
        let mut expected_value = Vec::new();
        expected_value.extend(std::iter::repeat(b'a').take(512));

        for id in 0..64 {
            let key = format!("k{id:03}").into_bytes();
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            builder.add_key(&key);
            block_buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
            block_buffer.extend_from_slice(&key);
            block_buffer.extend_from_slice(&(expected_value.len() as u32).to_le_bytes());
            block_buffer.extend_from_slice(&expected_value);
        }

        builder
            .flush_block(first_key.unwrap(), 64, &block_buffer)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let mut file = tokio::fs::File::open(&path).await.unwrap();
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic).await.unwrap();
        assert_eq!(&magic, COMPRESSED_BLOCK_MAGIC);

        let table = SsTable::open(path.clone(), 1, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        let found = table.find_ge(b"k050").await.unwrap().unwrap();
        assert_eq!(found.0, b"k050");
        assert_eq!(found.1, expected_value);

        let mut iter = table.new_iterator(None).await.unwrap();
        let mut count = 0;
        while let Some((_key, value)) = iter.next().await.unwrap() {
            assert_eq!(value.len(), 512);
            count += 1;
        }
        assert_eq!(count, 64);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn range_iterator_upper_bound_stops_before_next_block() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_upper_bound_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());

        let mut first_block = Vec::new();
        for key in [b"k001".as_slice(), b"k050".as_slice(), b"k060".as_slice()] {
            builder.add_key(key);
            first_block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            first_block.extend_from_slice(key);
            first_block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            first_block.extend_from_slice(key);
        }
        builder
            .flush_block(b"k001".to_vec(), 3, &first_block)
            .await
            .unwrap();

        let mut second_block = Vec::new();
        for key in [b"k100".as_slice(), b"k101".as_slice()] {
            builder.add_key(key);
            second_block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            second_block.extend_from_slice(key);
            second_block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            second_block.extend_from_slice(key);
        }
        builder
            .flush_block(b"k100".to_vec(), 2, &second_block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let cache = Arc::new(Cache::new(16));
        let table = SsTable::open(path.clone(), 99, cache).await.unwrap();
        let second_offset = table.index_offset_for(b"k100").unwrap();

        let mut iter = table.new_range_iterator(None, Some(b"k050")).await.unwrap();
        let mut keys = Vec::new();
        while let Some((key, _value)) = iter.next().await.unwrap() {
            keys.push(key);
        }

        assert_eq!(keys, vec![b"k001".to_vec()]);
        assert!(
            table.block_cache.get(&(99, second_offset)).is_none(),
            "upper bound should prevent reading the next block"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn range_iterator_lower_bound_skips_previous_block_with_block_properties() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_lower_bound_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());

        let mut first_block = Vec::new();
        for key in [b"k001".as_slice(), b"k050".as_slice(), b"k060".as_slice()] {
            builder.add_key(key);
            first_block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            first_block.extend_from_slice(key);
            first_block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            first_block.extend_from_slice(key);
        }
        builder
            .flush_block(b"k001".to_vec(), 3, &first_block)
            .await
            .unwrap();

        let mut second_block = Vec::new();
        for key in [b"k100".as_slice(), b"k101".as_slice()] {
            builder.add_key(key);
            second_block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            second_block.extend_from_slice(key);
            second_block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            second_block.extend_from_slice(key);
        }
        builder
            .flush_block(b"k100".to_vec(), 2, &second_block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 101, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        let first_offset = table.index_offset_for(b"k001").unwrap();
        assert_eq!(
            table.meta.format_version,
            SSTABLE_META_VERSION_BLOCK_SQL_INDEX_PREFIXES
        );
        let block_properties = table
            .block_properties
            .get()
            .expect("block properties should be eager for full open");
        assert_eq!(block_properties.len(), 2);
        assert_eq!(block_properties[0].last_key, b"k060");

        let mut iter = table.new_range_iterator(Some(b"k075"), None).await.unwrap();
        let mut keys = Vec::new();
        while let Some((key, _value)) = iter.next().await.unwrap() {
            keys.push(key);
        }

        assert_eq!(keys, vec![b"k100".to_vec(), b"k101".to_vec()]);
        assert!(
            table.block_cache.get(&(101, first_offset)).is_none(),
            "lower bound should skip blocks whose last key is before the start key"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn open_with_descriptor_defers_block_properties_until_preload() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_lazy_descriptor_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());

        let mut first_block = Vec::new();
        for key in [b"k001".as_slice(), b"k050".as_slice()] {
            builder.add_key(key);
            first_block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            first_block.extend_from_slice(key);
            first_block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            first_block.extend_from_slice(key);
        }
        builder
            .flush_block(b"k001".to_vec(), 2, &first_block)
            .await
            .unwrap();

        let mut second_block = Vec::new();
        for key in [b"k100".as_slice(), b"k101".as_slice()] {
            builder.add_key(key);
            second_block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            second_block.extend_from_slice(key);
            second_block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            second_block.extend_from_slice(key);
        }
        builder
            .flush_block(b"k100".to_vec(), 2, &second_block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let full = SsTable::open(path.clone(), 104, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        let descriptor = SsTableOpenDescriptor {
            first_key: full.meta.first_key.clone(),
            last_key: full.meta.last_key.clone(),
            format_version: full.meta.format_version,
        };

        let lazy = SsTable::open_with_descriptor(
            path.clone(),
            105,
            Arc::new(Cache::new(16)),
            Some(descriptor),
        )
        .await
        .unwrap();
        assert_eq!(lazy.meta.first_key, b"k001");
        assert_eq!(lazy.meta.last_key, b"k101");
        assert!(
            lazy.block_properties.get().is_none(),
            "descriptor open should skip block properties decode"
        );
        assert_eq!(lazy.get(b"k100").await.unwrap(), Some(b"k100".to_vec()));

        lazy.preload_block_properties().await;
        let block_properties = lazy
            .block_properties
            .get()
            .expect("preload should decode block properties");
        assert_eq!(block_properties.len(), 2);
        assert_eq!(block_properties[1].first_key, b"k100");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn user_key_range_reverse_iterator_bounds_skip_outside_blocks() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_reverse_bounds_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());

        let mut first_block = Vec::new();
        for key in [b"k001".as_slice(), b"k050".as_slice(), b"k060".as_slice()] {
            builder.add_key(key);
            append_block_entry(&mut first_block, key, key);
        }
        builder
            .flush_block(b"k001".to_vec(), 3, &first_block)
            .await
            .unwrap();

        let mut second_block = Vec::new();
        for key in [b"k100".as_slice(), b"k101".as_slice()] {
            builder.add_key(key);
            append_block_entry(&mut second_block, key, key);
        }
        builder
            .flush_block(b"k100".to_vec(), 2, &second_block)
            .await
            .unwrap();

        let mut third_block = Vec::new();
        for key in [b"k200".as_slice(), b"k201".as_slice()] {
            builder.add_key(key);
            append_block_entry(&mut third_block, key, key);
        }
        builder
            .flush_block(b"k200".to_vec(), 2, &third_block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let cache = Arc::new(Cache::new(16));
        let table = SsTable::open(path.clone(), 105, cache).await.unwrap();
        let first_offset = table.index_offset_for(b"k001").unwrap();
        let third_offset = table.index_offset_for(b"k200").unwrap();

        let mut iter = table
            .new_user_key_range_reverse_iterator(Some(b"k075"), Some(b"k200"), 0)
            .await
            .unwrap();
        let mut keys = Vec::new();
        while let Some((key, _value)) = iter.next().await.unwrap() {
            keys.push(key);
        }

        assert_eq!(keys, vec![b"k101".to_vec(), b"k100".to_vec()]);
        assert!(
            table.block_cache.get(&(105, first_offset)).is_none(),
            "reverse lower bound should stop before blocks whose last key is below the lower bound"
        );
        assert!(
            table.block_cache.get(&(105, third_offset)).is_none(),
            "reverse upper bound should start before blocks whose first key is at the upper bound"
        );

        let mut inclusive_lower_iter = table
            .new_user_key_range_reverse_iterator(Some(b"k050"), Some(b"k200"), 0)
            .await
            .unwrap();
        let mut inclusive_keys = Vec::new();
        while let Some((key, _value)) = inclusive_lower_iter.next().await.unwrap() {
            inclusive_keys.push(key);
        }
        assert_eq!(
            inclusive_keys,
            vec![
                b"k101".to_vec(),
                b"k100".to_vec(),
                b"k060".to_vec(),
                b"k050".to_vec(),
            ]
        );

        let mut empty_iter = table
            .new_user_key_range_reverse_iterator(Some(b"k100"), Some(b"k100"), 0)
            .await
            .unwrap();
        assert!(empty_iter.next().await.unwrap().is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn reverse_frontier_user_key_for_range_uses_in_range_block_properties() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_reverse_frontier_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());

        let mut first_block = Vec::new();
        for key in [b"k001".as_slice(), b"k050".as_slice(), b"k060".as_slice()] {
            builder.add_key(key);
            append_block_entry(&mut first_block, key, key);
        }
        builder
            .flush_block(b"k001".to_vec(), 3, &first_block)
            .await
            .unwrap();

        let mut second_block = Vec::new();
        for key in [b"k100".as_slice(), b"k101".as_slice()] {
            builder.add_key(key);
            append_block_entry(&mut second_block, key, key);
        }
        builder
            .flush_block(b"k100".to_vec(), 2, &second_block)
            .await
            .unwrap();

        let mut third_block = Vec::new();
        for key in [b"k200".as_slice(), b"k201".as_slice()] {
            builder.add_key(key);
            append_block_entry(&mut third_block, key, key);
        }
        builder
            .flush_block(b"k200".to_vec(), 2, &third_block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 106, Arc::new(Cache::new(16)))
            .await
            .unwrap();

        assert_eq!(
            table.reverse_frontier_user_key_for_range(b"k075", b"k200", 0),
            Some(b"k101".to_vec()),
            "frontier should use the highest in-range block last key"
        );
        assert_eq!(
            table.reverse_frontier_user_key_for_range(b"k075", b"k100", 0),
            None,
            "block properties should prove that no block overlaps this range"
        );
        assert_eq!(
            table.reverse_frontier_user_key_for_range(b"k100", b"k101", 0),
            Some(b"k101".to_vec()),
            "exclusive upper bound is a safe frontier when the block may contain keys below it"
        );
        assert_eq!(
            table.reverse_frontier_user_key_for_range(b"k000", b"k999", 0),
            Some(b"k201".to_vec()),
            "wide ranges can still use the table/block max key"
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(SsTable::reverse_seek_path(&path));
    }

    #[tokio::test]
    async fn versioned_filter_block_supports_user_key_table_prefix_negative_checks() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_prefix_filter_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);

        let mut block = Vec::new();
        let keys = [b"data:alpha:001".as_slice(), b"data:alpha:002".as_slice()];
        for user_key in keys {
            let mut key = user_key.to_vec();
            key.extend_from_slice(&[0xff; 8]);
            builder.add_key(&key);
            block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            block.extend_from_slice(&key);
            block.extend_from_slice(&(user_key.len() as u32).to_le_bytes());
            block.extend_from_slice(user_key);
        }
        let mut first_key = b"data:alpha:001".to_vec();
        first_key.extend_from_slice(&[0xff; 8]);
        builder
            .flush_block(first_key.clone(), 2, &block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 102, Arc::new(Cache::new(16)))
            .await
            .unwrap();

        assert!(table.get(&first_key).await.unwrap().is_some());
        assert!(table.prefix_may_match(b"data:alpha:"));
        assert!(table.prefix_may_match(b"data:alpha:001"));
        assert!(!table.prefix_may_match(b"data:beta:"));
        assert!(table.prefix_may_match(b"schema:alpha"));
        assert_eq!(
            table.probe_user_key_prefix_filter(b"data:alpha:"),
            SsTablePrefixFilterProbe::MayMatch
        );
        assert_eq!(
            table.probe_user_key_prefix_filter(b"data:beta:"),
            SsTablePrefixFilterProbe::NoMatch
        );
        assert_eq!(
            table.probe_user_key_prefix_filter(b"schema:alpha"),
            SsTablePrefixFilterProbe::FailOpen
        );
        assert_eq!(
            table.probe_user_key_filter(b"data:alpha:001", 8),
            SsTablePrefixFilterProbe::MayMatch
        );
        assert_eq!(
            table.probe_user_key_filter(b"data:beta:001", 8),
            SsTablePrefixFilterProbe::NoMatch
        );
        assert_eq!(
            table.probe_user_key_filter(b"data:beta:001", 4),
            SsTablePrefixFilterProbe::FailOpen
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn versioned_filter_block_supports_sql_index_prefix_negative_checks() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_index_prefix_filter_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);

        let mut block = Vec::new();
        let keys = [
            b"index:metrics:host_id,ts:i1|i001:row1".as_slice(),
            b"index:metrics:host_id,ts:i1|i002:row2".as_slice(),
        ];
        let mut first_key = None;
        for user_key in keys {
            let mut key = user_key.to_vec();
            key.extend_from_slice(&[0; 8]);
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            builder.add_key(&key);
            append_block_entry(&mut block, &key, user_key);
        }
        builder
            .flush_block(first_key.clone().unwrap(), 2, &block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 106, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        let prefix = b"index:metrics:host_id,ts:i1|";
        let start = b"index:metrics:host_id,ts:i1|i000:";
        let mut end = prefix.to_vec();
        end.push(0xff);

        assert_eq!(
            SsTable::sql_index_prefix_for_range(start, &end),
            Some(prefix.to_vec())
        );
        assert_eq!(
            table.probe_sql_index_prefix_filter(prefix),
            SsTablePrefixFilterProbe::MayMatch
        );
        assert_eq!(
            table.probe_sql_index_prefix_filter(b"index:metrics:host_id,ts:i2|"),
            SsTablePrefixFilterProbe::NoMatch
        );
        assert_eq!(
            table.probe_sql_index_prefix_filter(b"data:metrics:"),
            SsTablePrefixFilterProbe::FailOpen
        );

        let mut sharded_end = b"shard:3:index:metrics:host_id,ts:i1|".to_vec();
        sharded_end.push(0xff);
        assert_eq!(
            SsTable::sql_index_prefix_for_range(
                b"shard:3:index:metrics:host_id,ts:i1|i000:",
                &sharded_end
            ),
            Some(b"shard:3:index:metrics:host_id,ts:i1|".to_vec())
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn user_key_range_iterator_skips_block_without_target_sql_index_prefix_property() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_block_index_prefix_filter_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);

        let mut block = Vec::new();
        let mut first_key = None;
        for user_key in [
            b"index:metrics:host_id,ts:i1|i001:row1".as_slice(),
            b"index:metrics:host_id,ts:i3|i001:row3".as_slice(),
        ] {
            let mut key = user_key.to_vec();
            key.extend_from_slice(&[0; 8]);
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            builder.add_key(&key);
            append_block_entry(&mut block, &key, user_key);
        }

        let first_key = first_key.unwrap();
        builder
            .flush_block(first_key.clone(), 2, &block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let cache = Arc::new(Cache::new(16));
        let table = SsTable::open(path.clone(), 107, cache).await.unwrap();
        let block_offset = table.index_offset_for(first_key.as_slice()).unwrap();
        let block_properties = table
            .block_properties
            .get()
            .expect("block properties should be eager for full open");
        assert!(block_properties[0].sql_index_prefixes_complete);
        assert_eq!(
            block_properties[0].sql_index_prefixes,
            vec![
                b"index:metrics:host_id,ts:i1|".to_vec(),
                b"index:metrics:host_id,ts:i3|".to_vec()
            ]
        );

        let mut absent_start = b"index:metrics:host_id,ts:i2|".to_vec();
        absent_start.extend_from_slice(&[0; 8]);
        let mut absent_end = b"index:metrics:host_id,ts:i2|".to_vec();
        absent_end.push(0xff);
        let mut absent_iter = table
            .new_user_key_range_iterator(Some(&absent_start), Some(&absent_end), 8)
            .await
            .unwrap();

        assert!(absent_iter.next().await.unwrap().is_none());
        assert!(
            table.block_cache.get(&(107, block_offset)).is_none(),
            "block SQL index-prefix property should skip a mixed block that lacks the target prefix"
        );

        let mut positive_start = b"index:metrics:host_id,ts:i3|".to_vec();
        positive_start.extend_from_slice(&[0; 8]);
        let mut positive_end = b"index:metrics:host_id,ts:i3|".to_vec();
        positive_end.push(0xff);
        let mut positive_iter = table
            .new_user_key_range_iterator(Some(&positive_start), Some(&positive_end), 8)
            .await
            .unwrap();

        let row = positive_iter.next().await.unwrap().unwrap();
        let mut expected_key = b"index:metrics:host_id,ts:i3|i001:row3".to_vec();
        expected_key.extend_from_slice(&[0; 8]);
        assert_eq!(row.0, expected_key);
        assert_eq!(row.1, b"index:metrics:host_id,ts:i3|i001:row3");
        assert!(table.block_cache.get(&(107, block_offset)).is_some());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn builder_without_sql_zone_maps_keeps_v4_meta_format() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_default_meta_v4_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);

        let user_key = b"index:metrics:host_id,ts:i1|i001:row1";
        let mut key = user_key.to_vec();
        key.extend_from_slice(&[0; 8]);
        let mut block = Vec::new();
        append_block_entry(&mut block, &key, user_key);
        builder.add_key(&key);
        builder.flush_block(key.clone(), 1, &block).await.unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 117, Arc::new(Cache::new(16)))
            .await
            .unwrap();

        assert_eq!(
            table.meta.format_version,
            SSTABLE_META_VERSION_BLOCK_SQL_INDEX_PREFIXES
        );
        let block_properties = table
            .block_properties
            .get()
            .expect("block properties should be eager for full open");
        assert_eq!(block_properties.len(), 1);
        assert!(!block_properties[0].sql_zone_maps_complete);
        assert!(block_properties[0].sql_zone_maps.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn builder_with_sql_zone_maps_writes_v5_meta_format() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_zone_map_meta_v5_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);

        let user_key = b"index:metrics:host_id,ts:i1|i001:row1";
        let mut key = user_key.to_vec();
        key.extend_from_slice(&[0; 8]);
        let mut block = Vec::new();
        append_block_entry(&mut block, &key, user_key);
        builder.add_key(&key);
        builder.flush_block(key.clone(), 1, &block).await.unwrap();

        let zone_map = SsTableSqlZoneMap {
            table_prefix: b"data:metrics:".to_vec(),
            schema_fingerprint: 42,
            column_index: 1,
            column_name: "bucket".to_string(),
            type_tag: 1,
            value_encoding_version: 1,
            min_scalar: 7,
            max_scalar: 9,
            row_count: 1,
            null_count: 0,
            non_null_count: 1,
            put_count: 1,
            tombstone_count: 0,
            bounds_valid: true,
        };
        builder.block_properties[0].sql_zone_maps_complete = true;
        builder.block_properties[0].sql_zone_maps = vec![zone_map.clone()];

        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 118, Arc::new(Cache::new(16)))
            .await
            .unwrap();

        assert_eq!(
            table.meta.format_version,
            SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS
        );
        let block_properties = table
            .block_properties
            .get()
            .expect("block properties should be eager for full open");
        assert_eq!(block_properties.len(), 1);
        assert!(block_properties[0].sql_zone_maps_complete);
        assert_eq!(block_properties[0].sql_zone_maps, vec![zone_map]);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn builder_collects_sql_zone_maps_for_supported_data_rows() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_zone_map_producer_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut schemas = BTreeMap::new();
        schemas.insert("metrics".to_string(), zone_map_test_schema());

        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);
        builder.enable_sql_zone_map_collection(Arc::new(schemas));

        let rows = [
            (
                b"data:metrics:001".as_slice(),
                encoded_put_row(&[
                    Value::Integer(1),
                    Value::Integer(7),
                    Value::Boolean(true),
                    Value::Timestamp(100),
                    Value::String("a".to_string()),
                ]),
            ),
            (
                b"data:metrics:002".as_slice(),
                encoded_put_row(&[
                    Value::Integer(2),
                    Value::Null,
                    Value::Boolean(false),
                    Value::Timestamp(200),
                    Value::String("b".to_string()),
                ]),
            ),
            (b"data:metrics:003".as_slice(), Vec::from([0u8])),
        ];
        let mut block = Vec::new();
        let mut first_key = None;
        for (user_key, value) in rows {
            let mut key = user_key.to_vec();
            key.extend_from_slice(&[0; 8]);
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            builder.add_key(&key);
            append_block_entry(&mut block, &key, &value);
        }
        builder
            .flush_block(first_key.unwrap(), 3, &block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 119, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        assert_eq!(
            table.meta.format_version,
            SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS
        );
        let block_properties = table
            .block_properties
            .get()
            .expect("block properties should be eager for full open");
        assert_eq!(block_properties.len(), 1);
        assert!(block_properties[0].sql_zone_maps_complete);
        assert_eq!(block_properties[0].sql_zone_maps.len(), 4);

        let map_for = |column_name: &str| {
            block_properties[0]
                .sql_zone_maps
                .iter()
                .find(|map| map.column_name == column_name)
                .expect("zone map should exist")
        };
        let id = map_for("id");
        assert_eq!(id.min_scalar, 1);
        assert_eq!(id.max_scalar, 2);
        assert_eq!(id.row_count, 3);
        assert_eq!(id.put_count, 2);
        assert_eq!(id.tombstone_count, 1);
        assert_eq!(id.non_null_count, 2);
        assert_eq!(id.null_count, 0);
        assert!(id.bounds_valid);

        let bucket = map_for("bucket");
        assert_eq!(bucket.min_scalar, 7);
        assert_eq!(bucket.max_scalar, 7);
        assert_eq!(bucket.row_count, 3);
        assert_eq!(bucket.put_count, 2);
        assert_eq!(bucket.tombstone_count, 1);
        assert_eq!(bucket.non_null_count, 1);
        assert_eq!(bucket.null_count, 1);
        assert!(bucket.bounds_valid);

        let flag = map_for("flag");
        assert_eq!(flag.min_scalar, 0);
        assert_eq!(flag.max_scalar, 1);
        assert_eq!(flag.type_tag, SQL_BLOCK_ZONE_MAP_TYPE_BOOLEAN);

        let ts = map_for("ts");
        assert_eq!(ts.min_scalar, 100);
        assert_eq!(ts.max_scalar, 200);
        assert_eq!(ts.type_tag, SQL_BLOCK_ZONE_MAP_TYPE_TIMESTAMP);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn builder_sql_zone_map_collection_fails_open_without_schema() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_zone_map_missing_schema_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);
        builder.enable_sql_zone_map_collection(Arc::new(BTreeMap::new()));

        let user_key = b"data:metrics:001";
        let mut key = user_key.to_vec();
        key.extend_from_slice(&[0; 8]);
        let value = encoded_put_row(&[
            Value::Integer(1),
            Value::Integer(7),
            Value::Boolean(true),
            Value::Timestamp(100),
            Value::String("a".to_string()),
        ]);
        let mut block = Vec::new();
        builder.add_key(&key);
        append_block_entry(&mut block, &key, &value);
        builder.flush_block(key, 1, &block).await.unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 120, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        assert_eq!(
            table.meta.format_version,
            SSTABLE_META_VERSION_BLOCK_SQL_INDEX_PREFIXES
        );
        let block_properties = table
            .block_properties
            .get()
            .expect("block properties should be eager for full open");
        assert!(!block_properties[0].sql_zone_maps_complete);
        assert!(block_properties[0].sql_zone_maps.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn builder_sql_zone_map_collection_keeps_v4_for_unsupported_only_schema() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_zone_map_unsupported_schema_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let schema = TableSchema::new(
            "logs".to_string(),
            vec![Column {
                name: "payload".to_string(),
                data_type: "TEXT".to_string(),
                is_primary: false,
                is_indexed: false,
                index_type: IndexType::None,
                default_value: None,
                is_nullable: true,
                is_unique: false,
                check_expr: None,
            }],
        );
        let mut schemas = BTreeMap::new();
        schemas.insert("logs".to_string(), schema);

        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);
        builder.enable_sql_zone_map_collection(Arc::new(schemas));

        let user_key = b"data:logs:001";
        let mut key = user_key.to_vec();
        key.extend_from_slice(&[0; 8]);
        let value = encoded_put_row(&[Value::String("hello".to_string())]);
        let mut block = Vec::new();
        builder.add_key(&key);
        append_block_entry(&mut block, &key, &value);
        builder.flush_block(key, 1, &block).await.unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 121, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        assert_eq!(
            table.meta.format_version,
            SSTABLE_META_VERSION_BLOCK_SQL_INDEX_PREFIXES
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn builder_sql_zone_map_collection_fails_open_on_type_mismatch() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_zone_map_type_mismatch_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut schemas = BTreeMap::new();
        schemas.insert("metrics".to_string(), zone_map_test_schema());

        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);
        builder.enable_sql_zone_map_collection(Arc::new(schemas));

        let user_key = b"data:metrics:001";
        let mut key = user_key.to_vec();
        key.extend_from_slice(&[0; 8]);
        let value = encoded_put_row(&[
            Value::Integer(1),
            Value::String("wrong-type".to_string()),
            Value::Boolean(true),
            Value::Timestamp(100),
            Value::String("payload".to_string()),
        ]);
        let mut block = Vec::new();
        builder.add_key(&key);
        append_block_entry(&mut block, &key, &value);
        builder.flush_block(key, 1, &block).await.unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 122, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        assert_eq!(
            table.meta.format_version,
            SSTABLE_META_VERSION_BLOCK_SQL_INDEX_PREFIXES
        );
        let block_properties = table.current_block_properties();
        assert!(!block_properties[0].sql_zone_maps_complete);
        assert!(block_properties[0].sql_zone_maps.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn builder_sql_zone_map_collection_fails_open_on_malformed_values() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_zone_map_malformed_values_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut schemas = BTreeMap::new();
        schemas.insert("metrics".to_string(), zone_map_test_schema());

        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);
        builder.enable_sql_zone_map_collection(Arc::new(schemas));

        let entries = [
            (
                b"data:metrics:001".as_slice(),
                Vec::from([1u8, 0xff, 0xff, 0xff]),
            ),
            (b"data:metrics:002".as_slice(), Vec::from([9u8])),
        ];
        let mut block = Vec::new();
        let mut first_key = None;
        for (user_key, value) in entries {
            let mut key = user_key.to_vec();
            key.extend_from_slice(&[0; 8]);
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            builder.add_key(&key);
            append_block_entry(&mut block, &key, &value);
        }
        builder
            .flush_block(first_key.unwrap(), 2, &block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 123, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        assert_eq!(
            table.meta.format_version,
            SSTABLE_META_VERSION_BLOCK_SQL_INDEX_PREFIXES
        );
        let block_properties = table.current_block_properties();
        assert!(!block_properties[0].sql_zone_maps_complete);
        assert!(block_properties[0].sql_zone_maps.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn user_key_range_iterator_fails_open_on_incomplete_sql_index_prefix_property() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_incomplete_block_index_prefix_filter_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);

        let user_key_low = b"index:metrics:host_id,ts:i1|i001:row1";
        let user_key_high = b"index:metrics:host_id,ts:i3|i001:row3";
        let mut low_key = user_key_low.to_vec();
        low_key.extend_from_slice(&[0; 8]);
        let malformed_key = b"bad".to_vec();
        let mut high_key = user_key_high.to_vec();
        high_key.extend_from_slice(&[0; 8]);

        let mut block = Vec::new();
        append_block_entry(&mut block, &low_key, user_key_low);
        append_block_entry(&mut block, &malformed_key, b"malformed");
        append_block_entry(&mut block, &high_key, user_key_high);
        builder.add_key(&low_key);
        builder.add_key(&high_key);
        builder
            .flush_block(low_key.clone(), 3, &block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let cache = Arc::new(Cache::new(16));
        let table = SsTable::open(path.clone(), 108, cache).await.unwrap();
        let block_offset = table.index_offset_for(low_key.as_slice()).unwrap();
        let block_properties = table
            .block_properties
            .get()
            .expect("block properties should be eager for full open");
        assert!(!block_properties[0].sql_index_prefixes_complete);
        assert!(block_properties[0].sql_index_prefixes.is_empty());

        let mut absent_start = b"index:metrics:host_id,ts:i2|".to_vec();
        absent_start.extend_from_slice(&[0; 8]);
        let mut absent_end = b"index:metrics:host_id,ts:i2|".to_vec();
        absent_end.push(0xff);
        let mut absent_iter = table
            .new_user_key_range_iterator(Some(&absent_start), Some(&absent_end), 8)
            .await
            .unwrap();

        assert!(absent_iter.next().await.unwrap().is_none());
        assert!(
            table.block_cache.get(&(108, block_offset)).is_some(),
            "incomplete block SQL index-prefix property should fail open and read the block"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn user_key_filter_fails_open_after_builder_sees_short_internal_key() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_invalid_user_key_filter_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);

        let short_key = b"aaa".to_vec();
        let mut valid_key = b"data:valid:001".to_vec();
        valid_key.extend_from_slice(&[0; 8]);
        let mut block = Vec::new();
        append_block_entry(&mut block, &short_key, b"short-value");
        append_block_entry(&mut block, &valid_key, b"valid-value");
        builder.add_key(&short_key);
        builder.add_key(&valid_key);
        builder
            .flush_block(short_key.clone(), 2, &block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 104, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        assert_eq!(
            table.probe_user_key_filter(b"data:valid:001", 8),
            SsTablePrefixFilterProbe::FailOpen
        );
        assert_eq!(
            table.probe_user_key_prefix_filter(b"data:valid:"),
            SsTablePrefixFilterProbe::FailOpen
        );
        assert!(table.get(&valid_key).await.unwrap().is_some());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn user_key_range_iterator_skips_block_without_target_table_prefix_property() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_block_prefix_filter_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);

        let mut block = Vec::new();
        let mut first_key = None;
        for user_key in [b"data:a:001".as_slice(), b"data:z:001".as_slice()] {
            let mut key = user_key.to_vec();
            key.extend_from_slice(&[0; 8]);
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            builder.add_key(&key);
            append_block_entry(&mut block, &key, user_key);
        }

        let first_key = first_key.unwrap();
        builder
            .flush_block(first_key.clone(), 2, &block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let cache = Arc::new(Cache::new(16));
        let table = SsTable::open(path.clone(), 103, cache).await.unwrap();
        let block_offset = table.index_offset_for(first_key.as_slice()).unwrap();
        let block_properties = table
            .block_properties
            .get()
            .expect("block properties should be eager for full open");

        assert_eq!(
            block_properties[0].table_prefixes,
            vec![b"data:a:".to_vec(), b"data:z:".to_vec()]
        );

        let mut absent_start = b"data:m:".to_vec();
        absent_start.extend_from_slice(&[0; 8]);
        let absent_end = prefix_end(b"data:m:").unwrap();
        let mut absent_iter = table
            .new_user_key_range_iterator(Some(&absent_start), Some(&absent_end), 8)
            .await
            .unwrap();

        assert!(absent_iter.next().await.unwrap().is_none());
        assert!(
            table.block_cache.get(&(103, block_offset)).is_none(),
            "block prefix property should skip a mixed block that lacks the target table prefix"
        );

        let mut positive_start = b"data:a:".to_vec();
        positive_start.extend_from_slice(&[0; 8]);
        let positive_end = prefix_end(b"data:a:").unwrap();
        let mut positive_iter = table
            .new_user_key_range_iterator(Some(&positive_start), Some(&positive_end), 8)
            .await
            .unwrap();

        let row = positive_iter.next().await.unwrap().unwrap();
        assert_eq!(row.0, first_key);
        assert_eq!(row.1, b"data:a:001");
        assert!(table.block_cache.get(&(103, block_offset)).is_some());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn user_key_range_reverse_iterator_skips_block_without_target_table_prefix_property() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_reverse_block_prefix_filter_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);

        let mut block = Vec::new();
        let mut first_key = None;
        for user_key in [b"data:a:001".as_slice(), b"data:z:001".as_slice()] {
            let mut key = user_key.to_vec();
            key.extend_from_slice(&[0; 8]);
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            builder.add_key(&key);
            append_block_entry(&mut block, &key, user_key);
        }

        let first_key = first_key.unwrap();
        builder
            .flush_block(first_key.clone(), 2, &block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let cache = Arc::new(Cache::new(16));
        let table = SsTable::open(path.clone(), 106, cache).await.unwrap();
        let block_offset = table.index_offset_for(first_key.as_slice()).unwrap();

        let absent_end = prefix_end(b"data:m:").unwrap();
        let mut absent_iter = table
            .new_user_key_range_reverse_iterator(Some(b"data:m:"), Some(&absent_end), 8)
            .await
            .unwrap();

        assert!(absent_iter.next().await.unwrap().is_none());
        assert!(
            table.block_cache.get(&(106, block_offset)).is_none(),
            "reverse block prefix property should skip a mixed block that lacks the target table prefix"
        );

        let positive_end = prefix_end(b"data:z:").unwrap();
        let mut positive_iter = table
            .new_user_key_range_reverse_iterator(Some(b"data:z:"), Some(&positive_end), 8)
            .await
            .unwrap();

        let row = positive_iter.next().await.unwrap().unwrap();
        let mut expected_key = b"data:z:001".to_vec();
        expected_key.extend_from_slice(&[0; 8]);
        assert_eq!(row.0, expected_key);
        assert_eq!(row.1, b"data:z:001");
        assert!(table.block_cache.get(&(106, block_offset)).is_some());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn user_key_range_reverse_iterator_skips_block_without_target_sql_index_prefix_property()
    {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_reverse_block_index_prefix_filter_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(8);

        let mut block = Vec::new();
        let mut first_key = None;
        for user_key in [
            b"index:metrics:host_id,ts:i1|i001:row1".as_slice(),
            b"index:metrics:host_id,ts:i3|i001:row3".as_slice(),
        ] {
            let mut key = user_key.to_vec();
            key.extend_from_slice(&[0; 8]);
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            builder.add_key(&key);
            append_block_entry(&mut block, &key, user_key);
        }

        let first_key = first_key.unwrap();
        builder
            .flush_block(first_key.clone(), 2, &block)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let cache = Arc::new(Cache::new(16));
        let table = SsTable::open(path.clone(), 108, cache).await.unwrap();
        let block_offset = table.index_offset_for(first_key.as_slice()).unwrap();

        let mut absent_end = b"index:metrics:host_id,ts:i2|".to_vec();
        absent_end.push(0xff);
        let mut absent_iter = table
            .new_user_key_range_reverse_iterator(
                Some(b"index:metrics:host_id,ts:i2|"),
                Some(&absent_end),
                8,
            )
            .await
            .unwrap();

        assert!(absent_iter.next().await.unwrap().is_none());
        assert!(
            table.block_cache.get(&(108, block_offset)).is_none(),
            "reverse block SQL index-prefix property should skip a mixed block that lacks the target prefix"
        );

        let mut positive_end = b"index:metrics:host_id,ts:i3|".to_vec();
        positive_end.push(0xff);
        let mut positive_iter = table
            .new_user_key_range_reverse_iterator(
                Some(b"index:metrics:host_id,ts:i3|"),
                Some(&positive_end),
                8,
            )
            .await
            .unwrap();

        let row = positive_iter.next().await.unwrap().unwrap();
        let mut expected_key = b"index:metrics:host_id,ts:i3|i001:row3".to_vec();
        expected_key.extend_from_slice(&[0; 8]);
        assert_eq!(row.0, expected_key);
        assert_eq!(row.1, b"index:metrics:host_id,ts:i3|i001:row3");
        assert!(table.block_cache.get(&(108, block_offset)).is_some());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn user_key_upper_bound_ignores_internal_timestamp_suffix() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_user_upper_bound_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());

        let mut key = b"a".to_vec();
        key.extend_from_slice(&[0xff; 8]);
        let value = b"short-key";
        let mut block = Vec::new();
        block.extend_from_slice(&(key.len() as u32).to_le_bytes());
        block.extend_from_slice(&key);
        block.extend_from_slice(&(value.len() as u32).to_le_bytes());
        block.extend_from_slice(value);

        builder.add_key(&key);
        builder.flush_block(key.clone(), 1, &block).await.unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 100, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        let mut iter = table
            .new_user_key_range_iterator(None, Some(b"a\0"), 8)
            .await
            .unwrap();

        let row = iter.next().await.unwrap().unwrap();
        assert_eq!(row.0, key);
        assert_eq!(row.1, value);
        assert!(iter.next().await.unwrap().is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn user_key_reverse_upper_bound_ignores_internal_timestamp_suffix() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_reverse_user_upper_bound_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());

        let mut key = b"a".to_vec();
        key.extend_from_slice(&[0xff; 8]);
        let value = b"short-key";
        let mut block = Vec::new();
        append_block_entry(&mut block, &key, value);

        builder.add_key(&key);
        builder.flush_block(key.clone(), 1, &block).await.unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 107, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        let mut iter = table
            .new_user_key_range_reverse_iterator(None, Some(b"a\0"), 8)
            .await
            .unwrap();

        let row = iter.next().await.unwrap().unwrap();
        assert_eq!(row.0, key);
        assert_eq!(row.1, value);
        assert!(iter.next().await.unwrap().is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn user_key_reverse_iterator_returns_same_user_internal_versions_descending() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_reverse_internal_versions_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());

        let mut block = Vec::new();
        for (suffix, value) in [
            ([0x00; 8], b"newest".as_slice()),
            ([0x7f; 8], b"middle".as_slice()),
            ([0xff; 8], b"oldest".as_slice()),
        ] {
            let mut key = b"data:r:001".to_vec();
            key.extend_from_slice(&suffix);
            builder.add_key(&key);
            append_block_entry(&mut block, &key, value);
        }

        let mut first_key = b"data:r:001".to_vec();
        first_key.extend_from_slice(&[0x00; 8]);
        builder.flush_block(first_key, 3, &block).await.unwrap();
        builder.finish().await.unwrap();

        let table = SsTable::open(path.clone(), 108, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        let mut iter = table
            .new_user_key_range_reverse_iterator(Some(b"data:r:001"), Some(b"data:r:002"), 8)
            .await
            .unwrap();
        let mut values = Vec::new();
        while let Some((_key, value)) = iter.next().await.unwrap() {
            values.push(value);
        }

        assert_eq!(
            values,
            vec![b"oldest".to_vec(), b"middle".to_vec(), b"newest".to_vec()]
        );

        let _ = std::fs::remove_file(&path);
    }
}

// Builder for SSTable
pub struct SsTableBuilder {
    file: Option<tokio::fs::File>,
    path: PathBuf,
    index: BTreeMap<Vec<u8>, u64>,
    block_properties: Vec<SsTableBlockProperties>,
    reverse_seek_blocks: Vec<SsTableReverseSeekBlockIndex>,
    filter: BloomFilter,
    expected_filter_items: usize,
    prefix_filter: Option<BloomFilter>,
    user_key_filter: Option<BloomFilter>,
    sql_index_prefix_filter: Option<BloomFilter>,
    prefix_filter_suffix_len: Option<usize>,
    sql_zone_map_schemas: Option<Arc<BTreeMap<String, TableSchema>>>,
    current_offset: u64,
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
}

impl SsTableBuilder {
    pub fn new(path: PathBuf) -> Self {
        // Default capacity; production callers size the filters to the real
        // entry count via set_expected_filter_items (a saturated bloom
        // degrades to ~100% false positives and every point probe pays a
        // real block read — BENCHPROD-468).
        let filter = BloomFilter::with_false_pos(0.01).expected_items(100_000);

        Self {
            file: None,
            path,
            index: BTreeMap::new(),
            block_properties: Vec::new(),
            reverse_seek_blocks: Vec::new(),
            filter,
            expected_filter_items: 100_000,
            prefix_filter: None,
            user_key_filter: None,
            sql_index_prefix_filter: None,
            prefix_filter_suffix_len: None,
            sql_zone_map_schemas: None,
            current_offset: 0,
            first_key: None,
            last_key: None,
        }
    }

    /// Size every bloom filter for the real number of entries this SSTable
    /// will hold. Must be called before any add_key and before
    /// enable_user_key_prefix_filter; the whole-key filter is rebuilt here.
    pub fn set_expected_filter_items(&mut self, expected_items: usize) {
        let expected_items = expected_items.max(10_000);
        self.expected_filter_items = expected_items;
        self.filter = BloomFilter::with_false_pos(0.01).expected_items(expected_items);
    }

    pub fn enable_user_key_prefix_filter(&mut self, suffix_len: usize) {
        let expected_items = self.expected_filter_items;
        if self.prefix_filter.is_none() {
            self.prefix_filter =
                Some(BloomFilter::with_false_pos(0.01).expected_items(expected_items));
        }
        if self.user_key_filter.is_none() {
            self.user_key_filter =
                Some(BloomFilter::with_false_pos(0.01).expected_items(expected_items));
        }
        if self.sql_index_prefix_filter.is_none() {
            self.sql_index_prefix_filter =
                Some(BloomFilter::with_false_pos(0.01).expected_items(expected_items));
        }
        self.prefix_filter_suffix_len = Some(suffix_len);
    }

    pub fn enable_sql_zone_map_collection(&mut self, schemas: Arc<BTreeMap<String, TableSchema>>) {
        if !schemas.is_empty() {
            self.sql_zone_map_schemas = Some(schemas);
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        let file = tokio::fs::File::create(&self.path).await?;
        self.file = Some(file);
        Ok(())
    }

    pub fn add(&mut self, _key: &[u8], _val: &[u8]) {
        // This is a simplified "add single entry" method.
        // But for flushing MemTable, we iterate and batch.
        // Let's make a generic batch add or expose low-level.
        // For simplicity, let's just make `add` handle blocking logic?
        // No, `write_memtable` logic in `lsm.rs` was efficient (buffering blocks).

        // Let's reimplement the block buffering logic here but exposing `add`.
        // Wait, `SsTableBuilder` needs to buffer a block.
    }

    // We will port `write_memtable` logic but make it generic iterator based?
    // Or just keep it simple and assume caller handles iteration.

    pub async fn flush_block(&mut self, start_key: Vec<u8>, count: u32, buf: &[u8]) -> Result<()> {
        if self.file.is_none() {
            self.init().await?;
        }

        let block_offset = self.current_offset;
        self.index.insert(start_key.clone(), block_offset);
        let (table_prefix_ranges_complete, table_prefix_ranges) =
            block_table_prefix_ranges(buf, count, self.prefix_filter_suffix_len);
        let (sql_index_prefixes_complete, sql_index_prefixes) =
            block_sql_index_prefixes(buf, count, self.prefix_filter_suffix_len);
        let (sql_zone_maps_complete, sql_zone_maps) = match &self.sql_zone_map_schemas {
            Some(schemas) => {
                block_sql_zone_maps(buf, count, self.prefix_filter_suffix_len, schemas)
            }
            None => (false, Vec::new()),
        };
        self.block_properties.push(SsTableBlockProperties {
            offset: block_offset,
            first_key: start_key.clone(),
            last_key: self.last_key.clone().unwrap_or_else(|| start_key.clone()),
            entry_count: count,
            table_prefixes: block_table_prefixes(buf, count, self.prefix_filter_suffix_len),
            table_prefix_ranges_complete,
            table_prefix_ranges,
            sql_index_prefixes_complete,
            sql_index_prefixes,
            sql_zone_maps_complete,
            sql_zone_maps,
        });
        if let Some(block_index) =
            SsTable::reverse_seek_block_index_from_entries(block_offset, count, buf)
        {
            self.reverse_seek_blocks.push(block_index);
        }

        if self.first_key.is_none() {
            self.first_key = Some(start_key.clone());
        }

        if let Some(file) = &mut self.file {
            let encoded = SsTable::encode_block_payload(count, buf);

            // CRC covers the encoded block payload, whether compressed or legacy.
            let mut hasher = Crc32Hasher::new();
            hasher.update(&encoded);
            let crc = hasher.finalize();

            file.write_all(&encoded).await?;
            self.current_offset += encoded.len() as u64;
            file.write_all(&crc.to_le_bytes()).await?;
            self.current_offset += 4;
        }
        Ok(())
    }

    pub fn add_key(&mut self, key: &[u8]) {
        self.filter.insert(key);
        if let Some(suffix_len) = self.prefix_filter_suffix_len {
            let Some(user_key_len) = key.len().checked_sub(suffix_len) else {
                self.prefix_filter = None;
                self.user_key_filter = None;
                self.sql_index_prefix_filter = None;
                self.prefix_filter_suffix_len = None;
                self.last_key = Some(key.to_vec());
                return;
            };
            let user_key = &key[..user_key_len];
            if let Some(filter) = self.user_key_filter.as_mut() {
                filter.insert(user_key);
            }
            if let Some(filter) = self.prefix_filter.as_mut() {
                if let Some(prefix) = table_user_key_prefix(user_key) {
                    filter.insert(prefix);
                }
            }
            if let Some(filter) = self.sql_index_prefix_filter.as_mut() {
                if let Some(prefix) = sql_index_scan_prefix(user_key) {
                    filter.insert(prefix);
                }
            }
        }
        // Track last_key (assuming sorted insertion)
        self.last_key = Some(key.to_vec());
    }

    pub async fn finish(mut self) -> Result<()> {
        if self.file.is_none() {
            self.init().await?;
        }

        let index_offset = self.current_offset;
        let index_bytes = SsTable::encode_index_block(self.index);

        let mut file = self.file.unwrap();

        file.write_all(&index_bytes).await?;

        let filter_offset = index_offset + index_bytes.len() as u64;
        let filter_block = SsTableFilterBlock {
            format_version: SSTABLE_FILTER_VERSION_SQL_INDEX_PREFIX,
            whole_key_filter: self.filter,
            prefix_filter: self.prefix_filter.map(|filter| SsTablePrefixFilter {
                extractor_id: SSTABLE_PREFIX_EXTRACTOR_TABLE_USER_KEY,
                filter,
            }),
            user_key_filter: self.user_key_filter.map(|filter| SsTableUserKeyFilter {
                extractor_id: SSTABLE_USER_KEY_EXTRACTOR_MVCC_USER_KEY,
                suffix_len: self.prefix_filter_suffix_len.unwrap_or_default(),
                filter,
            }),
            sql_index_prefix_filter: self.sql_index_prefix_filter.map(|filter| {
                SsTablePrefixFilter {
                    extractor_id: SSTABLE_SQL_INDEX_PREFIX_EXTRACTOR,
                    filter,
                }
            }),
        };
        let filter_bytes = bincode::serialize(&filter_block).unwrap();
        file.write_all(&filter_bytes).await?;

        let meta_offset = filter_offset + filter_bytes.len() as u64;
        let block_property_count = self.block_properties.len();
        let first_key = self.first_key.unwrap_or_default();
        let last_key = self.last_key.unwrap_or_default();
        let has_sql_zone_maps = self
            .block_properties
            .iter()
            .any(|property| !property.sql_zone_maps.is_empty());
        let meta_bytes = if has_sql_zone_maps {
            let meta = BlockSqlZoneMapsSsTableMetaV6 {
                first_key,
                last_key,
                block_properties: self
                    .block_properties
                    .into_iter()
                    .map(BlockSqlZoneMapsSsTableBlockPropertiesV6::from)
                    .collect(),
                format_version: SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS,
            };
            SsTable::encode_versioned_meta(SSTABLE_META_VERSION_BLOCK_SQL_ZONE_MAPS, &meta).unwrap()
        } else {
            let meta = BlockSqlIndexPrefixesSsTableMetaV4 {
                first_key,
                last_key,
                block_properties: self
                    .block_properties
                    .into_iter()
                    .map(BlockSqlIndexPrefixesSsTableBlockPropertiesV4::from)
                    .collect(),
                format_version: SSTABLE_META_VERSION_BLOCK_SQL_INDEX_PREFIXES,
            };
            bincode::serialize(&meta).unwrap()
        };
        file.write_all(&meta_bytes).await?;

        // Footer: [IndexOffset: 8b] [FilterOffset: 8b] [MetaOffset: 8b] [Magic: 4b]
        file.write_all(&index_offset.to_le_bytes()).await?;
        file.write_all(&filter_offset.to_le_bytes()).await?;
        file.write_all(&meta_offset.to_le_bytes()).await?;
        file.write_all(&SST_MAGIC.to_le_bytes()).await?;

        file.sync_all().await?;
        drop(file);

        if self.reverse_seek_blocks.len() == block_property_count {
            if let Ok(metadata) = tokio::fs::metadata(&self.path).await {
                let index_len = filter_offset - index_offset;
                if let Some(fingerprint) = SsTable::index_cache_fingerprint(
                    &metadata,
                    index_offset,
                    filter_offset,
                    meta_offset,
                    index_len,
                ) {
                    let sidecar = SsTableReverseSeekSidecar {
                        blocks: self.reverse_seek_blocks,
                    };
                    let sidecar_path = SsTable::reverse_seek_path(&self.path);
                    if let Err(error) =
                        SsTable::persist_reverse_seek_sidecar(&sidecar_path, fingerprint, &sidecar)
                            .await
                    {
                        monitor::inc_sstable_reverse_seek_sidecar_write_error();
                        eprintln!(
                            "Warning: failed to persist SSTable reverse seek sidecar {}: {}",
                            sidecar_path.display(),
                            error
                        );
                    } else {
                        monitor::inc_sstable_reverse_seek_sidecar_write();
                    }
                }
            }
        }
        Ok(())
    }
}
