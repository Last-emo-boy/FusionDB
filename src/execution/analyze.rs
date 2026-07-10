use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use bincode::Options;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::{Executor, QueryResult};

const ANALYZE_DISTINCT_PREALLOC_LIMIT: usize = 1024;
const ANALYZE_HLL_PRECISION: u8 = 12;
const TABLE_STATS_STORAGE_VERSION_V1: u16 = 1;
const TABLE_STATS_STORAGE_VERSION: u16 = 2;
const STABLE_HASH_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const STABLE_HASH_PRIME: u64 = 0x0000_0100_0000_01b3;

fn analyze_distinct_capacity(row_count: usize) -> usize {
    row_count.min(ANALYZE_DISTINCT_PREALLOC_LIMIT)
}

#[cfg(test)]
fn analyze_data_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
    prefix.push_str("data:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

fn analyze_schema_key_for_table(table_name: &str) -> String {
    let mut key = String::with_capacity("schema:".len() + table_name.len());
    key.push_str("schema:");
    key.push_str(table_name);
    key
}

fn table_stats_key_for_table(table_name: &str) -> String {
    let mut key = String::with_capacity("stats:table:".len() + table_name.len());
    key.push_str("stats:table:");
    key.push_str(table_name);
    key
}

fn stats_ndv_key(value: &Value) -> Option<Vec<u8>> {
    let mut key = Vec::with_capacity(16);
    match value {
        Value::Null => return None,
        Value::Boolean(value) => {
            key.push(1);
            key.push(u8::from(*value));
        }
        Value::Integer(value) => {
            key.push(2);
            key.extend_from_slice(&value.to_le_bytes());
        }
        Value::Float(value) => {
            key.push(3);
            let bits = if value.is_nan() {
                0x7ff8_0000_0000_0000u64
            } else if *value == 0.0 {
                0.0f64.to_bits()
            } else {
                value.to_bits()
            };
            key.extend_from_slice(&bits.to_le_bytes());
        }
        Value::Decimal(value) => {
            key.push(4);
            stable_key_str(
                &mut key,
                &Value::normalize_decimal(value).unwrap_or_else(|| value.clone()),
            );
        }
        Value::String(value) => {
            key.push(5);
            stable_key_str(&mut key, value);
        }
        Value::Date(value) => {
            key.push(6);
            key.extend_from_slice(&value.to_le_bytes());
        }
        Value::Timestamp(value) => {
            key.push(7);
            key.extend_from_slice(&value.to_le_bytes());
        }
        Value::Interval(value) => {
            key.push(8);
            key.extend_from_slice(&value.to_le_bytes());
        }
        Value::Blob(_) | Value::Vector(_) | Value::Array(_) | Value::Object(_) => return None,
    }
    Some(key)
}

fn stable_key_str(key: &mut Vec<u8>, value: &str) {
    key.extend_from_slice(&(value.len() as u64).to_le_bytes());
    key.extend_from_slice(value.as_bytes());
}

fn stats_ndv_hash(key: &[u8]) -> u64 {
    let mut hash = STABLE_HASH_OFFSET;
    stable_hash_bytes(&mut hash, key);
    mix_stable_hash(hash)
}

fn mix_stable_hash(mut hash: u64) -> u64 {
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

fn stable_hash_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(STABLE_HASH_PRIME);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct TableStats {
    pub table_name: String,
    pub row_count: usize,
    pub analyzed_rows: usize,
    pub sampled: bool,
    pub columns: Vec<ColumnStats>,
    pub updated_at_epoch_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct ColumnStats {
    pub name: String,
    pub null_count: usize,
    pub distinct_count: usize,
    pub distinct_kind: DistinctCountKind,
    pub distinct_method: DistinctCountMethod,
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub most_common_values: Vec<MostCommonValue>,
    pub histogram: Vec<HistogramBucket>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct MostCommonValue {
    pub value: Value,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct HistogramBucket {
    pub lower: Option<Value>,
    pub upper: Option<Value>,
    pub count: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum DistinctCountKind {
    Exact,
    Estimated,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum DistinctCountMethod {
    ExactSet,
    HyperLogLog,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct StoredTableStatsHeader {
    version: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct StoredTableStatsV1 {
    version: u16,
    stats: TableStatsV1,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct StoredTableStatsV2 {
    version: u16,
    stats: TableStatsV2,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TableStatsV1 {
    table_name: String,
    row_count: usize,
    columns: Vec<ColumnStatsV1>,
    updated_at_epoch_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ColumnStatsV1 {
    name: String,
    null_count: usize,
    distinct_count: usize,
    min: Option<Value>,
    max: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TableStatsV2 {
    table_name: String,
    row_count: usize,
    analyzed_rows: usize,
    sampled: bool,
    columns: Vec<ColumnStatsV2>,
    updated_at_epoch_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ColumnStatsV2 {
    name: String,
    null_count: usize,
    distinct_count: usize,
    distinct_kind: DistinctCountKind,
    distinct_method: DistinctCountMethod,
    min: Option<Value>,
    max: Option<Value>,
    most_common_values: Vec<MostCommonValue>,
    histogram: Vec<HistogramBucket>,
}

impl From<&TableStats> for TableStatsV1 {
    fn from(stats: &TableStats) -> Self {
        Self {
            table_name: stats.table_name.clone(),
            row_count: stats.row_count,
            columns: stats.columns.iter().map(ColumnStatsV1::from).collect(),
            updated_at_epoch_ms: stats.updated_at_epoch_ms,
        }
    }
}

impl From<&TableStats> for TableStatsV2 {
    fn from(stats: &TableStats) -> Self {
        Self {
            table_name: stats.table_name.clone(),
            row_count: stats.row_count,
            analyzed_rows: stats.analyzed_rows,
            sampled: stats.sampled,
            columns: stats.columns.iter().map(ColumnStatsV2::from).collect(),
            updated_at_epoch_ms: stats.updated_at_epoch_ms,
        }
    }
}

impl From<TableStatsV2> for TableStats {
    fn from(stats: TableStatsV2) -> Self {
        Self {
            table_name: stats.table_name,
            row_count: stats.row_count,
            analyzed_rows: stats.analyzed_rows,
            sampled: stats.sampled,
            columns: stats.columns.into_iter().map(ColumnStats::from).collect(),
            updated_at_epoch_ms: stats.updated_at_epoch_ms,
        }
    }
}

impl From<&ColumnStats> for ColumnStatsV2 {
    fn from(stats: &ColumnStats) -> Self {
        Self {
            name: stats.name.clone(),
            null_count: stats.null_count,
            distinct_count: stats.distinct_count,
            distinct_kind: stats.distinct_kind,
            distinct_method: stats.distinct_method,
            min: stats.min.clone(),
            max: stats.max.clone(),
            most_common_values: stats.most_common_values.clone(),
            histogram: stats.histogram.clone(),
        }
    }
}

impl From<ColumnStatsV2> for ColumnStats {
    fn from(stats: ColumnStatsV2) -> Self {
        Self {
            name: stats.name,
            null_count: stats.null_count,
            distinct_count: stats.distinct_count,
            distinct_kind: stats.distinct_kind,
            distinct_method: stats.distinct_method,
            min: stats.min,
            max: stats.max,
            most_common_values: stats.most_common_values,
            histogram: stats.histogram,
        }
    }
}

impl From<TableStatsV1> for TableStats {
    fn from(stats: TableStatsV1) -> Self {
        Self {
            table_name: stats.table_name,
            row_count: stats.row_count,
            analyzed_rows: stats.row_count,
            sampled: false,
            columns: stats.columns.into_iter().map(ColumnStats::from).collect(),
            updated_at_epoch_ms: stats.updated_at_epoch_ms,
        }
    }
}

impl From<&ColumnStats> for ColumnStatsV1 {
    fn from(stats: &ColumnStats) -> Self {
        Self {
            name: stats.name.clone(),
            null_count: stats.null_count,
            distinct_count: stats.distinct_count,
            min: stats.min.clone(),
            max: stats.max.clone(),
        }
    }
}

impl From<ColumnStatsV1> for ColumnStats {
    fn from(stats: ColumnStatsV1) -> Self {
        Self {
            name: stats.name,
            null_count: stats.null_count,
            distinct_count: stats.distinct_count,
            distinct_kind: DistinctCountKind::Exact,
            distinct_method: DistinctCountMethod::ExactSet,
            min: stats.min,
            max: stats.max,
            most_common_values: Vec::new(),
            histogram: Vec::new(),
        }
    }
}

impl Executor {
    pub(crate) async fn handle_analyze(
        &self,
        analyze: &sqlparser::ast::Analyze,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name = analyze.table_name.to_string();
        let schema = self.load_analyze_schema(&table_name, txn).await?;
        let stats = self.collect_table_stats(&table_name, &schema, txn).await?;
        self.store_table_stats(&stats, txn).await?;

        Ok(QueryResult::Success {
            message: format!(
                "Analyzed table {}, {} rows, {} columns",
                table_name,
                stats.row_count,
                stats.columns.len()
            ),
        })
    }

    pub(crate) async fn load_table_stats(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Option<TableStats>> {
        let key = table_stats_key_for_table(table_name);
        let Some(bytes) = txn.get(key.as_bytes()).await? else {
            return Ok(None);
        };
        Self::deserialize_table_stats(&bytes).map(Some)
    }

    async fn store_table_stats(&self, stats: &TableStats, txn: &mut dyn Transaction) -> Result<()> {
        let key = table_stats_key_for_table(&stats.table_name);
        let bytes = Self::serialize_table_stats(stats)?;
        txn.put(key.as_bytes(), &bytes).await
    }

    fn serialize_table_stats(stats: &TableStats) -> Result<Vec<u8>> {
        let stored = StoredTableStatsV2 {
            version: TABLE_STATS_STORAGE_VERSION,
            stats: TableStatsV2::from(stats),
        };
        bincode::serialize(&stored)
            .map_err(|e| FusionError::Execution(format!("Stats serialization error: {}", e)))
    }

    fn deserialize_table_stats(bytes: &[u8]) -> Result<TableStats> {
        let versioned_error = match bincode::options()
            .allow_trailing_bytes()
            .deserialize::<StoredTableStatsHeader>(bytes)
        {
            Ok(header) => match header.version {
                TABLE_STATS_STORAGE_VERSION_V1 => {
                    return bincode::deserialize::<StoredTableStatsV1>(bytes)
                        .map(|stored| TableStats::from(stored.stats))
                        .map_err(|e| {
                            FusionError::Execution(format!(
                                "Stats deserialization error: V1 format: {}",
                                e
                            ))
                        });
                }
                TABLE_STATS_STORAGE_VERSION => {
                    return bincode::deserialize::<StoredTableStatsV2>(bytes)
                        .map(|stored| TableStats::from(stored.stats))
                        .map_err(|e| {
                            FusionError::Execution(format!(
                                "Stats deserialization error: V2 format: {}",
                                e
                            ))
                        });
                }
                unsupported => FusionError::Execution(format!(
                    "Stats deserialization error: unsupported stats storage version {}",
                    unsupported
                )),
            },
            Err(e) => FusionError::Execution(format!(
                "Stats deserialization error: version header: {}",
                e
            )),
        };

        bincode::deserialize::<TableStatsV1>(bytes)
            .map(TableStats::from)
            .map_err(|legacy_error| {
                FusionError::Execution(format!(
                    "{}; legacy format: {}",
                    versioned_error, legacy_error
                ))
            })
    }

    async fn collect_table_stats(
        &self,
        table_name: &str,
        schema: &TableSchema,
        txn: &mut dyn Transaction,
    ) -> Result<TableStats> {
        let kv_pairs = self
            .scan_routed_data_prefixes_for_table_with_options(
                table_name,
                txn,
                None,
                self.bulk_scan_options(),
            )
            .await?;
        let distinct_capacity = analyze_distinct_capacity(kv_pairs.len());
        let mut collectors = Vec::with_capacity(schema.columns.len());
        for column in &schema.columns {
            collectors.push(ColumnStatsCollector::new(
                column.name.clone(),
                distinct_capacity,
            ));
        }

        let mut row_count = 0usize;
        for (key, bytes) in kv_pairs {
            row_count += 1;
            let key_str = std::str::from_utf8(&key).ok();
            let row = if let Some(key_str) = key_str {
                if let Some(row) = self.row_cache_lookup(key_str, &bytes) {
                    row
                } else {
                    crate::common::encoding::RowDecoder::decode(&bytes).map_err(|e| {
                        FusionError::Execution(format!("Data deserialization error: {}", e))
                    })?
                }
            } else {
                crate::common::encoding::RowDecoder::decode(&bytes).map_err(|e| {
                    FusionError::Execution(format!("Data deserialization error: {}", e))
                })?
            };

            for (idx, collector) in collectors.iter_mut().enumerate() {
                let value = row.get(idx).cloned().unwrap_or(Value::Null);
                collector.observe(value);
            }
        }

        let mut columns = Vec::with_capacity(collectors.len());
        for collector in collectors {
            columns.push(collector.finish());
        }

        Ok(TableStats {
            table_name: table_name.to_string(),
            row_count,
            analyzed_rows: row_count,
            sampled: false,
            columns,
            updated_at_epoch_ms: Self::current_epoch_ms(),
        })
    }

    async fn load_analyze_schema(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<TableSchema> {
        let schema_key = analyze_schema_key_for_table(table_name);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name)))?;
        bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))
    }
}

struct DistinctEstimate {
    count: usize,
    kind: DistinctCountKind,
    method: DistinctCountMethod,
}

enum DistinctValueCollector {
    Exact {
        keys: HashSet<Vec<u8>>,
        exact_limit: usize,
    },
    Hll(HyperLogLog),
}

impl DistinctValueCollector {
    fn new(exact_limit: usize) -> Self {
        Self::Exact {
            keys: HashSet::with_capacity(exact_limit),
            exact_limit,
        }
    }

    fn observe(&mut self, key: &[u8]) {
        match self {
            Self::Exact { keys, exact_limit } => {
                if keys.len() < *exact_limit || keys.contains(key) {
                    keys.insert(key.to_vec());
                    return;
                }

                let mut hll = HyperLogLog::new(ANALYZE_HLL_PRECISION);
                for key in keys.iter() {
                    hll.observe(stats_ndv_hash(key));
                }
                hll.observe(stats_ndv_hash(key));
                *self = Self::Hll(hll);
            }
            Self::Hll(hll) => hll.observe(stats_ndv_hash(key)),
        }
    }

    fn finish(self, non_null_count: usize) -> DistinctEstimate {
        match self {
            Self::Exact { keys, .. } => DistinctEstimate {
                count: keys.len(),
                kind: DistinctCountKind::Exact,
                method: DistinctCountMethod::ExactSet,
            },
            Self::Hll(hll) => {
                let count = hll
                    .estimate()
                    .min(non_null_count)
                    .max(usize::from(non_null_count > 0));
                DistinctEstimate {
                    count,
                    kind: DistinctCountKind::Estimated,
                    method: DistinctCountMethod::HyperLogLog,
                }
            }
        }
    }
}

struct HyperLogLog {
    precision: u8,
    registers: Vec<u8>,
}

impl HyperLogLog {
    fn new(precision: u8) -> Self {
        debug_assert!(precision > 0 && precision < 64);
        Self {
            precision,
            registers: vec![0; 1usize << precision],
        }
    }

    fn observe(&mut self, hash: u64) {
        let index = (hash >> (64 - self.precision)) as usize;
        let remaining = hash << self.precision;
        let max_rank = 64 - u32::from(self.precision) + 1;
        let rank = (remaining.leading_zeros() + 1).min(max_rank) as u8;
        if let Some(register) = self.registers.get_mut(index) {
            *register = (*register).max(rank);
        }
    }

    fn estimate(&self) -> usize {
        let m = self.registers.len() as f64;
        let zero_registers = self
            .registers
            .iter()
            .filter(|register| **register == 0)
            .count();
        let harmonic_sum = self
            .registers
            .iter()
            .map(|register| 2f64.powi(-i32::from(*register)))
            .sum::<f64>();
        let raw = Self::alpha(self.registers.len()) * m * m / harmonic_sum;
        let estimate = if raw <= 2.5 * m && zero_registers > 0 {
            m * (m / zero_registers as f64).ln()
        } else {
            raw
        };
        estimate.round().max(0.0) as usize
    }

    fn alpha(registers: usize) -> f64 {
        match registers {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / registers as f64),
        }
    }
}

struct ColumnStatsCollector {
    name: String,
    null_count: usize,
    non_null_count: usize,
    distinct: DistinctValueCollector,
    min: Option<Value>,
    max: Option<Value>,
}

impl ColumnStatsCollector {
    fn new(name: String, distinct_capacity: usize) -> Self {
        Self {
            name,
            null_count: 0,
            non_null_count: 0,
            distinct: DistinctValueCollector::new(distinct_capacity),
            min: None,
            max: None,
        }
    }

    fn observe(&mut self, value: Value) {
        if value == Value::Null {
            self.null_count += 1;
            return;
        }

        if let Some(ndv_key) = stats_ndv_key(&value) {
            self.non_null_count += 1;
            self.distinct.observe(&ndv_key);
            if self
                .min
                .as_ref()
                .is_none_or(|current| value.compare(current).is_lt())
            {
                self.min = Some(value.clone());
            }
            if self
                .max
                .as_ref()
                .is_none_or(|current| value.compare(current).is_gt())
            {
                self.max = Some(value);
            }
        }
    }

    fn finish(self) -> ColumnStats {
        let distinct = self.distinct.finish(self.non_null_count);
        ColumnStats {
            name: self.name,
            null_count: self.null_count,
            distinct_count: distinct.count,
            distinct_kind: distinct.kind,
            distinct_method: distinct.method,
            min: self.min,
            max: self.max,
            most_common_values: Vec::new(),
            histogram: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_table_stats() -> TableStats {
        TableStats {
            table_name: "items".to_string(),
            row_count: 3,
            analyzed_rows: 3,
            sampled: false,
            columns: vec![ColumnStats {
                name: "qty".to_string(),
                null_count: 1,
                distinct_count: 2,
                distinct_kind: DistinctCountKind::Exact,
                distinct_method: DistinctCountMethod::ExactSet,
                min: Some(Value::Integer(5)),
                max: Some(Value::Integer(7)),
                most_common_values: Vec::new(),
                histogram: Vec::new(),
            }],
            updated_at_epoch_ms: 42,
        }
    }

    fn observe_distinct_value(collector: &mut DistinctValueCollector, value: Value) {
        let key = stats_ndv_key(&value).expect("stats ndv key");
        collector.observe(&key);
    }

    #[test]
    fn analyze_distinct_capacity_is_bounded() {
        assert_eq!(analyze_distinct_capacity(0), 0);
        assert_eq!(analyze_distinct_capacity(3), 3);
        assert_eq!(
            analyze_distinct_capacity(ANALYZE_DISTINCT_PREALLOC_LIMIT + 1),
            ANALYZE_DISTINCT_PREALLOC_LIMIT
        );
    }

    #[test]
    fn distinct_value_collector_stays_exact_below_limit() {
        let mut collector = DistinctValueCollector::new(4);
        observe_distinct_value(&mut collector, Value::Integer(1));
        observe_distinct_value(&mut collector, Value::Integer(2));
        observe_distinct_value(&mut collector, Value::Integer(2));

        let estimate = collector.finish(3);

        assert_eq!(estimate.count, 2);
        assert_eq!(estimate.kind, DistinctCountKind::Exact);
        assert_eq!(estimate.method, DistinctCountMethod::ExactSet);
    }

    #[test]
    fn distinct_value_collector_switches_to_hll_above_limit() {
        let mut collector = DistinctValueCollector::new(8);
        for value in 0..2_000 {
            observe_distinct_value(&mut collector, Value::Integer(value));
        }

        let estimate = collector.finish(2_000);

        assert!(estimate.count >= 1_700, "count was {}", estimate.count);
        assert!(estimate.count <= 2_000, "count was {}", estimate.count);
        assert_eq!(estimate.kind, DistinctCountKind::Estimated);
        assert_eq!(estimate.method, DistinctCountMethod::HyperLogLog);
    }

    #[test]
    fn stats_ndv_key_canonicalizes_equivalent_values() {
        assert_eq!(
            stats_ndv_key(&Value::Float(-0.0)),
            stats_ndv_key(&Value::Float(0.0))
        );
        assert_eq!(
            stats_ndv_key(&Value::Decimal("001.2300".to_string())),
            stats_ndv_key(&Value::Decimal("1.23".to_string()))
        );
        assert_eq!(
            stats_ndv_key(&Value::Float(f64::NAN)),
            stats_ndv_key(&Value::Float(f64::from_bits(0x7ff0_0000_0000_0001)))
        );
    }

    #[test]
    fn stats_ndv_hash_is_stable_for_the_same_key() {
        let key = stats_ndv_key(&Value::String("book".to_string())).expect("stats ndv key");

        assert_eq!(stats_ndv_hash(&key), stats_ndv_hash(&key));
    }

    #[test]
    fn column_stats_collector_marks_high_ndv_as_hll_estimated() {
        let mut collector = ColumnStatsCollector::new("id".to_string(), 8);
        for value in 0..2_000 {
            collector.observe(Value::Integer(value));
        }

        let stats = collector.finish();

        assert!(stats.distinct_count >= 1_700);
        assert!(stats.distinct_count <= 2_000);
        assert_eq!(stats.distinct_kind, DistinctCountKind::Estimated);
        assert_eq!(stats.distinct_method, DistinctCountMethod::HyperLogLog);
        assert_eq!(stats.min, Some(Value::Integer(0)));
        assert_eq!(stats.max, Some(Value::Integer(1_999)));
    }

    #[test]
    fn analyze_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = analyze_data_prefix_for_table("items");

        assert_eq!(prefix, "data:items:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn analyze_schema_key_for_table_preallocates_exact_key() {
        let key = analyze_schema_key_for_table("items");

        assert_eq!(key, "schema:items");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn table_stats_key_for_table_preallocates_exact_key() {
        let key = table_stats_key_for_table("items");

        assert_eq!(key, "stats:table:items");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn table_stats_round_trips_versioned_storage_format() {
        let stats = sample_table_stats();

        let encoded = Executor::serialize_table_stats(&stats).expect("serialize stats");
        let decoded = Executor::deserialize_table_stats(&encoded).expect("deserialize stats");

        assert_eq!(decoded, stats);
        let stored: StoredTableStatsV2 = bincode::deserialize(&encoded).expect("stored wrapper");
        assert_eq!(stored.version, TABLE_STATS_STORAGE_VERSION);
        assert_eq!(stored.stats, TableStatsV2::from(&stats));
        assert_eq!(TableStats::from(stored.stats), stats);
    }

    #[test]
    fn table_stats_deserializes_versioned_v1_storage_format() {
        let stats = sample_table_stats();
        let encoded = bincode::serialize(&StoredTableStatsV1 {
            version: TABLE_STATS_STORAGE_VERSION_V1,
            stats: TableStatsV1::from(&stats),
        })
        .expect("stored stats v1");

        let decoded = Executor::deserialize_table_stats(&encoded).expect("versioned v1 decode");

        assert_eq!(decoded, stats);
    }

    #[test]
    fn table_stats_deserializes_legacy_storage_format() {
        let stats = sample_table_stats();
        let legacy = bincode::serialize(&TableStatsV1::from(&stats)).expect("legacy stats");

        let decoded = Executor::deserialize_table_stats(&legacy).expect("legacy decode");

        assert_eq!(decoded, stats);
    }

    #[test]
    fn table_stats_rejects_unknown_storage_version() {
        let stats = sample_table_stats();
        let encoded = bincode::serialize(&StoredTableStatsV1 {
            version: TABLE_STATS_STORAGE_VERSION + 1,
            stats: TableStatsV1::from(&stats),
        })
        .expect("stored stats");

        let err = Executor::deserialize_table_stats(&encoded).expect_err("unknown version");

        assert!(format!("{err}").contains("unsupported stats storage version"));
    }
}
