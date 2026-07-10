use fusiondb::monitor;
use fusiondb::storage::sstable::{BlockCache, SsTable, SsTableBuilder, SsTablePrefixFilterProbe};
use moka::sync::Cache;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Instant;

const TS_SIZE: usize = 8;
const TARGET_PREFIX: &[u8] = b"index:metrics:host_id,ts:i2|";

#[derive(Serialize)]
struct PhaseMetrics {
    block_cache_hit_count: u64,
    block_cache_miss_count: u64,
    block_cache_insert_count: u64,
    block_cache_insert_bytes: u64,
    sstable_index_prefix_filter_check_count: u64,
    sstable_index_prefix_filter_positive_count: u64,
    sstable_index_prefix_filter_skip_count: u64,
    sstable_index_prefix_filter_fail_open_count: u64,
    sstable_block_index_prefix_filter_check_count: u64,
    sstable_block_index_prefix_filter_positive_count: u64,
    sstable_block_index_prefix_filter_skip_count: u64,
    sstable_block_index_prefix_filter_fail_open_count: u64,
}

#[derive(Serialize)]
struct PhaseReport {
    name: String,
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    min_ms: f64,
    max_ms: f64,
    times_ms: Vec<f64>,
    row_count: usize,
    metrics_delta: PhaseMetrics,
}

#[derive(Serialize)]
struct BenchReport {
    benchmark: String,
    config: BenchConfig,
    optimized: PhaseReport,
    fail_open: PhaseReport,
    incomplete: PhaseReport,
    natural_false_positive: PhaseReport,
    target_prefix: String,
    natural_target_prefix: String,
    speedup_vs_fail_open: f64,
}

#[derive(Clone, Serialize)]
struct BenchConfig {
    sstable_count: usize,
    iters: usize,
    payload_bytes: usize,
    block_cache_capacity: u64,
    natural_prefixes: usize,
    natural_iters: usize,
    natural_payload_bytes: usize,
    natural_candidate_limit: usize,
}

enum TableShape {
    Optimized,
    FailOpen,
    Incomplete,
}

struct TableSet {
    tables: Vec<SsTable>,
    cache: Arc<BlockCache>,
    paths: Vec<PathBuf>,
}

impl Drop for TableSet {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = std::fs::remove_file(path);
        }
        if let Some(dir) = self.paths.first().and_then(|path| path.parent()) {
            let _ = std::fs::remove_dir(dir);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BenchConfig {
        sstable_count: env_usize("BENCH_SST_BLOCK_INDEX_PREFIX_SSTABLES", 512),
        iters: env_usize("BENCH_SST_BLOCK_INDEX_PREFIX_ITERS", 5),
        payload_bytes: env_usize("BENCH_SST_BLOCK_INDEX_PREFIX_PAYLOAD_BYTES", 1024),
        block_cache_capacity: env_u64("BENCH_SST_BLOCK_INDEX_PREFIX_CACHE_BLOCKS", 1_000_000),
        natural_prefixes: env_usize("BENCH_SST_BLOCK_INDEX_PREFIX_NATURAL_PREFIXES", 32_768),
        natural_iters: env_usize("BENCH_SST_BLOCK_INDEX_PREFIX_NATURAL_ITERS", 3),
        natural_payload_bytes: env_usize("BENCH_SST_BLOCK_INDEX_PREFIX_NATURAL_PAYLOAD_BYTES", 16),
        natural_candidate_limit: env_usize(
            "BENCH_SST_BLOCK_INDEX_PREFIX_NATURAL_CANDIDATES",
            200_000,
        ),
    };

    let base_dir = std::env::temp_dir().join(format!(
        "fusiondb_sstable_block_index_prefix_bench_{}",
        uuid::Uuid::new_v4()
    ));
    tokio::fs::create_dir_all(&base_dir).await?;

    let optimized = build_table_set(&base_dir, "optimized", &config, TableShape::Optimized).await?;
    let fail_open = build_table_set(&base_dir, "fail_open", &config, TableShape::FailOpen).await?;
    let incomplete =
        build_table_set(&base_dir, "incomplete", &config, TableShape::Incomplete).await?;
    let natural = build_natural_false_positive_table_set(&base_dir, &config).await?;
    let target_prefix = TARGET_PREFIX.to_vec();
    let natural_target_prefix =
        find_natural_false_positive_prefix(&natural.tables[0], config.natural_candidate_limit)?;

    let optimized_report = run_phase("optimized", &optimized, config.iters, &target_prefix).await?;
    let fail_open_report = run_phase("fail_open", &fail_open, config.iters, &target_prefix).await?;
    let incomplete_report =
        run_phase("incomplete", &incomplete, config.iters, &target_prefix).await?;
    let natural_false_positive_report = run_phase(
        "natural_false_positive",
        &natural,
        config.natural_iters,
        &natural_target_prefix,
    )
    .await?;
    let speedup_vs_fail_open = if optimized_report.avg_ms > 0.0 {
        fail_open_report.avg_ms / optimized_report.avg_ms
    } else {
        0.0
    };

    let report = BenchReport {
        benchmark: "sstable_block_index_prefix_micro".to_string(),
        config,
        optimized: optimized_report,
        fail_open: fail_open_report,
        incomplete: incomplete_report,
        natural_false_positive: natural_false_positive_report,
        target_prefix: String::from_utf8_lossy(&target_prefix).to_string(),
        natural_target_prefix: String::from_utf8_lossy(&natural_target_prefix).to_string(),
        speedup_vs_fail_open,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn build_table_set(
    base_dir: &Path,
    label: &str,
    config: &BenchConfig,
    shape: TableShape,
) -> Result<TableSet, Box<dyn std::error::Error>> {
    let cache = Arc::new(Cache::new(config.block_cache_capacity));
    let mut tables = Vec::with_capacity(config.sstable_count);
    let mut paths = Vec::with_capacity(config.sstable_count);

    for sst_idx in 0..config.sstable_count {
        let path = base_dir.join(format!("{label}_{sst_idx:06}.sst"));
        let mut builder = SsTableBuilder::new(path.clone());
        if matches!(shape, TableShape::Optimized | TableShape::Incomplete) {
            builder.enable_user_key_prefix_filter(TS_SIZE);
        }

        let low_key =
            internal_key(format!("index:metrics:host_id,ts:i1|i{sst_idx:08}:row_low").as_bytes());
        let high_key =
            internal_key(format!("index:metrics:host_id,ts:i3|i{sst_idx:08}:row_high").as_bytes());
        let mut block = Vec::new();
        append_block_entry(
            &mut block,
            &low_key,
            &payload(config.payload_bytes, sst_idx, 0),
        );
        if matches!(shape, TableShape::Incomplete) {
            append_block_entry(
                &mut block,
                b"bad",
                &payload(config.payload_bytes, sst_idx, 1),
            );
        }
        append_block_entry(
            &mut block,
            &high_key,
            &payload(config.payload_bytes, sst_idx, 2),
        );

        builder.add_key(&low_key);
        builder.add_key(&high_key);
        let entry_count = if matches!(shape, TableShape::Incomplete) {
            3
        } else {
            2
        };
        builder.flush_block(low_key, entry_count, &block).await?;
        if matches!(shape, TableShape::Optimized | TableShape::Incomplete) {
            let filter_only_key = internal_key(TARGET_PREFIX);
            builder.add_key(&filter_only_key);
        }
        builder.finish().await?;

        let table = SsTable::open(path.clone(), sst_idx as u64, cache.clone()).await?;
        tables.push(table);
        paths.push(path);
    }

    Ok(TableSet {
        tables,
        cache,
        paths,
    })
}

async fn build_natural_false_positive_table_set(
    base_dir: &Path,
    config: &BenchConfig,
) -> Result<TableSet, Box<dyn std::error::Error>> {
    let cache = Arc::new(Cache::new(config.block_cache_capacity));
    let path = base_dir.join("natural_false_positive_000000.sst");
    let mut builder = SsTableBuilder::new(path.clone());
    builder.enable_user_key_prefix_filter(TS_SIZE);

    let low_count = config.natural_prefixes / 2;
    let high_count = config.natural_prefixes.saturating_sub(low_count);
    if low_count == 0 || high_count == 0 {
        return Err("natural-prefix benchmark requires at least 2 prefixes".into());
    }

    let mut block = Vec::new();
    let mut first_key = None;
    let mut count = 0u32;
    for idx in 0..low_count {
        let key = internal_key(format!("index:metrics:host_id,ts:i1{idx:08}|row_low").as_bytes());
        if first_key.is_none() {
            first_key = Some(key.clone());
        }
        append_block_entry(
            &mut block,
            &key,
            &payload(config.natural_payload_bytes, idx, 0),
        );
        builder.add_key(&key);
        count += 1;
    }
    for idx in 0..high_count {
        let key = internal_key(format!("index:metrics:host_id,ts:i3{idx:08}|row_high").as_bytes());
        append_block_entry(
            &mut block,
            &key,
            &payload(config.natural_payload_bytes, idx, 1),
        );
        builder.add_key(&key);
        count += 1;
    }

    builder
        .flush_block(first_key.expect("natural block has entries"), count, &block)
        .await?;
    builder.finish().await?;

    let table = SsTable::open(path.clone(), 900_000, cache.clone()).await?;
    Ok(TableSet {
        tables: vec![table],
        cache,
        paths: vec![path],
    })
}

async fn run_phase(
    name: &str,
    table_set: &TableSet,
    iters: usize,
    target_prefix: &[u8],
) -> Result<PhaseReport, Box<dyn std::error::Error>> {
    let mut times_ms = Vec::with_capacity(iters);
    let mut row_count = 0usize;
    let start_key = internal_key(target_prefix);
    let mut upper_bound = target_prefix.to_vec();
    upper_bound.push(0xff);

    monitor::GLOBAL_METRICS.reset();
    for _ in 0..iters {
        table_set.cache.invalidate_all();
        table_set.cache.run_pending_tasks();

        let started = Instant::now();
        for table in &table_set.tables {
            record_sql_index_prefix_probe(table, target_prefix);
            let mut iter = table
                .new_user_key_range_iterator(Some(&start_key), Some(&upper_bound), TS_SIZE)
                .await?;
            while iter.next().await?.is_some() {
                row_count += 1;
            }
        }
        times_ms.push(started.elapsed().as_secs_f64() * 1000.0);
    }

    Ok(PhaseReport {
        name: name.to_string(),
        avg_ms: mean(&times_ms),
        p50_ms: percentile(&times_ms, 0.50),
        p95_ms: percentile(&times_ms, 0.95),
        p99_ms: percentile(&times_ms, 0.99),
        min_ms: times_ms.iter().copied().fold(f64::INFINITY, f64::min),
        max_ms: times_ms.iter().copied().fold(0.0, f64::max),
        times_ms,
        row_count,
        metrics_delta: capture_phase_metrics(),
    })
}

fn find_natural_false_positive_prefix(
    table: &SsTable,
    candidate_limit: usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    for candidate in 0..candidate_limit {
        let prefix = format!("index:metrics:host_id,ts:i2{candidate:08}|").into_bytes();
        if table.probe_sql_index_prefix_filter(&prefix) == SsTablePrefixFilterProbe::MayMatch {
            return Ok(prefix);
        }
    }
    Err(format!(
        "failed to find natural SQL index-prefix Bloom false positive across {candidate_limit} candidates"
    )
    .into())
}

fn record_sql_index_prefix_probe(table: &SsTable, prefix: &[u8]) {
    monitor::inc_sstable_index_prefix_filter_check();
    match table.probe_sql_index_prefix_filter(prefix) {
        SsTablePrefixFilterProbe::MayMatch => {
            monitor::inc_sstable_index_prefix_filter_positive();
        }
        SsTablePrefixFilterProbe::NoMatch => {
            monitor::inc_sstable_index_prefix_filter_skip();
        }
        SsTablePrefixFilterProbe::FailOpen => {
            monitor::inc_sstable_index_prefix_filter_fail_open();
        }
    }
}

fn capture_phase_metrics() -> PhaseMetrics {
    let metrics = &monitor::GLOBAL_METRICS;
    PhaseMetrics {
        block_cache_hit_count: metrics.block_cache_hit_count.load(Relaxed),
        block_cache_miss_count: metrics.block_cache_miss_count.load(Relaxed),
        block_cache_insert_count: metrics.block_cache_insert_count.load(Relaxed),
        block_cache_insert_bytes: metrics.block_cache_insert_bytes.load(Relaxed),
        sstable_index_prefix_filter_check_count: metrics
            .sstable_index_prefix_filter_check_count
            .load(Relaxed),
        sstable_index_prefix_filter_positive_count: metrics
            .sstable_index_prefix_filter_positive_count
            .load(Relaxed),
        sstable_index_prefix_filter_skip_count: metrics
            .sstable_index_prefix_filter_skip_count
            .load(Relaxed),
        sstable_index_prefix_filter_fail_open_count: metrics
            .sstable_index_prefix_filter_fail_open_count
            .load(Relaxed),
        sstable_block_index_prefix_filter_check_count: metrics
            .sstable_block_index_prefix_filter_check_count
            .load(Relaxed),
        sstable_block_index_prefix_filter_positive_count: metrics
            .sstable_block_index_prefix_filter_positive_count
            .load(Relaxed),
        sstable_block_index_prefix_filter_skip_count: metrics
            .sstable_block_index_prefix_filter_skip_count
            .load(Relaxed),
        sstable_block_index_prefix_filter_fail_open_count: metrics
            .sstable_block_index_prefix_filter_fail_open_count
            .load(Relaxed),
    }
}

fn append_block_entry(block: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    block.extend_from_slice(&(key.len() as u32).to_le_bytes());
    block.extend_from_slice(key);
    block.extend_from_slice(&(value.len() as u32).to_le_bytes());
    block.extend_from_slice(value);
}

fn internal_key(user_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(user_key.len() + TS_SIZE);
    key.extend_from_slice(user_key);
    key.extend_from_slice(&0u64.to_be_bytes());
    key
}

fn payload(len: usize, sst_idx: usize, entry_idx: usize) -> Vec<u8> {
    let seed = format!("payload_{sst_idx:08}_{entry_idx}_");
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        out.extend_from_slice(seed.as_bytes());
        out.extend_from_slice(format!("{:08x}", out.len()).as_bytes());
    }
    out.truncate(len);
    out
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted[index]
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}
