use fusiondb::monitor;
use fusiondb::storage::sstable::{BlockCache, SsTable, SsTableBuilder, SsTableReadOptions};
use moka::sync::Cache;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Instant;

#[derive(Default, Serialize)]
struct PhaseMetrics {
    block_cache_hit_count: u64,
    block_cache_miss_count: u64,
    block_cache_insert_count: u64,
    block_cache_insert_bytes: u64,
    block_cache_fill_skip_count: u64,
    block_cache_eviction_count: u64,
    sstable_block_file_open_count: u64,
    sstable_block_read_bytes: u64,
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
    hot_after_scan_hits: u64,
    hot_after_scan_misses: u64,
    metrics_delta: PhaseMetrics,
}

#[derive(Serialize)]
struct BenchReport {
    benchmark: String,
    config: BenchConfig,
    fill_cache: PhaseReport,
    no_fill_cache: PhaseReport,
    speedup_vs_fill_cache: f64,
}

#[derive(Clone, Serialize)]
struct BenchConfig {
    scan_blocks: usize,
    iters: usize,
    payload_bytes: usize,
    block_cache_capacity: u64,
}

struct TableSet {
    table: SsTable,
    cache: Arc<BlockCache>,
    path: PathBuf,
    hot_key: Vec<u8>,
    first_scan_key: Vec<u8>,
}

impl Drop for TableSet {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        if let Some(dir) = self.path.parent() {
            let _ = std::fs::remove_dir(dir);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BenchConfig {
        scan_blocks: env_usize("BENCH_SST_NO_FILL_SCAN_BLOCKS", 512),
        iters: env_usize("BENCH_SST_NO_FILL_ITERS", 5),
        payload_bytes: env_usize("BENCH_SST_NO_FILL_PAYLOAD_BYTES", 1024),
        block_cache_capacity: env_u64("BENCH_SST_NO_FILL_CACHE_BLOCKS", 1),
    };

    let base_dir = std::env::temp_dir().join(format!(
        "fusiondb_sstable_no_fill_cache_bench_{}",
        uuid::Uuid::new_v4()
    ));
    tokio::fs::create_dir_all(&base_dir).await?;
    let table_set = build_table_set(&base_dir, &config).await?;

    let fill_cache = run_phase(
        "fill_cache",
        &table_set,
        &config,
        SsTableReadOptions::fill_cache(),
    )
    .await?;
    let no_fill_cache = run_phase(
        "no_fill_cache",
        &table_set,
        &config,
        SsTableReadOptions::no_fill_cache(),
    )
    .await?;
    let speedup_vs_fill_cache = if no_fill_cache.avg_ms > 0.0 {
        fill_cache.avg_ms / no_fill_cache.avg_ms
    } else {
        0.0
    };

    let report = BenchReport {
        benchmark: "sstable_no_fill_cache_micro".to_string(),
        config,
        fill_cache,
        no_fill_cache,
        speedup_vs_fill_cache,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn build_table_set(
    base_dir: &Path,
    config: &BenchConfig,
) -> Result<TableSet, Box<dyn std::error::Error>> {
    let cache = Arc::new(Cache::new(config.block_cache_capacity));
    let path = base_dir.join("no_fill_cache.sst");
    let mut builder = SsTableBuilder::new(path.clone());

    let hot_key = key(0);
    let first_scan_key = key(1);
    for block_idx in 0..=config.scan_blocks {
        let key = key(block_idx);
        let mut block = Vec::new();
        append_block_entry(&mut block, &key, &payload(config.payload_bytes, block_idx));
        builder.add_key(&key);
        builder.flush_block(key, 1, &block).await?;
    }
    builder.finish().await?;

    let table = SsTable::open(path.clone(), 1, cache.clone()).await?;
    Ok(TableSet {
        table,
        cache,
        path,
        hot_key,
        first_scan_key,
    })
}

async fn run_phase(
    name: &str,
    table_set: &TableSet,
    config: &BenchConfig,
    read_options: SsTableReadOptions,
) -> Result<PhaseReport, Box<dyn std::error::Error>> {
    let mut times_ms = Vec::with_capacity(config.iters);
    let mut row_count = 0usize;
    let mut hot_after_scan_hits = 0u64;
    let mut hot_after_scan_misses = 0u64;
    let mut metrics_delta = PhaseMetrics::default();
    let hot_offset = table_set
        .table
        .index_offset_for(table_set.hot_key.as_slice())
        .expect("hot block offset");

    monitor::GLOBAL_METRICS.reset();
    for _ in 0..config.iters {
        table_set.cache.invalidate_all();
        table_set.cache.run_pending_tasks();
        table_set.table.read_block(hot_offset).await?;
        table_set.cache.run_pending_tasks();
        monitor::GLOBAL_METRICS.reset();

        let started = Instant::now();
        let mut iter = table_set
            .table
            .new_iterator_with_options(Some(&table_set.first_scan_key), read_options)
            .await?;
        while iter.next().await?.is_some() {
            row_count += 1;
        }
        table_set.cache.run_pending_tasks();

        let hits_before = monitor::GLOBAL_METRICS.block_cache_hit_count.load(Relaxed);
        let misses_before = monitor::GLOBAL_METRICS.block_cache_miss_count.load(Relaxed);
        table_set.table.read_block(hot_offset).await?;
        let hits_after = monitor::GLOBAL_METRICS.block_cache_hit_count.load(Relaxed);
        let misses_after = monitor::GLOBAL_METRICS.block_cache_miss_count.load(Relaxed);
        hot_after_scan_hits += hits_after.saturating_sub(hits_before);
        hot_after_scan_misses += misses_after.saturating_sub(misses_before);
        add_metrics(&mut metrics_delta, capture_phase_metrics());
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
        hot_after_scan_hits,
        hot_after_scan_misses,
        metrics_delta,
    })
}

fn add_metrics(total: &mut PhaseMetrics, delta: PhaseMetrics) {
    total.block_cache_hit_count += delta.block_cache_hit_count;
    total.block_cache_miss_count += delta.block_cache_miss_count;
    total.block_cache_insert_count += delta.block_cache_insert_count;
    total.block_cache_insert_bytes += delta.block_cache_insert_bytes;
    total.block_cache_fill_skip_count += delta.block_cache_fill_skip_count;
    total.block_cache_eviction_count += delta.block_cache_eviction_count;
    total.sstable_block_file_open_count += delta.sstable_block_file_open_count;
    total.sstable_block_read_bytes += delta.sstable_block_read_bytes;
}

fn capture_phase_metrics() -> PhaseMetrics {
    let metrics = &monitor::GLOBAL_METRICS;
    PhaseMetrics {
        block_cache_hit_count: metrics.block_cache_hit_count.load(Relaxed),
        block_cache_miss_count: metrics.block_cache_miss_count.load(Relaxed),
        block_cache_insert_count: metrics.block_cache_insert_count.load(Relaxed),
        block_cache_insert_bytes: metrics.block_cache_insert_bytes.load(Relaxed),
        block_cache_fill_skip_count: metrics.block_cache_fill_skip_count.load(Relaxed),
        block_cache_eviction_count: metrics.block_cache_eviction_count.load(Relaxed),
        sstable_block_file_open_count: metrics.sstable_block_file_open_count.load(Relaxed),
        sstable_block_read_bytes: metrics.sstable_block_read_bytes.load(Relaxed),
    }
}

fn append_block_entry(block: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    block.extend_from_slice(&(key.len() as u32).to_le_bytes());
    block.extend_from_slice(key);
    block.extend_from_slice(&(value.len() as u32).to_le_bytes());
    block.extend_from_slice(value);
}

fn key(idx: usize) -> Vec<u8> {
    format!("k{idx:08}").into_bytes()
}

fn payload(len: usize, block_idx: usize) -> Vec<u8> {
    let seed = format!("payload_{block_idx:08}_");
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

fn percentile(values: &[f64], q: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx]
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
