use fusiondb::monitor;
use fusiondb::storage::sstable::{BlockCache, SsTable, SsTableBuilder, SsTablePrefixFilterProbe};
use moka::sync::Cache;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Instant;

const TS_SIZE: usize = 8;

#[derive(Serialize)]
struct PhaseMetrics {
    block_cache_hit_count: u64,
    block_cache_miss_count: u64,
    block_cache_insert_count: u64,
    block_cache_insert_bytes: u64,
    sstable_point_probe_count: u64,
    sstable_user_key_filter_check_count: u64,
    sstable_user_key_filter_positive_count: u64,
    sstable_user_key_filter_skip_count: u64,
    sstable_user_key_filter_fail_open_count: u64,
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
    speedup_vs_fail_open: f64,
}

#[derive(Clone, Serialize)]
struct BenchConfig {
    sstable_count: usize,
    iters: usize,
    payload_bytes: usize,
    block_cache_capacity: u64,
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
        sstable_count: env_usize("BENCH_SST_USER_KEY_BLOOM_SSTABLES", 512),
        iters: env_usize("BENCH_SST_USER_KEY_BLOOM_ITERS", 5),
        payload_bytes: env_usize("BENCH_SST_USER_KEY_BLOOM_PAYLOAD_BYTES", 1024),
        block_cache_capacity: env_u64("BENCH_SST_USER_KEY_BLOOM_CACHE_BLOCKS", 1_000_000),
    };

    let base_dir = std::env::temp_dir().join(format!(
        "fusiondb_sstable_user_key_bloom_bench_{}",
        uuid::Uuid::new_v4()
    ));
    tokio::fs::create_dir_all(&base_dir).await?;

    let optimized = build_table_set(&base_dir, "optimized", &config, true).await?;
    let fail_open = build_table_set(&base_dir, "fail_open", &config, false).await?;
    let absent_user_key = choose_absent_user_key(&optimized.tables)?;

    let optimized_report = run_phase("optimized", &optimized, &config, &absent_user_key).await?;
    let fail_open_report = run_phase("fail_open", &fail_open, &config, &absent_user_key).await?;
    let speedup_vs_fail_open = if optimized_report.avg_ms > 0.0 {
        fail_open_report.avg_ms / optimized_report.avg_ms
    } else {
        0.0
    };

    let report = BenchReport {
        benchmark: "sstable_user_key_bloom_micro".to_string(),
        config,
        optimized: optimized_report,
        fail_open: fail_open_report,
        speedup_vs_fail_open,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn build_table_set(
    base_dir: &Path,
    label: &str,
    config: &BenchConfig,
    with_user_key_filter: bool,
) -> Result<TableSet, Box<dyn std::error::Error>> {
    let cache = Arc::new(Cache::new(config.block_cache_capacity));
    let mut tables = Vec::with_capacity(config.sstable_count);
    let mut paths = Vec::with_capacity(config.sstable_count);

    for sst_idx in 0..config.sstable_count {
        let path = base_dir.join(format!("{label}_{sst_idx:06}.sst"));
        let mut builder = SsTableBuilder::new(path.clone());
        if with_user_key_filter {
            builder.enable_user_key_prefix_filter(TS_SIZE);
        }

        let low_key = internal_key(format!("data:point_bloom:a:{sst_idx:08}").as_bytes());
        let high_key = internal_key(format!("data:point_bloom:z:{sst_idx:08}").as_bytes());
        let mut block = Vec::new();
        append_block_entry(
            &mut block,
            &low_key,
            &payload(config.payload_bytes, sst_idx, 0),
        );
        append_block_entry(
            &mut block,
            &high_key,
            &payload(config.payload_bytes, sst_idx, 1),
        );

        builder.add_key(&low_key);
        builder.add_key(&high_key);
        builder.flush_block(low_key, 2, &block).await?;
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

fn choose_absent_user_key(tables: &[SsTable]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    for id in 0..100_000 {
        let user_key = format!("data:point_bloom:m:{id:08}").into_bytes();
        if tables.iter().all(|table| {
            matches!(
                table.probe_user_key_filter(&user_key, TS_SIZE),
                SsTablePrefixFilterProbe::NoMatch
            )
        }) {
            return Ok(user_key);
        }
    }
    Err("could not find an absent user key with Bloom NoMatch across all SSTables".into())
}

async fn run_phase(
    name: &str,
    table_set: &TableSet,
    config: &BenchConfig,
    user_key: &[u8],
) -> Result<PhaseReport, Box<dyn std::error::Error>> {
    let mut times_ms = Vec::with_capacity(config.iters);
    let mut row_count = 0usize;
    let search_key = internal_key(user_key);

    monitor::GLOBAL_METRICS.reset();
    for _ in 0..config.iters {
        table_set.cache.invalidate_all();
        table_set.cache.run_pending_tasks();

        let started = Instant::now();
        for table in &table_set.tables {
            monitor::inc_sstable_point_probe();
            monitor::inc_sstable_user_key_filter_check();
            match table.probe_user_key_filter(user_key, TS_SIZE) {
                SsTablePrefixFilterProbe::MayMatch => {
                    monitor::inc_sstable_user_key_filter_positive();
                }
                SsTablePrefixFilterProbe::NoMatch => {
                    monitor::inc_sstable_user_key_filter_skip();
                    continue;
                }
                SsTablePrefixFilterProbe::FailOpen => {
                    monitor::inc_sstable_user_key_filter_fail_open();
                }
            }
            if let Some((key, _value)) = table.find_ge(&search_key).await? {
                if key == search_key {
                    row_count += 1;
                }
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

fn capture_phase_metrics() -> PhaseMetrics {
    let metrics = &monitor::GLOBAL_METRICS;
    PhaseMetrics {
        block_cache_hit_count: metrics.block_cache_hit_count.load(Relaxed),
        block_cache_miss_count: metrics.block_cache_miss_count.load(Relaxed),
        block_cache_insert_count: metrics.block_cache_insert_count.load(Relaxed),
        block_cache_insert_bytes: metrics.block_cache_insert_bytes.load(Relaxed),
        sstable_point_probe_count: metrics.sstable_point_probe_count.load(Relaxed),
        sstable_user_key_filter_check_count: metrics
            .sstable_user_key_filter_check_count
            .load(Relaxed),
        sstable_user_key_filter_positive_count: metrics
            .sstable_user_key_filter_positive_count
            .load(Relaxed),
        sstable_user_key_filter_skip_count: metrics
            .sstable_user_key_filter_skip_count
            .load(Relaxed),
        sstable_user_key_filter_fail_open_count: metrics
            .sstable_user_key_filter_fail_open_count
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
