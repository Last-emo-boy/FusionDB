use fusiondb::config::StorageConfig;
use fusiondb::monitor;
use fusiondb::storage::{FusionStorage, Storage};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

const START: &[u8] = b"data:frontier:";
const END: &[u8] = b"data:frontier;";
const EQ_START: &[u8] = b"data:eq:";
const EQ_END: &[u8] = b"data:eq;";

#[derive(Clone, Serialize)]
struct BenchConfig {
    decoy_sstables: usize,
    iters: usize,
    payload_bytes: usize,
    block_cache_capacity: u64,
}

#[derive(Default, Serialize)]
struct PhaseMetrics {
    fusion_reverse_scan_count: u64,
    fusion_reverse_sstable_frontier_probe_count: u64,
    fusion_reverse_sstable_frontier_in_range_count: u64,
    fusion_reverse_sstable_frontier_file_count: u64,
    fusion_reverse_sstable_frontier_tighten_count: u64,
    fusion_reverse_sstable_frontier_empty_skip_count: u64,
    fusion_reverse_sstable_frontier_fail_open_count: u64,
    fusion_reverse_sstable_pending_count: u64,
    fusion_reverse_sstable_activation_count: u64,
    fusion_reverse_sstable_deferred_unopened_count: u64,
    fusion_reverse_sstable_activation_equal_frontier_count: u64,
    fusion_reverse_visible_put_count: u64,
    sstable_iterator_open_count: u64,
    sstable_reverse_iterator_open_count: u64,
    sstable_reverse_block_read_count: u64,
    sstable_reverse_block_entry_decode_count: u64,
    sstable_reverse_block_entry_yield_count: u64,
    compaction_run_count: u64,
    live_sstable_count: u64,
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
    result_keys: Vec<String>,
    result_checksum: String,
    metrics_delta: PhaseMetrics,
}

#[derive(Serialize)]
struct BenchReport {
    benchmark: String,
    config: BenchConfig,
    limit1_deferred: PhaseReport,
    full_drain: PhaseReport,
    equal_frontier_tombstone: PhaseReport,
}

struct BenchStorage {
    storage: FusionStorage,
    data_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BenchConfig {
        decoy_sstables: env_usize("BENCH_FUSION_REVERSE_FRONTIER_DECOYS", 2),
        iters: env_usize("BENCH_FUSION_REVERSE_FRONTIER_ITERS", 3),
        payload_bytes: env_usize("BENCH_FUSION_REVERSE_FRONTIER_PAYLOAD_BYTES", 8192),
        block_cache_capacity: env_u64("BENCH_FUSION_REVERSE_FRONTIER_CACHE_BLOCKS", 1_000_000),
    };

    let main_storage = build_frontier_storage(&config).await?;
    let limit1_deferred = run_phase(
        "limit1_deferred",
        &main_storage.storage,
        config.iters,
        START,
        END,
        Some(1),
    )
    .await?;
    let full_drain = run_phase(
        "full_drain",
        &main_storage.storage,
        config.iters,
        START,
        END,
        None,
    )
    .await?;

    let tombstone_storage = build_equal_frontier_tombstone_storage(&config).await?;
    let equal_frontier_tombstone = run_phase(
        "equal_frontier_tombstone",
        &tombstone_storage.storage,
        config.iters,
        EQ_START,
        EQ_END,
        Some(1),
    )
    .await?;

    let report = BenchReport {
        benchmark: "fusion_reverse_frontier_public_api".to_string(),
        config,
        limit1_deferred,
        full_drain,
        equal_frontier_tombstone,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);

    cleanup_dir(main_storage.data_dir).await;
    cleanup_dir(tombstone_storage.data_dir).await;
    Ok(())
}

async fn build_frontier_storage(
    config: &BenchConfig,
) -> Result<BenchStorage, Box<dyn std::error::Error>> {
    let bench = create_storage("fusiondb_fusion_reverse_frontier_bench", config).await?;

    write_entries(
        &bench.storage,
        &[
            (
                b"data:frontier:900".as_slice(),
                payload(b"target-900", config.payload_bytes, 0),
            ),
            (
                b"data:frontier:901".as_slice(),
                payload(b"target-901", config.payload_bytes, 1),
            ),
        ],
    )
    .await?;
    bench.storage.create_snapshot().await?;

    for decoy_idx in 0..config.decoy_sstables {
        let low_key = format!("data:frontier:1{decoy_idx:08}");
        let high_key = format!("data:frontier;sentinel:{decoy_idx:08}");
        write_entries(
            &bench.storage,
            &[
                (
                    low_key.as_bytes(),
                    payload(b"decoy-low", config.payload_bytes, decoy_idx + 10),
                ),
                (
                    high_key.as_bytes(),
                    payload(b"sentinel", config.payload_bytes, decoy_idx + 20),
                ),
            ],
        )
        .await?;
        bench.storage.create_snapshot().await?;
    }

    Ok(bench)
}

async fn build_equal_frontier_tombstone_storage(
    config: &BenchConfig,
) -> Result<BenchStorage, Box<dyn std::error::Error>> {
    let bench = create_storage("fusiondb_fusion_reverse_frontier_tombstone_bench", config).await?;
    write_entries(
        &bench.storage,
        &[(
            b"data:eq:500".as_slice(),
            payload(b"equal-frontier-live", config.payload_bytes, 100),
        )],
    )
    .await?;
    bench.storage.create_snapshot().await?;

    delete_entries(&bench.storage, &[b"data:eq:500".as_slice()]).await?;
    bench.storage.create_snapshot().await?;
    Ok(bench)
}

async fn create_storage(
    prefix: &str,
    config: &BenchConfig,
) -> Result<BenchStorage, Box<dyn std::error::Error>> {
    let data_dir = std::env::temp_dir().join(format!("{prefix}_{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&data_dir).await?;
    let storage_config = StorageConfig {
        data_dir: data_dir.to_string_lossy().into_owned(),
        wal_file: "fusion.wal".to_string(),
        sstable_dir: "sstables".to_string(),
        memtable_flush_mb: 64,
        row_cache_capacity: 0,
        statement_cache_capacity: 0,
        block_cache_capacity: config.block_cache_capacity,
        sql_bulk_scan_no_fill: true,
        structured_data_shadow_v2: false,
        slow_query_threshold_ms: 100,
    };
    let wal_path = storage_config.wal_path();
    let wal_path = wal_path.to_string_lossy().into_owned();
    let storage = FusionStorage::with_config(&wal_path, &storage_config).await?;
    Ok(BenchStorage { storage, data_dir })
}

async fn write_entries(
    storage: &FusionStorage,
    entries: &[(&[u8], Vec<u8>)],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut txn = storage.begin_transaction().await?;
    for (key, value) in entries {
        txn.put(key, value).await?;
    }
    txn.commit().await?;
    Ok(())
}

async fn delete_entries(
    storage: &FusionStorage,
    keys: &[&[u8]],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut txn = storage.begin_transaction().await?;
    for key in keys {
        txn.delete(key).await?;
    }
    txn.commit().await?;
    Ok(())
}

async fn run_phase(
    name: &str,
    storage: &FusionStorage,
    iters: usize,
    start: &[u8],
    end: &[u8],
    limit: Option<usize>,
) -> Result<PhaseReport, Box<dyn std::error::Error>> {
    let mut times_ms = Vec::with_capacity(iters);
    let mut row_count = 0usize;
    let mut last_result_keys = Vec::new();

    monitor::GLOBAL_METRICS.reset();
    for _ in 0..iters {
        let txn = storage.begin_transaction().await?;
        let started = Instant::now();
        let rows = txn.scan_range_reverse(start, end, limit).await?;
        times_ms.push(started.elapsed().as_secs_f64() * 1000.0);
        row_count += rows.len();
        last_result_keys = rows
            .into_iter()
            .map(|(key, _value)| String::from_utf8_lossy(&key).into_owned())
            .collect();
    }

    let metrics_delta = capture_phase_metrics();
    let result_checksum = checksum_keys(&last_result_keys);
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
        result_keys: last_result_keys,
        result_checksum,
        metrics_delta,
    })
}

fn capture_phase_metrics() -> PhaseMetrics {
    let metrics = &monitor::GLOBAL_METRICS;
    PhaseMetrics {
        fusion_reverse_scan_count: metrics.fusion_reverse_scan_count.load(Relaxed),
        fusion_reverse_sstable_frontier_probe_count: metrics
            .fusion_reverse_sstable_frontier_probe_count
            .load(Relaxed),
        fusion_reverse_sstable_frontier_in_range_count: metrics
            .fusion_reverse_sstable_frontier_in_range_count
            .load(Relaxed),
        fusion_reverse_sstable_frontier_file_count: metrics
            .fusion_reverse_sstable_frontier_file_count
            .load(Relaxed),
        fusion_reverse_sstable_frontier_tighten_count: metrics
            .fusion_reverse_sstable_frontier_tighten_count
            .load(Relaxed),
        fusion_reverse_sstable_frontier_empty_skip_count: metrics
            .fusion_reverse_sstable_frontier_empty_skip_count
            .load(Relaxed),
        fusion_reverse_sstable_frontier_fail_open_count: metrics
            .fusion_reverse_sstable_frontier_fail_open_count
            .load(Relaxed),
        fusion_reverse_sstable_pending_count: metrics
            .fusion_reverse_sstable_pending_count
            .load(Relaxed),
        fusion_reverse_sstable_activation_count: metrics
            .fusion_reverse_sstable_activation_count
            .load(Relaxed),
        fusion_reverse_sstable_deferred_unopened_count: metrics
            .fusion_reverse_sstable_deferred_unopened_count
            .load(Relaxed),
        fusion_reverse_sstable_activation_equal_frontier_count: metrics
            .fusion_reverse_sstable_activation_equal_frontier_count
            .load(Relaxed),
        fusion_reverse_visible_put_count: metrics.fusion_reverse_visible_put_count.load(Relaxed),
        sstable_iterator_open_count: metrics.sstable_iterator_open_count.load(Relaxed),
        sstable_reverse_iterator_open_count: metrics
            .sstable_reverse_iterator_open_count
            .load(Relaxed),
        sstable_reverse_block_read_count: metrics.sstable_reverse_block_read_count.load(Relaxed),
        sstable_reverse_block_entry_decode_count: metrics
            .sstable_reverse_block_entry_decode_count
            .load(Relaxed),
        sstable_reverse_block_entry_yield_count: metrics
            .sstable_reverse_block_entry_yield_count
            .load(Relaxed),
        compaction_run_count: metrics.compaction_run_count.load(Relaxed),
        live_sstable_count: metrics.live_sstable_count.load(Relaxed),
    }
}

fn payload(base: &[u8], len: usize, salt: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len.max(base.len()));
    out.extend_from_slice(base);
    while out.len() < len {
        out.extend_from_slice(format!("_{salt:08x}").as_bytes());
    }
    out.truncate(len.max(base.len()));
    out
}

fn checksum_keys(keys: &[String]) -> String {
    let mut hasher = Sha256::new();
    for key in keys {
        hasher.update((key.len() as u64).to_le_bytes());
        hasher.update(key.as_bytes());
    }
    format!("{:x}", hasher.finalize())
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
    sorted[idx.min(sorted.len() - 1)]
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

async fn cleanup_dir(path: PathBuf) {
    if Path::new(&path).exists() {
        let _ = tokio::fs::remove_dir_all(path).await;
    }
}
