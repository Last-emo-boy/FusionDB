use fusiondb::monitor;
use fusiondb::storage::sstable::{BlockCache, SsTable, SsTableBuilder, SsTableReverseFrontierKind};
use moka::sync::Cache;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Instant;

const TS_SIZE: usize = 8;
const START: &[u8] = b"data:frontier:";
const END: &[u8] = b"data:frontier;";
const LIMIT: usize = 1;

#[derive(Clone, Copy)]
enum FrontierPolicy {
    RangeLocal,
    FileLevel,
}

#[derive(Default, Serialize)]
struct PhaseMetrics {
    fusion_reverse_sstable_frontier_probe_count: u64,
    fusion_reverse_sstable_frontier_in_range_count: u64,
    fusion_reverse_sstable_frontier_file_count: u64,
    fusion_reverse_sstable_frontier_tighten_count: u64,
    fusion_reverse_sstable_frontier_empty_skip_count: u64,
    fusion_reverse_sstable_frontier_fail_open_count: u64,
    fusion_reverse_sstable_pending_count: u64,
    fusion_reverse_sstable_activation_count: u64,
    fusion_reverse_sstable_deferred_unopened_count: u64,
    sstable_iterator_open_count: u64,
    sstable_reverse_iterator_open_count: u64,
    sstable_reverse_block_read_count: u64,
    sstable_reverse_block_entry_decode_count: u64,
    sstable_reverse_block_entry_yield_count: u64,
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
    iterator_opens: u64,
    activations: u64,
    deferred_unopened: u64,
    result_keys: Vec<String>,
    result_checksum: String,
    metrics_delta: PhaseMetrics,
}

#[derive(Serialize)]
struct BenchReport {
    benchmark: String,
    config: BenchConfig,
    optimized: PhaseReport,
    file_level_control: PhaseReport,
    activation_reduction: i64,
    activation_reduction_ratio: f64,
    same_results: bool,
}

#[derive(Clone, Serialize)]
struct BenchConfig {
    decoy_sstables: usize,
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
            let _ = std::fs::remove_file(path.with_extension("rseek"));
        }
        if let Some(dir) = self.paths.first().and_then(|path| path.parent()) {
            let _ = std::fs::remove_dir(dir);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BenchConfig {
        decoy_sstables: env_usize("BENCH_SST_REVERSE_FRONTIER_DECOYS", 64),
        iters: env_usize("BENCH_SST_REVERSE_FRONTIER_ITERS", 5),
        payload_bytes: env_usize("BENCH_SST_REVERSE_FRONTIER_PAYLOAD_BYTES", 256),
        block_cache_capacity: env_u64("BENCH_SST_REVERSE_FRONTIER_CACHE_BLOCKS", 1_000_000),
    };

    let base_dir = std::env::temp_dir().join(format!(
        "fusiondb_sstable_reverse_frontier_bench_{}",
        uuid::Uuid::new_v4()
    ));
    tokio::fs::create_dir_all(&base_dir).await?;
    let table_set = build_table_set(&base_dir, &config).await?;

    let optimized = run_phase("optimized", &table_set, &config, FrontierPolicy::RangeLocal).await?;
    let file_level_control = run_phase(
        "file_level_control",
        &table_set,
        &config,
        FrontierPolicy::FileLevel,
    )
    .await?;

    let activation_reduction = file_level_control.activations as i64 - optimized.activations as i64;
    let activation_reduction_ratio = if optimized.activations > 0 {
        file_level_control.activations as f64 / optimized.activations as f64
    } else {
        0.0
    };
    let same_results = optimized.result_checksum == file_level_control.result_checksum;

    let report = BenchReport {
        benchmark: "sstable_reverse_frontier_micro".to_string(),
        config,
        optimized,
        file_level_control,
        activation_reduction,
        activation_reduction_ratio,
        same_results,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn build_table_set(
    base_dir: &Path,
    config: &BenchConfig,
) -> Result<TableSet, Box<dyn std::error::Error>> {
    let cache = Arc::new(Cache::new(config.block_cache_capacity));
    let mut tables = Vec::with_capacity(config.decoy_sstables + 1);
    let mut paths = Vec::with_capacity(config.decoy_sstables + 1);

    let target_path = base_dir.join("target.sst");
    write_table(
        &target_path,
        &[vec![
            (b"data:frontier:900".as_slice(), b"target-900".as_slice()),
            (b"data:frontier:901".as_slice(), b"target-901".as_slice()),
        ]],
        config.payload_bytes,
        0,
    )
    .await?;
    tables.push(SsTable::open(target_path.clone(), 0, cache.clone()).await?);
    paths.push(target_path);

    for decoy_idx in 0..config.decoy_sstables {
        let path = base_dir.join(format!("decoy_{decoy_idx:06}.sst"));
        let low_key = format!("data:frontier:1{decoy_idx:08}");
        let high_key = format!("data:frontier;sentinel:{decoy_idx:08}");
        write_table(
            &path,
            &[
                vec![(low_key.as_bytes(), b"decoy-low".as_slice())],
                vec![(high_key.as_bytes(), b"sentinel".as_slice())],
            ],
            config.payload_bytes,
            decoy_idx + 1,
        )
        .await?;
        tables.push(SsTable::open(path.clone(), decoy_idx as u64 + 1, cache.clone()).await?);
        paths.push(path);
    }

    Ok(TableSet {
        tables,
        cache,
        paths,
    })
}

async fn write_table(
    path: &Path,
    blocks: &[Vec<(&[u8], &[u8])>],
    payload_bytes: usize,
    salt: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = SsTableBuilder::new(path.to_path_buf());
    builder.enable_user_key_prefix_filter(TS_SIZE);
    for (block_idx, block_entries) in blocks.iter().enumerate() {
        let mut block = Vec::new();
        let mut first_key = None;
        for (entry_idx, (user_key, value)) in block_entries.iter().enumerate() {
            let key = internal_key(user_key);
            let payload = payload(value, payload_bytes, salt, block_idx, entry_idx);
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            builder.add_key(&key);
            append_block_entry(&mut block, &key, &payload);
        }
        builder
            .flush_block(
                first_key.expect("benchmark block must not be empty"),
                block_entries.len() as u32,
                &block,
            )
            .await?;
    }
    builder.finish().await?;
    Ok(())
}

async fn run_phase(
    name: &str,
    table_set: &TableSet,
    config: &BenchConfig,
    policy: FrontierPolicy,
) -> Result<PhaseReport, Box<dyn std::error::Error>> {
    let mut times_ms = Vec::with_capacity(config.iters);
    let mut row_count = 0usize;
    let mut last_result_keys = Vec::new();

    monitor::GLOBAL_METRICS.reset();
    for _ in 0..config.iters {
        table_set.cache.invalidate_all();
        table_set.cache.run_pending_tasks();

        let started = Instant::now();
        let result_keys = run_lazy_reverse_policy(table_set, policy).await?;
        times_ms.push(started.elapsed().as_secs_f64() * 1000.0);
        row_count += result_keys.len();
        last_result_keys = result_keys;
    }

    let metrics_delta = capture_phase_metrics();
    let activations = metrics_delta.fusion_reverse_sstable_activation_count;
    let deferred_unopened = metrics_delta.fusion_reverse_sstable_deferred_unopened_count;
    let iterator_opens = metrics_delta.sstable_reverse_iterator_open_count;
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
        iterator_opens,
        activations,
        deferred_unopened,
        result_keys: last_result_keys,
        result_checksum,
        metrics_delta,
    })
}

async fn run_lazy_reverse_policy(
    table_set: &TableSet,
    policy: FrontierPolicy,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut pending = Vec::with_capacity(table_set.tables.len());
    for (table_idx, table) in table_set.tables.iter().enumerate() {
        monitor::inc_fusion_reverse_sstable_frontier_probe();
        let Some(frontier) = frontier_for_policy(table, policy) else {
            monitor::inc_fusion_reverse_sstable_frontier_empty_skip();
            continue;
        };
        if matches!(policy, FrontierPolicy::RangeLocal) {
            match frontier.kind {
                SsTableReverseFrontierKind::BlockProperty => {
                    monitor::inc_fusion_reverse_sstable_frontier_in_range();
                }
                SsTableReverseFrontierKind::FileFallback => {
                    monitor::inc_fusion_reverse_sstable_frontier_file();
                    monitor::inc_fusion_reverse_sstable_frontier_fail_open();
                }
            }
        } else {
            monitor::inc_fusion_reverse_sstable_frontier_file();
        }
        if frontier.user_key.as_slice() < user_part(&table.meta.last_key) {
            monitor::inc_fusion_reverse_sstable_frontier_tighten();
        }
        monitor::inc_fusion_reverse_sstable_pending();
        pending.push((frontier.user_key, table_idx));
    }

    let mut active = Vec::new();
    loop {
        pending.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
        let pending_top = pending.first().map(|(frontier, _)| frontier.as_slice());
        let active_top = active.iter().map(Vec::as_slice).max();
        let should_activate = match (active_top, pending_top) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(active_top), Some(pending_top)) => pending_top >= active_top,
        };
        if !should_activate {
            break;
        }
        let (_frontier, table_idx) = pending.remove(0);
        let table = &table_set.tables[table_idx];
        let mut iter = table
            .new_user_key_range_reverse_iterator(Some(START), Some(END), TS_SIZE)
            .await?;
        monitor::inc_sstable_iterator_open();
        monitor::inc_sstable_reverse_iterator_open();
        monitor::inc_fusion_reverse_sstable_activation();
        if let Some((key, _value)) = iter.next().await? {
            active.push(user_part(&key).to_vec());
        }
    }

    monitor::add_fusion_reverse_sstable_deferred_unopened(pending.len() as u64);
    active.sort_by(|left, right| right.cmp(left));
    active.truncate(LIMIT);
    Ok(active
        .into_iter()
        .map(|key| String::from_utf8_lossy(&key).into_owned())
        .collect())
}

fn frontier_for_policy(
    table: &SsTable,
    policy: FrontierPolicy,
) -> Option<fusiondb::storage::sstable::SsTableReverseFrontier> {
    match policy {
        FrontierPolicy::RangeLocal => table.reverse_frontier_for_range(START, END, TS_SIZE),
        FrontierPolicy::FileLevel => {
            let table_min = user_part(&table.meta.first_key);
            let table_max = user_part(&table.meta.last_key);
            if table_max < START || table_min >= END {
                return None;
            }
            Some(fusiondb::storage::sstable::SsTableReverseFrontier {
                user_key: if table_max < END {
                    table_max.to_vec()
                } else {
                    END.to_vec()
                },
                kind: SsTableReverseFrontierKind::FileFallback,
            })
        }
    }
}

fn capture_phase_metrics() -> PhaseMetrics {
    let metrics = &monitor::GLOBAL_METRICS;
    PhaseMetrics {
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

fn user_part(key: &[u8]) -> &[u8] {
    key.len()
        .checked_sub(TS_SIZE)
        .map(|len| &key[..len])
        .unwrap_or(key)
}

fn payload(base: &[u8], len: usize, salt: usize, block_idx: usize, entry_idx: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len.max(base.len()));
    out.extend_from_slice(base);
    while out.len() < len {
        out.extend_from_slice(format!("_{salt:08x}_{block_idx:04x}_{entry_idx:04x}").as_bytes());
    }
    out.truncate(len.max(base.len()));
    out
}

fn checksum_keys(keys: &[String]) -> String {
    use sha2::{Digest, Sha256};

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
