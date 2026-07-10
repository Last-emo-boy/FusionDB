use lazy_static::lazy_static;
use serde::Serialize;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

/// Default slow query threshold: 100ms
const DEFAULT_SLOW_QUERY_THRESHOLD_MS: u64 = 100;

lazy_static! {
    pub static ref GLOBAL_METRICS: Metrics = Metrics::default();
    pub static ref SLOW_QUERY_LOG: SlowQueryLog =
        SlowQueryLog::new(DEFAULT_SLOW_QUERY_THRESHOLD_MS);
}

#[derive(Default, Serialize)]
pub struct Metrics {
    pub sql_parse_count: AtomicU64,
    pub sql_plan_count: AtomicU64,
    pub row_read_count: AtomicU64,
    pub row_cache_hit_count: AtomicU64,
    pub query_result_cache_eligible_count: AtomicU64,
    pub query_result_cache_hit_count: AtomicU64,
    pub query_result_cache_miss_count: AtomicU64,
    pub query_result_cache_stale_count: AtomicU64,
    pub query_result_cache_insert_count: AtomicU64,
    pub query_result_cache_invalidation_count: AtomicU64,
    pub block_cache_hit_count: AtomicU64,
    pub block_cache_miss_count: AtomicU64,
    pub block_cache_insert_count: AtomicU64,
    pub block_cache_insert_bytes: AtomicU64,
    pub block_cache_fill_skip_count: AtomicU64,
    pub block_cache_eviction_count: AtomicU64,
    pub block_cache_eviction_bytes: AtomicU64,
    pub sstable_block_file_open_count: AtomicU64,
    pub sstable_block_read_bytes: AtomicU64,
    pub sstable_open_count: AtomicU64,
    pub sstable_open_total_us: AtomicU64,
    pub sstable_open_index_bytes: AtomicU64,
    pub sstable_open_index_read_us: AtomicU64,
    pub sstable_open_index_decode_us: AtomicU64,
    pub sstable_open_filter_bytes: AtomicU64,
    pub sstable_open_filter_read_us: AtomicU64,
    pub sstable_open_filter_decode_us: AtomicU64,
    pub sstable_open_meta_bytes: AtomicU64,
    pub sstable_open_meta_read_us: AtomicU64,
    pub sstable_open_meta_decode_us: AtomicU64,
    pub sstable_open_index_entries: AtomicU64,
    pub sstable_open_block_property_count: AtomicU64,
    pub sstable_index_cache_hit_count: AtomicU64,
    pub sstable_index_cache_miss_count: AtomicU64,
    pub sstable_index_cache_stale_count: AtomicU64,
    pub sstable_index_cache_invalid_count: AtomicU64,
    pub sstable_index_cache_write_count: AtomicU64,
    pub sstable_index_cache_write_error_count: AtomicU64,
    pub sstable_prefix_filter_check_count: AtomicU64,
    pub sstable_prefix_filter_positive_count: AtomicU64,
    pub sstable_prefix_filter_skip_count: AtomicU64,
    pub sstable_prefix_filter_fail_open_count: AtomicU64,
    pub sstable_index_prefix_filter_check_count: AtomicU64,
    pub sstable_index_prefix_filter_positive_count: AtomicU64,
    pub sstable_index_prefix_filter_skip_count: AtomicU64,
    pub sstable_index_prefix_filter_fail_open_count: AtomicU64,
    pub sstable_user_key_filter_check_count: AtomicU64,
    pub sstable_user_key_filter_positive_count: AtomicU64,
    pub sstable_user_key_filter_skip_count: AtomicU64,
    pub sstable_user_key_filter_fail_open_count: AtomicU64,
    pub sstable_block_prefix_filter_check_count: AtomicU64,
    pub sstable_block_prefix_filter_positive_count: AtomicU64,
    pub sstable_block_prefix_filter_skip_count: AtomicU64,
    pub sstable_block_prefix_filter_fail_open_count: AtomicU64,
    pub sstable_block_index_prefix_filter_check_count: AtomicU64,
    pub sstable_block_index_prefix_filter_positive_count: AtomicU64,
    pub sstable_block_index_prefix_filter_skip_count: AtomicU64,
    pub sstable_block_index_prefix_filter_fail_open_count: AtomicU64,
    pub sstable_block_zone_map_filter_check_count: AtomicU64,
    pub sstable_block_zone_map_filter_positive_count: AtomicU64,
    pub sstable_block_zone_map_filter_skip_count: AtomicU64,
    pub sstable_block_zone_map_filter_fail_open_count: AtomicU64,
    pub sstable_block_zone_map_metadata_bytes: AtomicU64,
    pub sstable_block_zone_map_mvcc_overlap_fail_open_count: AtomicU64,
    pub sstable_block_zone_map_mvcc_boundary_split_fail_open_count: AtomicU64,
    pub sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count: AtomicU64,
    pub sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count: AtomicU64,
    pub sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count: AtomicU64,
    pub sstable_block_zone_map_schema_fail_open_count: AtomicU64,
    pub sstable_point_probe_count: AtomicU64,
    pub sstable_point_overlap_skip_count: AtomicU64,
    pub sstable_range_probe_count: AtomicU64,
    pub sstable_range_overlap_skip_count: AtomicU64,
    pub sstable_iterator_open_count: AtomicU64,
    pub sstable_reverse_iterator_open_count: AtomicU64,
    pub sstable_reverse_block_read_count: AtomicU64,
    pub sstable_reverse_block_entry_decode_count: AtomicU64,
    pub sstable_reverse_block_entry_yield_count: AtomicU64,
    pub sstable_reverse_block_span_scan_count: AtomicU64,
    pub sstable_reverse_block_span_scan_entry_count: AtomicU64,
    pub sstable_reverse_block_span_materialize_entry_count: AtomicU64,
    pub sstable_reverse_seek_sidecar_hit_count: AtomicU64,
    pub sstable_reverse_seek_sidecar_miss_count: AtomicU64,
    pub sstable_reverse_seek_sidecar_stale_count: AtomicU64,
    pub sstable_reverse_seek_sidecar_invalid_count: AtomicU64,
    pub sstable_reverse_seek_sidecar_write_count: AtomicU64,
    pub sstable_reverse_seek_sidecar_write_error_count: AtomicU64,
    pub sstable_reverse_seek_sidecar_use_count: AtomicU64,
    pub sstable_reverse_seek_sidecar_fail_open_count: AtomicU64,
    pub sstable_reverse_seek_sidecar_index_entry_count: AtomicU64,
    pub sstable_reverse_seek_sidecar_entry_materialize_count: AtomicU64,
    pub sstable_reverse_seek_sidecar_offset_probe_count: AtomicU64,
    pub fusion_reverse_scan_count: AtomicU64,
    pub fusion_reverse_source_open_count: AtomicU64,
    pub fusion_reverse_sstable_frontier_probe_count: AtomicU64,
    pub fusion_reverse_sstable_frontier_in_range_count: AtomicU64,
    pub fusion_reverse_sstable_frontier_file_count: AtomicU64,
    pub fusion_reverse_sstable_frontier_tighten_count: AtomicU64,
    pub fusion_reverse_sstable_frontier_empty_skip_count: AtomicU64,
    pub fusion_reverse_sstable_frontier_fail_open_count: AtomicU64,
    pub fusion_reverse_sstable_pending_count: AtomicU64,
    pub fusion_reverse_sstable_activation_count: AtomicU64,
    pub fusion_reverse_sstable_deferred_unopened_count: AtomicU64,
    pub fusion_reverse_sstable_activation_equal_frontier_count: AtomicU64,
    pub fusion_reverse_raw_entry_read_count: AtomicU64,
    pub fusion_reverse_visible_candidate_count: AtomicU64,
    pub fusion_reverse_visible_put_count: AtomicU64,
    pub index_key_stream_entry_visit_count: AtomicU64,
    pub index_ordered_topk_scan_count: AtomicU64,
    pub index_ordered_topk_entry_visit_count: AtomicU64,
    pub index_ordered_topk_reverse_scan_count: AtomicU64,
    pub index_ordered_topk_index_only_row_count: AtomicU64,
    pub index_ordered_topk_base_row_fetch_count: AtomicU64,
    pub index_group_count_summary_entry_visit_count: AtomicU64,
    pub index_loose_seek_count: AtomicU64,
    pub index_loose_value_count: AtomicU64,
    pub index_loose_run_skip_count: AtomicU64,
    pub compaction_run_count: AtomicU64,
    pub compaction_input_bytes: AtomicU64,
    pub compaction_output_bytes: AtomicU64,
    pub compaction_dropped_version_count: AtomicU64,
    pub live_sstable_count: AtomicU64,
    pub sstable_manifest_load_count: AtomicU64,
    pub sstable_manifest_load_total_us: AtomicU64,
    pub sstable_manifest_load_error_count: AtomicU64,
    pub sstable_manifest_live_file_count: AtomicU64,
    pub sstable_manifest_legacy_scan_count: AtomicU64,
    pub sstable_manifest_legacy_scan_candidate_count: AtomicU64,
    pub sstable_manifest_open_error_count: AtomicU64,
    pub row_write_count: AtomicU64,
    pub fts_search_count: AtomicU64,
    pub fts_doc_hits: AtomicU64,
    pub wal_write_count: AtomicU64,
    pub wal_write_bytes: AtomicU64,
    pub wal_replay_count: AtomicU64,
    pub wal_replay_total_us: AtomicU64,
    pub wal_replay_segment_count: AtomicU64,
    pub wal_replay_bytes: AtomicU64,
    pub wal_replay_valid_bytes: AtomicU64,
    pub wal_replay_last_segment_id: AtomicU64,
    pub wal_replay_last_valid_offset: AtomicU64,
    pub wal_replay_entry_count: AtomicU64,
    pub wal_replay_put_count: AtomicU64,
    pub wal_replay_delete_count: AtomicU64,
    pub wal_replay_partial_tail_count: AtomicU64,
    pub wal_replay_truncate_count: AtomicU64,
    pub wal_replay_error_count: AtomicU64,
    pub wal_replay_apply_count: AtomicU64,
    pub wal_replay_apply_total_us: AtomicU64,
    pub wal_replay_max_ts: AtomicU64,
    pub query_count: AtomicU64,
    pub slow_query_count: AtomicU64,
    pub query_total_us: AtomicU64,
    pub query_sort_fallback_count: AtomicU64,
    pub pg_active_connection_count: AtomicU64,
    pub pg_connection_rejected_count: AtomicU64,
    pub pg_connection_limit: AtomicU64,
}

/// A single slow query log entry.
#[derive(Debug, Clone, Serialize)]
pub struct SlowQueryEntry {
    pub sql: String,
    pub duration_ms: f64,
    pub timestamp: String,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SstableOpenStats {
    pub total_us: u64,
    pub index_bytes: u64,
    pub index_read_us: u64,
    pub index_decode_us: u64,
    pub filter_bytes: u64,
    pub filter_read_us: u64,
    pub filter_decode_us: u64,
    pub meta_bytes: u64,
    pub meta_read_us: u64,
    pub meta_decode_us: u64,
    pub index_entries: u64,
    pub block_property_count: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct WalReplayStats {
    pub total_us: u64,
    pub segment_count: u64,
    pub bytes: u64,
    pub valid_bytes: u64,
    pub last_segment_id: u64,
    pub last_valid_offset: u64,
    pub entry_count: u64,
    pub put_count: u64,
    pub delete_count: u64,
    pub partial_tail_count: u64,
    pub truncate_count: u64,
    pub error_count: u64,
}

/// Ring buffer of recent slow queries.
pub struct SlowQueryLog {
    entries: Mutex<Vec<SlowQueryEntry>>,
    threshold_ms: AtomicU64,
    max_entries: usize,
}

impl SlowQueryLog {
    pub fn new(threshold_ms: u64) -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            threshold_ms: AtomicU64::new(threshold_ms),
            max_entries: 100,
        }
    }

    pub fn set_threshold_ms(&self, ms: u64) {
        self.threshold_ms.store(ms, Ordering::Relaxed);
    }

    pub fn threshold_ms(&self) -> u64 {
        self.threshold_ms.load(Ordering::Relaxed)
    }

    pub fn record(&self, sql: &str, duration: Duration) {
        let ms = duration.as_secs_f64() * 1000.0;
        GLOBAL_METRICS.query_count.fetch_add(1, Ordering::Relaxed);
        GLOBAL_METRICS
            .query_total_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);

        let threshold = self.threshold_ms.load(Ordering::Relaxed) as f64;
        if ms >= threshold {
            GLOBAL_METRICS
                .slow_query_count
                .fetch_add(1, Ordering::Relaxed);

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let entry = SlowQueryEntry {
                sql: if sql.len() > 500 {
                    format!("{}...", &sql[..500])
                } else {
                    sql.to_string()
                },
                duration_ms: ms,
                timestamp: format!("{:.3}", now.as_secs_f64()),
            };

            eprintln!("[slow-query] {:.2}ms | {}", ms, &entry.sql);

            if let Ok(mut entries) = self.entries.lock() {
                if entries.len() >= self.max_entries {
                    entries.remove(0);
                }
                entries.push(entry);
            }
        }
    }

    pub fn recent(&self) -> Vec<SlowQueryEntry> {
        self.entries.lock().map(|e| e.clone()).unwrap_or_default()
    }

    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }
}

/// Record a query execution for monitoring.
pub fn record_query(sql: &str, duration: Duration) {
    SLOW_QUERY_LOG.record(sql, duration);
}

#[derive(Default)]
struct LocalMetrics {
    sql_parse_count: u64,
    sql_plan_count: u64,
    row_read_count: u64,
    row_cache_hit_count: u64,
    row_write_count: u64,
    fts_search_count: u64,
    fts_doc_hits: u64,
    wal_write_count: u64,
    wal_write_bytes: u64,
}

thread_local! {
    static LOCAL_METRICS: RefCell<LocalMetrics> = RefCell::new(LocalMetrics::default());
}

const FLUSH_THRESHOLD: u64 = 100;

impl Metrics {
    pub fn reset(&self) {
        self.sql_parse_count.store(0, Ordering::Relaxed);
        self.sql_plan_count.store(0, Ordering::Relaxed);
        self.row_read_count.store(0, Ordering::Relaxed);
        self.row_cache_hit_count.store(0, Ordering::Relaxed);
        self.query_result_cache_eligible_count
            .store(0, Ordering::Relaxed);
        self.query_result_cache_hit_count
            .store(0, Ordering::Relaxed);
        self.query_result_cache_miss_count
            .store(0, Ordering::Relaxed);
        self.query_result_cache_stale_count
            .store(0, Ordering::Relaxed);
        self.query_result_cache_insert_count
            .store(0, Ordering::Relaxed);
        self.query_result_cache_invalidation_count
            .store(0, Ordering::Relaxed);
        self.block_cache_hit_count.store(0, Ordering::Relaxed);
        self.block_cache_miss_count.store(0, Ordering::Relaxed);
        self.block_cache_insert_count.store(0, Ordering::Relaxed);
        self.block_cache_insert_bytes.store(0, Ordering::Relaxed);
        self.block_cache_fill_skip_count.store(0, Ordering::Relaxed);
        self.block_cache_eviction_count.store(0, Ordering::Relaxed);
        self.block_cache_eviction_bytes.store(0, Ordering::Relaxed);
        self.sstable_block_file_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_read_bytes.store(0, Ordering::Relaxed);
        self.sstable_open_count.store(0, Ordering::Relaxed);
        self.sstable_open_total_us.store(0, Ordering::Relaxed);
        self.sstable_open_index_bytes.store(0, Ordering::Relaxed);
        self.sstable_open_index_read_us.store(0, Ordering::Relaxed);
        self.sstable_open_index_decode_us
            .store(0, Ordering::Relaxed);
        self.sstable_open_filter_bytes.store(0, Ordering::Relaxed);
        self.sstable_open_filter_read_us.store(0, Ordering::Relaxed);
        self.sstable_open_filter_decode_us
            .store(0, Ordering::Relaxed);
        self.sstable_open_meta_bytes.store(0, Ordering::Relaxed);
        self.sstable_open_meta_read_us.store(0, Ordering::Relaxed);
        self.sstable_open_meta_decode_us.store(0, Ordering::Relaxed);
        self.sstable_open_index_entries.store(0, Ordering::Relaxed);
        self.sstable_open_block_property_count
            .store(0, Ordering::Relaxed);
        self.sstable_index_cache_hit_count
            .store(0, Ordering::Relaxed);
        self.sstable_index_cache_miss_count
            .store(0, Ordering::Relaxed);
        self.sstable_index_cache_stale_count
            .store(0, Ordering::Relaxed);
        self.sstable_index_cache_invalid_count
            .store(0, Ordering::Relaxed);
        self.sstable_index_cache_write_count
            .store(0, Ordering::Relaxed);
        self.sstable_index_cache_write_error_count
            .store(0, Ordering::Relaxed);
        self.sstable_prefix_filter_check_count
            .store(0, Ordering::Relaxed);
        self.sstable_prefix_filter_positive_count
            .store(0, Ordering::Relaxed);
        self.sstable_prefix_filter_skip_count
            .store(0, Ordering::Relaxed);
        self.sstable_prefix_filter_fail_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_index_prefix_filter_check_count
            .store(0, Ordering::Relaxed);
        self.sstable_index_prefix_filter_positive_count
            .store(0, Ordering::Relaxed);
        self.sstable_index_prefix_filter_skip_count
            .store(0, Ordering::Relaxed);
        self.sstable_index_prefix_filter_fail_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_user_key_filter_check_count
            .store(0, Ordering::Relaxed);
        self.sstable_user_key_filter_positive_count
            .store(0, Ordering::Relaxed);
        self.sstable_user_key_filter_skip_count
            .store(0, Ordering::Relaxed);
        self.sstable_user_key_filter_fail_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_prefix_filter_check_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_prefix_filter_positive_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_prefix_filter_skip_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_prefix_filter_fail_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_index_prefix_filter_check_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_index_prefix_filter_positive_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_index_prefix_filter_skip_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_index_prefix_filter_fail_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_zone_map_filter_check_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_zone_map_filter_positive_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_zone_map_filter_skip_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_zone_map_filter_fail_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_zone_map_metadata_bytes
            .store(0, Ordering::Relaxed);
        self.sstable_block_zone_map_mvcc_overlap_fail_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_zone_map_mvcc_boundary_split_fail_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_block_zone_map_schema_fail_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_point_probe_count.store(0, Ordering::Relaxed);
        self.sstable_point_overlap_skip_count
            .store(0, Ordering::Relaxed);
        self.sstable_range_probe_count.store(0, Ordering::Relaxed);
        self.sstable_range_overlap_skip_count
            .store(0, Ordering::Relaxed);
        self.sstable_iterator_open_count.store(0, Ordering::Relaxed);
        self.sstable_reverse_iterator_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_block_read_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_block_entry_decode_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_block_entry_yield_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_block_span_scan_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_block_span_scan_entry_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_block_span_materialize_entry_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_seek_sidecar_hit_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_seek_sidecar_miss_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_seek_sidecar_stale_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_seek_sidecar_invalid_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_seek_sidecar_write_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_seek_sidecar_write_error_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_seek_sidecar_use_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_seek_sidecar_fail_open_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_seek_sidecar_index_entry_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_seek_sidecar_entry_materialize_count
            .store(0, Ordering::Relaxed);
        self.sstable_reverse_seek_sidecar_offset_probe_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_scan_count.store(0, Ordering::Relaxed);
        self.fusion_reverse_source_open_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_sstable_frontier_probe_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_sstable_frontier_in_range_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_sstable_frontier_file_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_sstable_frontier_tighten_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_sstable_frontier_empty_skip_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_sstable_frontier_fail_open_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_sstable_pending_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_sstable_activation_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_sstable_deferred_unopened_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_sstable_activation_equal_frontier_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_raw_entry_read_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_visible_candidate_count
            .store(0, Ordering::Relaxed);
        self.fusion_reverse_visible_put_count
            .store(0, Ordering::Relaxed);
        self.index_key_stream_entry_visit_count
            .store(0, Ordering::Relaxed);
        self.index_ordered_topk_scan_count
            .store(0, Ordering::Relaxed);
        self.index_ordered_topk_entry_visit_count
            .store(0, Ordering::Relaxed);
        self.index_ordered_topk_reverse_scan_count
            .store(0, Ordering::Relaxed);
        self.index_ordered_topk_index_only_row_count
            .store(0, Ordering::Relaxed);
        self.index_ordered_topk_base_row_fetch_count
            .store(0, Ordering::Relaxed);
        self.index_group_count_summary_entry_visit_count
            .store(0, Ordering::Relaxed);
        self.index_loose_seek_count.store(0, Ordering::Relaxed);
        self.index_loose_value_count.store(0, Ordering::Relaxed);
        self.index_loose_run_skip_count.store(0, Ordering::Relaxed);
        self.compaction_run_count.store(0, Ordering::Relaxed);
        self.compaction_input_bytes.store(0, Ordering::Relaxed);
        self.compaction_output_bytes.store(0, Ordering::Relaxed);
        self.compaction_dropped_version_count
            .store(0, Ordering::Relaxed);
        self.live_sstable_count.store(0, Ordering::Relaxed);
        self.sstable_manifest_load_count.store(0, Ordering::Relaxed);
        self.sstable_manifest_load_total_us
            .store(0, Ordering::Relaxed);
        self.sstable_manifest_load_error_count
            .store(0, Ordering::Relaxed);
        self.sstable_manifest_live_file_count
            .store(0, Ordering::Relaxed);
        self.sstable_manifest_legacy_scan_count
            .store(0, Ordering::Relaxed);
        self.sstable_manifest_legacy_scan_candidate_count
            .store(0, Ordering::Relaxed);
        self.sstable_manifest_open_error_count
            .store(0, Ordering::Relaxed);
        self.row_write_count.store(0, Ordering::Relaxed);
        self.fts_search_count.store(0, Ordering::Relaxed);
        self.fts_doc_hits.store(0, Ordering::Relaxed);
        self.wal_write_count.store(0, Ordering::Relaxed);
        self.wal_write_bytes.store(0, Ordering::Relaxed);
        self.wal_replay_count.store(0, Ordering::Relaxed);
        self.wal_replay_total_us.store(0, Ordering::Relaxed);
        self.wal_replay_segment_count.store(0, Ordering::Relaxed);
        self.wal_replay_bytes.store(0, Ordering::Relaxed);
        self.wal_replay_valid_bytes.store(0, Ordering::Relaxed);
        self.wal_replay_last_segment_id.store(0, Ordering::Relaxed);
        self.wal_replay_last_valid_offset
            .store(0, Ordering::Relaxed);
        self.wal_replay_entry_count.store(0, Ordering::Relaxed);
        self.wal_replay_put_count.store(0, Ordering::Relaxed);
        self.wal_replay_delete_count.store(0, Ordering::Relaxed);
        self.wal_replay_partial_tail_count
            .store(0, Ordering::Relaxed);
        self.wal_replay_truncate_count.store(0, Ordering::Relaxed);
        self.wal_replay_error_count.store(0, Ordering::Relaxed);
        self.wal_replay_apply_count.store(0, Ordering::Relaxed);
        self.wal_replay_apply_total_us.store(0, Ordering::Relaxed);
        self.wal_replay_max_ts.store(0, Ordering::Relaxed);
        self.query_count.store(0, Ordering::Relaxed);
        self.slow_query_count.store(0, Ordering::Relaxed);
        self.query_total_us.store(0, Ordering::Relaxed);
        self.query_sort_fallback_count.store(0, Ordering::Relaxed);
        self.pg_active_connection_count.store(0, Ordering::Relaxed);
        self.pg_connection_rejected_count
            .store(0, Ordering::Relaxed);
        self.pg_connection_limit.store(0, Ordering::Relaxed);
    }
}

pub fn inc_parse() {
    LOCAL_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.sql_parse_count += 1;
        if m.sql_parse_count >= FLUSH_THRESHOLD {
            GLOBAL_METRICS
                .sql_parse_count
                .fetch_add(m.sql_parse_count, Ordering::Relaxed);
            m.sql_parse_count = 0;
        }
    })
}

pub fn inc_plan() {
    LOCAL_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.sql_plan_count += 1;
        if m.sql_plan_count >= FLUSH_THRESHOLD {
            GLOBAL_METRICS
                .sql_plan_count
                .fetch_add(m.sql_plan_count, Ordering::Relaxed);
            m.sql_plan_count = 0;
        }
    })
}

pub fn inc_row_read() {
    LOCAL_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.row_read_count += 1;
        if m.row_read_count >= FLUSH_THRESHOLD {
            GLOBAL_METRICS
                .row_read_count
                .fetch_add(m.row_read_count, Ordering::Relaxed);
            m.row_read_count = 0;
        }
    })
}

pub fn inc_row_cache_hit() {
    LOCAL_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.row_cache_hit_count += 1;
        if m.row_cache_hit_count >= FLUSH_THRESHOLD {
            GLOBAL_METRICS
                .row_cache_hit_count
                .fetch_add(m.row_cache_hit_count, Ordering::Relaxed);
            m.row_cache_hit_count = 0;
        }
    })
}

pub fn inc_query_result_cache_eligible() {
    GLOBAL_METRICS
        .query_result_cache_eligible_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_query_result_cache_hit() {
    GLOBAL_METRICS
        .query_result_cache_hit_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_query_result_cache_miss() {
    GLOBAL_METRICS
        .query_result_cache_miss_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_query_result_cache_stale() {
    GLOBAL_METRICS
        .query_result_cache_stale_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_query_result_cache_insert() {
    GLOBAL_METRICS
        .query_result_cache_insert_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_query_result_cache_invalidation() {
    GLOBAL_METRICS
        .query_result_cache_invalidation_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_block_cache_hit() {
    GLOBAL_METRICS
        .block_cache_hit_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_block_cache_miss() {
    GLOBAL_METRICS
        .block_cache_miss_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_block_cache_insert(bytes: u64) {
    GLOBAL_METRICS
        .block_cache_insert_count
        .fetch_add(1, Ordering::Relaxed);
    GLOBAL_METRICS
        .block_cache_insert_bytes
        .fetch_add(bytes, Ordering::Relaxed);
}

pub fn inc_block_cache_fill_skip() {
    GLOBAL_METRICS
        .block_cache_fill_skip_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_block_cache_eviction(bytes: u64) {
    GLOBAL_METRICS
        .block_cache_eviction_count
        .fetch_add(1, Ordering::Relaxed);
    GLOBAL_METRICS
        .block_cache_eviction_bytes
        .fetch_add(bytes, Ordering::Relaxed);
}

pub fn inc_sstable_block_file_open() {
    GLOBAL_METRICS
        .sstable_block_file_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_read_bytes(bytes: u64) {
    GLOBAL_METRICS
        .sstable_block_read_bytes
        .fetch_add(bytes, Ordering::Relaxed);
}

pub fn record_sstable_open(stats: SstableOpenStats) {
    GLOBAL_METRICS
        .sstable_open_count
        .fetch_add(1, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_open_total_us
        .fetch_add(stats.total_us, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_open_index_bytes
        .fetch_add(stats.index_bytes, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_open_index_read_us
        .fetch_add(stats.index_read_us, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_open_index_decode_us
        .fetch_add(stats.index_decode_us, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_open_filter_bytes
        .fetch_add(stats.filter_bytes, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_open_filter_read_us
        .fetch_add(stats.filter_read_us, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_open_filter_decode_us
        .fetch_add(stats.filter_decode_us, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_open_meta_bytes
        .fetch_add(stats.meta_bytes, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_open_meta_read_us
        .fetch_add(stats.meta_read_us, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_open_meta_decode_us
        .fetch_add(stats.meta_decode_us, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_open_index_entries
        .fetch_add(stats.index_entries, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_open_block_property_count
        .fetch_add(stats.block_property_count, Ordering::Relaxed);
}

pub fn inc_sstable_index_cache_hit() {
    GLOBAL_METRICS
        .sstable_index_cache_hit_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_index_cache_miss() {
    GLOBAL_METRICS
        .sstable_index_cache_miss_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_index_cache_stale() {
    GLOBAL_METRICS
        .sstable_index_cache_stale_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_index_cache_invalid() {
    GLOBAL_METRICS
        .sstable_index_cache_invalid_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_index_cache_write() {
    GLOBAL_METRICS
        .sstable_index_cache_write_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_index_cache_write_error() {
    GLOBAL_METRICS
        .sstable_index_cache_write_error_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_prefix_filter_check() {
    GLOBAL_METRICS
        .sstable_prefix_filter_check_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_prefix_filter_positive() {
    GLOBAL_METRICS
        .sstable_prefix_filter_positive_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_prefix_filter_skip() {
    GLOBAL_METRICS
        .sstable_prefix_filter_skip_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_prefix_filter_fail_open() {
    GLOBAL_METRICS
        .sstable_prefix_filter_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_index_prefix_filter_check() {
    GLOBAL_METRICS
        .sstable_index_prefix_filter_check_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_index_prefix_filter_positive() {
    GLOBAL_METRICS
        .sstable_index_prefix_filter_positive_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_index_prefix_filter_skip() {
    GLOBAL_METRICS
        .sstable_index_prefix_filter_skip_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_index_prefix_filter_fail_open() {
    GLOBAL_METRICS
        .sstable_index_prefix_filter_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_user_key_filter_check() {
    GLOBAL_METRICS
        .sstable_user_key_filter_check_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_user_key_filter_positive() {
    GLOBAL_METRICS
        .sstable_user_key_filter_positive_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_user_key_filter_skip() {
    GLOBAL_METRICS
        .sstable_user_key_filter_skip_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_user_key_filter_fail_open() {
    GLOBAL_METRICS
        .sstable_user_key_filter_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_prefix_filter_check() {
    GLOBAL_METRICS
        .sstable_block_prefix_filter_check_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_prefix_filter_positive() {
    GLOBAL_METRICS
        .sstable_block_prefix_filter_positive_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_prefix_filter_skip() {
    GLOBAL_METRICS
        .sstable_block_prefix_filter_skip_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_prefix_filter_fail_open() {
    GLOBAL_METRICS
        .sstable_block_prefix_filter_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_index_prefix_filter_check() {
    GLOBAL_METRICS
        .sstable_block_index_prefix_filter_check_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_index_prefix_filter_positive() {
    GLOBAL_METRICS
        .sstable_block_index_prefix_filter_positive_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_index_prefix_filter_skip() {
    GLOBAL_METRICS
        .sstable_block_index_prefix_filter_skip_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_index_prefix_filter_fail_open() {
    GLOBAL_METRICS
        .sstable_block_index_prefix_filter_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_zone_map_filter_check() {
    GLOBAL_METRICS
        .sstable_block_zone_map_filter_check_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_zone_map_filter_positive() {
    GLOBAL_METRICS
        .sstable_block_zone_map_filter_positive_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_zone_map_filter_skip() {
    GLOBAL_METRICS
        .sstable_block_zone_map_filter_skip_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_zone_map_filter_fail_open() {
    GLOBAL_METRICS
        .sstable_block_zone_map_filter_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn add_sstable_block_zone_map_metadata_bytes(bytes: u64) {
    GLOBAL_METRICS
        .sstable_block_zone_map_metadata_bytes
        .fetch_add(bytes, Ordering::Relaxed);
}

pub fn inc_sstable_block_zone_map_mvcc_overlap_fail_open() {
    GLOBAL_METRICS
        .sstable_block_zone_map_mvcc_overlap_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_zone_map_mvcc_boundary_split_fail_open() {
    GLOBAL_METRICS
        .sstable_block_zone_map_mvcc_boundary_split_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open() {
    GLOBAL_METRICS
        .sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_zone_map_mvcc_memtable_overlap_fail_open() {
    GLOBAL_METRICS
        .sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_zone_map_mvcc_sstable_overlap_fail_open() {
    GLOBAL_METRICS
        .sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_block_zone_map_schema_fail_open() {
    GLOBAL_METRICS
        .sstable_block_zone_map_schema_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_point_probe() {
    GLOBAL_METRICS
        .sstable_point_probe_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_point_overlap_skip() {
    GLOBAL_METRICS
        .sstable_point_overlap_skip_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_range_probe() {
    GLOBAL_METRICS
        .sstable_range_probe_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_range_overlap_skip() {
    GLOBAL_METRICS
        .sstable_range_overlap_skip_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_iterator_open() {
    GLOBAL_METRICS
        .sstable_iterator_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_reverse_iterator_open() {
    GLOBAL_METRICS
        .sstable_reverse_iterator_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_reverse_block_read() {
    GLOBAL_METRICS
        .sstable_reverse_block_read_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn add_sstable_reverse_block_entry_decodes(count: u64) {
    if count == 0 {
        return;
    }
    GLOBAL_METRICS
        .sstable_reverse_block_entry_decode_count
        .fetch_add(count, Ordering::Relaxed);
}

pub fn add_sstable_reverse_block_entry_yields(count: u64) {
    if count == 0 {
        return;
    }
    GLOBAL_METRICS
        .sstable_reverse_block_entry_yield_count
        .fetch_add(count, Ordering::Relaxed);
}

pub fn add_sstable_reverse_block_span_scans(count: u64) {
    if count == 0 {
        return;
    }
    GLOBAL_METRICS
        .sstable_reverse_block_span_scan_count
        .fetch_add(count, Ordering::Relaxed);
}

pub fn add_sstable_reverse_block_span_scan_entries(count: u64) {
    if count == 0 {
        return;
    }
    GLOBAL_METRICS
        .sstable_reverse_block_span_scan_entry_count
        .fetch_add(count, Ordering::Relaxed);
}

pub fn add_sstable_reverse_block_span_materialize_entries(count: u64) {
    if count == 0 {
        return;
    }
    GLOBAL_METRICS
        .sstable_reverse_block_span_materialize_entry_count
        .fetch_add(count, Ordering::Relaxed);
}

pub fn inc_sstable_reverse_seek_sidecar_hit() {
    GLOBAL_METRICS
        .sstable_reverse_seek_sidecar_hit_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_reverse_seek_sidecar_miss() {
    GLOBAL_METRICS
        .sstable_reverse_seek_sidecar_miss_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_reverse_seek_sidecar_stale() {
    GLOBAL_METRICS
        .sstable_reverse_seek_sidecar_stale_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_reverse_seek_sidecar_invalid() {
    GLOBAL_METRICS
        .sstable_reverse_seek_sidecar_invalid_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_reverse_seek_sidecar_write() {
    GLOBAL_METRICS
        .sstable_reverse_seek_sidecar_write_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_reverse_seek_sidecar_write_error() {
    GLOBAL_METRICS
        .sstable_reverse_seek_sidecar_write_error_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_reverse_seek_sidecar_use() {
    GLOBAL_METRICS
        .sstable_reverse_seek_sidecar_use_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_reverse_seek_sidecar_fail_open() {
    GLOBAL_METRICS
        .sstable_reverse_seek_sidecar_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn add_sstable_reverse_seek_sidecar_index_entries(count: u64) {
    if count == 0 {
        return;
    }
    GLOBAL_METRICS
        .sstable_reverse_seek_sidecar_index_entry_count
        .fetch_add(count, Ordering::Relaxed);
}

pub fn add_sstable_reverse_seek_sidecar_entry_materializes(count: u64) {
    if count == 0 {
        return;
    }
    GLOBAL_METRICS
        .sstable_reverse_seek_sidecar_entry_materialize_count
        .fetch_add(count, Ordering::Relaxed);
}

pub fn add_sstable_reverse_seek_sidecar_offset_probes(count: u64) {
    if count == 0 {
        return;
    }
    GLOBAL_METRICS
        .sstable_reverse_seek_sidecar_offset_probe_count
        .fetch_add(count, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_scan() {
    GLOBAL_METRICS
        .fusion_reverse_scan_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_source_open() {
    GLOBAL_METRICS
        .fusion_reverse_source_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_sstable_frontier_probe() {
    GLOBAL_METRICS
        .fusion_reverse_sstable_frontier_probe_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_sstable_frontier_in_range() {
    GLOBAL_METRICS
        .fusion_reverse_sstable_frontier_in_range_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_sstable_frontier_file() {
    GLOBAL_METRICS
        .fusion_reverse_sstable_frontier_file_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_sstable_frontier_tighten() {
    GLOBAL_METRICS
        .fusion_reverse_sstable_frontier_tighten_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_sstable_frontier_empty_skip() {
    GLOBAL_METRICS
        .fusion_reverse_sstable_frontier_empty_skip_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_sstable_frontier_fail_open() {
    GLOBAL_METRICS
        .fusion_reverse_sstable_frontier_fail_open_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_sstable_pending() {
    GLOBAL_METRICS
        .fusion_reverse_sstable_pending_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_sstable_activation() {
    GLOBAL_METRICS
        .fusion_reverse_sstable_activation_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn add_fusion_reverse_sstable_deferred_unopened(count: u64) {
    if count == 0 {
        return;
    }
    GLOBAL_METRICS
        .fusion_reverse_sstable_deferred_unopened_count
        .fetch_add(count, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_sstable_activation_equal_frontier() {
    GLOBAL_METRICS
        .fusion_reverse_sstable_activation_equal_frontier_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_raw_entry_read() {
    GLOBAL_METRICS
        .fusion_reverse_raw_entry_read_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_visible_candidate() {
    GLOBAL_METRICS
        .fusion_reverse_visible_candidate_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_fusion_reverse_visible_put() {
    GLOBAL_METRICS
        .fusion_reverse_visible_put_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn add_index_key_stream_entry_visits(count: u64) {
    if count == 0 {
        return;
    }
    GLOBAL_METRICS
        .index_key_stream_entry_visit_count
        .fetch_add(count, Ordering::Relaxed);
}

pub fn inc_index_ordered_topk_scan() {
    GLOBAL_METRICS
        .index_ordered_topk_scan_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn add_index_ordered_topk_entry_visits(count: u64) {
    if count == 0 {
        return;
    }
    GLOBAL_METRICS
        .index_ordered_topk_entry_visit_count
        .fetch_add(count, Ordering::Relaxed);
}

pub fn inc_index_ordered_topk_reverse_scan() {
    GLOBAL_METRICS
        .index_ordered_topk_reverse_scan_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_index_ordered_topk_index_only_row() {
    GLOBAL_METRICS
        .index_ordered_topk_index_only_row_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_index_ordered_topk_base_row_fetch() {
    GLOBAL_METRICS
        .index_ordered_topk_base_row_fetch_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn add_index_group_count_summary_entry_visits(count: u64) {
    if count == 0 {
        return;
    }
    GLOBAL_METRICS
        .index_group_count_summary_entry_visit_count
        .fetch_add(count, Ordering::Relaxed);
}

pub fn inc_index_loose_seek() {
    GLOBAL_METRICS
        .index_loose_seek_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_index_loose_value() {
    GLOBAL_METRICS
        .index_loose_value_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_index_loose_run_skip() {
    GLOBAL_METRICS
        .index_loose_run_skip_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_query_sort_fallback() {
    GLOBAL_METRICS
        .query_sort_fallback_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_compaction_run() {
    GLOBAL_METRICS
        .compaction_run_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn add_compaction_input_bytes(bytes: u64) {
    GLOBAL_METRICS
        .compaction_input_bytes
        .fetch_add(bytes, Ordering::Relaxed);
}

pub fn add_compaction_output_bytes(bytes: u64) {
    GLOBAL_METRICS
        .compaction_output_bytes
        .fetch_add(bytes, Ordering::Relaxed);
}

pub fn add_compaction_dropped_versions(count: u64) {
    GLOBAL_METRICS
        .compaction_dropped_version_count
        .fetch_add(count, Ordering::Relaxed);
}

pub fn set_live_sstable_count(count: u64) {
    GLOBAL_METRICS
        .live_sstable_count
        .store(count, Ordering::Relaxed);
}

pub fn record_sstable_manifest_load(elapsed_us: u64, live_files: u64) {
    GLOBAL_METRICS
        .sstable_manifest_load_count
        .fetch_add(1, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_manifest_load_total_us
        .fetch_add(elapsed_us, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_manifest_live_file_count
        .store(live_files, Ordering::Relaxed);
}

pub fn inc_sstable_manifest_load_error() {
    GLOBAL_METRICS
        .sstable_manifest_load_error_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn record_sstable_manifest_legacy_scan(candidate_count: u64) {
    GLOBAL_METRICS
        .sstable_manifest_legacy_scan_count
        .fetch_add(1, Ordering::Relaxed);
    GLOBAL_METRICS
        .sstable_manifest_legacy_scan_candidate_count
        .fetch_add(candidate_count, Ordering::Relaxed);
}

pub fn inc_sstable_manifest_open_error() {
    GLOBAL_METRICS
        .sstable_manifest_open_error_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_row_write() {
    LOCAL_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.row_write_count += 1;
        if m.row_write_count >= FLUSH_THRESHOLD {
            GLOBAL_METRICS
                .row_write_count
                .fetch_add(m.row_write_count, Ordering::Relaxed);
            m.row_write_count = 0;
        }
    })
}

pub fn inc_fts_search() {
    LOCAL_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.fts_search_count += 1;
        if m.fts_search_count >= FLUSH_THRESHOLD {
            GLOBAL_METRICS
                .fts_search_count
                .fetch_add(m.fts_search_count, Ordering::Relaxed);
            m.fts_search_count = 0;
        }
    })
}

pub fn add_fts_hits(n: u64) {
    LOCAL_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.fts_doc_hits += n;
        if m.fts_doc_hits >= FLUSH_THRESHOLD {
            GLOBAL_METRICS
                .fts_doc_hits
                .fetch_add(m.fts_doc_hits, Ordering::Relaxed);
            m.fts_doc_hits = 0;
        }
    })
}

pub fn inc_wal_write() {
    LOCAL_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.wal_write_count += 1;
        if m.wal_write_count >= FLUSH_THRESHOLD {
            GLOBAL_METRICS
                .wal_write_count
                .fetch_add(m.wal_write_count, Ordering::Relaxed);
            m.wal_write_count = 0;
        }
    })
}

pub fn add_wal_bytes(n: u64) {
    LOCAL_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.wal_write_bytes += n;
        if m.wal_write_bytes >= FLUSH_THRESHOLD {
            GLOBAL_METRICS
                .wal_write_bytes
                .fetch_add(m.wal_write_bytes, Ordering::Relaxed);
            m.wal_write_bytes = 0;
        }
    })
}

pub fn record_wal_replay(stats: WalReplayStats) {
    GLOBAL_METRICS
        .wal_replay_count
        .fetch_add(1, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_total_us
        .fetch_add(stats.total_us, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_segment_count
        .fetch_add(stats.segment_count, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_bytes
        .fetch_add(stats.bytes, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_valid_bytes
        .fetch_add(stats.valid_bytes, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_last_segment_id
        .store(stats.last_segment_id, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_last_valid_offset
        .store(stats.last_valid_offset, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_entry_count
        .fetch_add(stats.entry_count, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_put_count
        .fetch_add(stats.put_count, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_delete_count
        .fetch_add(stats.delete_count, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_partial_tail_count
        .fetch_add(stats.partial_tail_count, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_truncate_count
        .fetch_add(stats.truncate_count, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_error_count
        .fetch_add(stats.error_count, Ordering::Relaxed);
}

pub fn inc_wal_replay_error() {
    GLOBAL_METRICS
        .wal_replay_error_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn record_wal_replay_apply(elapsed_us: u64, max_ts: u64) {
    GLOBAL_METRICS
        .wal_replay_apply_count
        .fetch_add(1, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_apply_total_us
        .fetch_add(elapsed_us, Ordering::Relaxed);
    GLOBAL_METRICS
        .wal_replay_max_ts
        .store(max_ts, Ordering::Relaxed);
}

pub fn set_pg_connection_limit(limit: u64) {
    GLOBAL_METRICS
        .pg_connection_limit
        .store(limit, Ordering::Relaxed);
}

pub fn inc_pg_active_connection() {
    GLOBAL_METRICS
        .pg_active_connection_count
        .fetch_add(1, Ordering::Relaxed);
}

pub fn dec_pg_active_connection() {
    GLOBAL_METRICS
        .pg_active_connection_count
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_sub(1))
        })
        .ok();
}

pub fn inc_pg_connection_rejected() {
    GLOBAL_METRICS
        .pg_connection_rejected_count
        .fetch_add(1, Ordering::Relaxed);
}
