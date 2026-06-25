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
    pub row_write_count: AtomicU64,
    pub fts_search_count: AtomicU64,
    pub fts_doc_hits: AtomicU64,
    pub wal_write_count: AtomicU64,
    pub wal_write_bytes: AtomicU64,
    pub query_count: AtomicU64,
    pub slow_query_count: AtomicU64,
    pub query_total_us: AtomicU64,
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
        self.row_write_count.store(0, Ordering::Relaxed);
        self.fts_search_count.store(0, Ordering::Relaxed);
        self.fts_doc_hits.store(0, Ordering::Relaxed);
        self.wal_write_count.store(0, Ordering::Relaxed);
        self.wal_write_bytes.store(0, Ordering::Relaxed);
        self.query_count.store(0, Ordering::Relaxed);
        self.slow_query_count.store(0, Ordering::Relaxed);
        self.query_total_us.store(0, Ordering::Relaxed);
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
