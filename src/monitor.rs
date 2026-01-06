use lazy_static::lazy_static;
use serde::Serialize;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};

lazy_static! {
    pub static ref GLOBAL_METRICS: Metrics = Metrics::default();
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
