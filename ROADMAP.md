# FusionDB Roadmap

> Last updated: 2026-06-26

## Phase 1: Data Integrity & Reliability ✅

- [x] **P1-1: Graceful shutdown** — Ctrl+C → flush MemTable → save indexes → truncate WAL
- [x] **P1-2: TOML config file** — `fusiondb.toml` with server/storage/auth sections, `--init` flag
- [x] **P1-3: Segmented WAL** — 64MB segment rotation, multi-segment replay/truncate, backward compat
- [x] **P1-4: SSTable checksums** — CRC32 per data block, verification on read, legacy block compat
- [x] **P1-5: Compaction dedup** — Skip stale MVCC key versions during 4-way merge

## Phase 2: SQL Completeness ✅✅

- [x] **P2-1: ALTER TABLE** — ADD COLUMN, DROP COLUMN, RENAME COLUMN (with row rewrite on DROP)
- [x] **P2-2: OFFSET** — `SELECT ... LIMIT N OFFSET M` (already existed)
- [x] **P2-3: Subqueries** — `WHERE id IN (SELECT ...)`, `NOT IN`, scalar subqueries
- [x] **P2-4: UNION / INTERSECT / EXCEPT** — UNION ALL, UNION (dedup), ORDER BY / LIMIT
- [x] **P2-5: CASE WHEN** — Both simple (`CASE x WHEN`) and searched (`CASE WHEN cond`) forms
- [x] **P2-6: Built-in functions** — UPPER, LOWER, LENGTH, CONCAT, SUBSTRING, REPLACE, TRIM, ABS, ROUND, COALESCE, NULLIF
- [x] **P2-7: SELECT without FROM** — `SELECT 1+1`, `SELECT UPPER('hello')`
- [x] **P2-8: SHOW CREATE TABLE** — Reconstruct DDL from stored schema
- [x] **P2-9: TRUNCATE TABLE** — Delete all rows, preserve schema
- [x] **P2-10: CTEs** — `WITH ... AS` common table expressions (materialized as temp tables)
- [x] **P2-11: CREATE TABLE IF NOT EXISTS** — Skip if table already exists
- [x] **P2-12: COUNT(DISTINCT col)** — Distinct counting via dedicated accumulator
- [x] **P2-13: Bare aggregates** — `SELECT SUM(x), AVG(x) FROM t` without GROUP BY
- [x] **P2-14: INSERT ... SELECT** — Insert from query results
- [x] **P2-15: CAST** — `CAST(expr AS type)` for INTEGER, FLOAT, TEXT, BOOLEAN conversions
- [x] **P2-16: EXISTS / NOT EXISTS** — Subquery existence checks (non-correlated)
- [x] **P2-17: String concat operator** — `||` operator for string concatenation
- [x] **P2-18: Window functions** — ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD with PARTITION BY + ORDER BY
- [x] **P2-19: CREATE VIEW / DROP VIEW** — Views stored as SQL, expanded at query time, OR REPLACE, IF EXISTS
- [x] **P2-20: ILIKE** — Case-insensitive LIKE pattern matching
- [x] **P2-21: INSERT with column list** — `INSERT INTO t(a,b) VALUES(...)`, missing columns get NULL
- [x] **P2-22: DEFAULT column values** — `CREATE TABLE t(x INT DEFAULT 0)`, applied during INSERT with column list
- [x] **P2-23: NOT NULL constraints** — Enforced on INSERT and UPDATE, extracted from column definitions
- [x] **P2-24: UNIQUE constraints** — Enforced on INSERT, scans for duplicate values
- [x] **P2-25: SHOW VIEWS** — Custom statement listing all views with definitions
- [x] **P2-26: DROP INDEX (IF EXISTS)** — Removes index entries, metadata, and updates schema
- [x] **P2-27: execute_sql() unified API** — Single entry point for custom + standard SQL
- [x] **P2-28: CHECK constraints** — Extracted from DDL, enforced on INSERT and UPDATE
- [x] **P2-29: STRING_AGG / GROUP_CONCAT** — Aggregate string concatenation with separator
- [x] **P2-30: Math functions** — CEIL, FLOOR, MOD, POWER, SQRT (Expr::Ceil/Floor as AST variants)
- [x] **P2-31: NOW / CURRENT_TIMESTAMP** — Unix epoch seconds, CURRENT_DATE as epoch days
- [x] **P2-32: RETURNING clause** — INSERT/UPDATE/DELETE ... RETURNING * or specific columns
- [x] **P2-33: UPSERT** — INSERT ... ON CONFLICT DO UPDATE SET / DO NOTHING with EXCLUDED references
- [x] **P2-34: CROSS JOIN** — Cartesian product join support
- [x] **P2-35: Multi-column ORDER BY** — Mixed ASC/DESC verified with tests
- [x] **P2-36: Correlated subqueries** — EXISTS/NOT EXISTS outer row references, aggregate HAVING binding, and scalar projection materialization

## Phase 3: Security

- [x] **P3-1: TLS infrastructure** — rustls + tokio-rustls deps, TlsConfig in TOML, tls.rs acceptor builder, server wiring (pgwire TLS requires proxy in v0.37)
- [x] **P3-2: Configurable auth** — Password from fusiondb.toml config, passed through server startup chain
- [ ] **P3-3: SCRAM-SHA-256** — Replace cleartext auth (blocked by pgwire 0.37 limitations)
- [x] **P3-4: RBAC** — CREATE/DROP USER (SHA-256 hashed passwords), GRANT/REVOKE per-table permissions, SHOW USERS, SUPERUSER flag, check_permission enforcement API

## Phase 4: Performance & Scale ✅

- [x] **P4-1: Connection pooling** — Configurable pgwire connection slots with max-connection backpressure and metrics
- [x] **P4-2: Parallel scan** — rayon-based multi-thread table scan for aggregations (>1000 rows)
- [x] **P4-3: Cost-based optimizer** — ANALYZE/EXPLAIN cardinality estimates plus stats-guided comma and INNER JOIN chain reordering
- [x] **P4-4: Page compression** — LZ4-compressed SSTable data blocks with CRC and legacy block compatibility

## Phase 5: Distributed

- [x] **P5-1: Wire OpenRaft into main loop** — Configurable Raft startup, `/raft/*` routes, leader-forwarded writes, local follower reads
- [x] **P5-2: Snapshot transfer** — Serialized visible key-value state for new node bootstrap
- [ ] **P5-3: Automatic sharding** — Hash/range control plane, local row/index KV shard layouts, and HTTP/pgwire INSERT/UPDATE/DELETE/COPY point-write owner guard with pgwire transaction-visible schema routing started; automatic cross-node SQL forwarding and distributed index ownership remain

## Phase 6: Operations & Ecosystem ✅

- [x] **P6-1: Slow query log** — Configurable threshold (100ms default), ring buffer, stderr output
- [x] **P6-2: Prometheus metrics** — `/metrics/prometheus` endpoint (OpenMetrics format)
- [x] **P6-3: Slow queries API** — `/slow_queries` JSON endpoint
- [x] **P6-4: Query timing** — Every query timed, total µs tracked in metrics
- [x] **P6-5: VACUUM** — SQL manual compaction trigger backed by FusionStorage compaction
- [x] **P6-6: Admin CLI** — `fusiondb-cli` health/query/tables/metrics/checkpoint/compact operations
- [x] **P6-7: CDC** — Durable committed change feed exposed via `/cdc/events` and `fusiondb-cli cdc`

## Phase 7: Dashboard UI (FusionDB Studio) ✅

- [x] **P7-1: Project scaffold** — React + Vite + TypeScript + TailwindCSS v4
- [x] **P7-2: Dark theme** — Supabase-style dark UI with green accent colors
- [x] **P7-3: Sidebar layout** — Navigation: Dashboard, Table Editor, SQL Editor, Settings
- [x] **P7-4: Dashboard page** — Real-time metrics cards, table list, slow query log, checkpoint trigger
- [x] **P7-5: SQL Editor** — CodeMirror with SQL syntax, Ctrl+Enter execute, result tabs, CSV export
- [x] **P7-6: Table Editor** — Browse tables, schema info, WHERE filter, inline insert/delete rows
- [x] **P7-7: Settings page** — Connection info, database capabilities overview
- [x] **P7-8: API client** — Typed fetch client for /query, /tables, /metrics, /slow_queries, /checkpoint
- [x] **P7-9: Enhanced /tables endpoint** — Added is_nullable, default_value, index_type to column info

## Phase 8: CI/CD & DevOps

- [x] **P8-1: GitHub Actions CI** — Parallel Rust (build+test+clippy+fmt) and Dashboard (npm build) jobs with caching
- [x] **P8-2: Multi-stage Dockerfile** — Rust builder + Node.js dashboard builder + slim runtime image, correct port exposure (8091/8092)
- [x] **P8-3: Unified Benchmark Suite** — 6-part benchmark (Base, E-commerce, Financial, Analytics, Concurrent, Stress), 4 scales (small/medium/large/xlarge), JSON report export, ~60 query benchmarks per run

## Phase 9: Performance Optimization

- [x] **P9-1: scan_range cleanup** — Removed dead code (redundant decode_key), store last_user_key directly instead of last_internal_key, pre-allocate result vector
- [x] **P9-2: count_prefix rewrite** — Replaced HashMap-based counting with streaming merge via scan_range (3-10x faster for COUNT(*))
- [x] **P9-3: Pre-allocate vectors** — scan_table_base, nested loop join, CROSS JOIN, hash join HashMap all pre-allocated with capacity
- [x] **P9-4: Benchmark results (LARGE, 228K rows)** — Load 1.8x faster (24K rows/s), index speedup fixed (3.2x), event queries 4-5x faster, subquery 6.3x faster, concurrent throughput 1.3x (1,261 ops/s)

## Test Coverage

| Suite | Count | Description |
|---|---|---|
| Unit tests | 74 | Storage, WAL, SSTable, config, encoding |
| SQL integration | 98 | DDL, DML, queries, joins, aggregation, subqueries, UNION, CASE, CTE, functions, window fns, views, constraints, UPSERT, RBAC |
| pgwire integration | 4 | PostgreSQL wire protocol end-to-end |
| **Total** | **176** | All passing |
