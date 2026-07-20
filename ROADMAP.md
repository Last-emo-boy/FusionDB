# FusionDB Roadmap

> Last updated: 2026-07-11. This file tracks implemented code and verified gaps;
> `TODO.md` is retained only as a historical planning document.

## Phase 1: Data Integrity & Reliability ✅

- [x] **P1-1: Graceful shutdown** — Ctrl+C and `SIGTERM` → block commits → flush all MemTables → persist manifest/indexes → truncate WAL
- [x] **P1-2: TOML config file** — `fusiondb.toml` with server/storage/auth sections, `--init` flag
- [x] **P1-3: Segmented WAL** — 64MB segment rotation, CRC32-protected atomic transaction frames, synchronous file/directory persistence, confirmed rollback to the last durable offset after write/flush/sync failures, and permanent writer poisoning when rollback durability is ambiguous
- [x] **P1-4: SSTable checksums and structure** — CRC32 per data block, strict declared-entry/trailing-byte validation, and legacy block compatibility
- [x] **P1-5: Reader-aware compaction** — Register transaction snapshots atomically with the active-reader watermark and retain every MVCC version still needed by a live reader
- [x] **P1-6: Atomic commit/checkpoint boundary** — Serialize OCC validation and publication, publish commit timestamps last, flush oldest-first through staged SSTables, and truncate WAL only after a complete checkpoint
- [x] **P1-7: Derived-index visibility** — Stream visible durable rows into detached HNSW/trigram state before serving, publish it atomically, and force old snapshots to scan instead of reading latest-only side indexes

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
- [x] **P2-24: UNIQUE constraints** — Column/table UNIQUE and composite unique indexes use transactionally maintained, index-namespaced sentinels with SQL `NULLS DISTINCT` semantics; composite PRIMARY metadata carries an explicit identity bit; CREATE/DROP backfills and all row writers share OCC barriers
- [x] **P2-25: SHOW VIEWS** — Custom statement listing all views with definitions
- [x] **P2-26: DROP INDEX (IF EXISTS)** — Removes index entries, metadata, and updates schema
- [x] **P2-27: execute_sql() unified API** — Single entry point for custom + standard SQL
- [x] **P2-28: CHECK constraints** — Extracted from DDL, enforced on INSERT and UPDATE
- [x] **P2-29: STRING_AGG / GROUP_CONCAT** — Aggregate string concatenation with separator
- [x] **P2-30: Math functions** — CEIL, FLOOR, MOD, POWER, SQRT (Expr::Ceil/Floor as AST variants)
- [x] **P2-31: NOW / CURRENT_TIMESTAMP** — Unix epoch seconds, CURRENT_DATE as epoch days
- [x] **P2-32: RETURNING clause** — INSERT/UPDATE/DELETE ... RETURNING * or specific columns
- [x] **P2-33: UPSERT** — INSERT ... ON CONFLICT DO UPDATE SET / DO NOTHING with EXCLUDED references; conflict targets resolve to the primary key or a single-column UNIQUE constraint (owner-row update), unsupported targets and DO UPDATE ... WHERE are rejected loudly
- [x] **P2-34: CROSS JOIN** — Cartesian product join support
- [x] **P2-35: Multi-column ORDER BY** — Mixed ASC/DESC verified with tests
- [x] **P2-36: Correlated subqueries** — EXISTS/NOT EXISTS outer row references, aggregate HAVING binding, and scalar projection materialization

## Phase 3: Security

- [x] **P3-1: Built-in TLS listeners** — HTTP and pgwire use the configured rustls certificate directly; missing or invalid TLS material fails startup
- [x] **P3-2: Fail-closed authentication** — HTTP Basic and pgwire password auth default to configured credentials/RBAC users; anonymous and trusted-header identity require an explicit unsafe compatibility flag
- [ ] **P3-3: SCRAM-SHA-256** — Replace cleartext password exchange
- [x] **P3-4: RBAC** — CREATE/DROP USER (salted PBKDF2 hashes with legacy SHA-256 verification), GRANT/REVOKE per-table permissions, SHOW USERS, SUPERUSER flag, check_permission enforcement API
- [x] **P3-5: Internal request authentication** — Distributed mode requires TLS and signs method, path/query, delegated user, timestamp, and body digest with HMAC; management routes require superuser or a valid internal signature
- [x] **P3-6: Protocol boundary hardening** — Redis is loopback-only, authenticated, connection-limited, frame/header-bounded, and rejects unsupported TTL semantics; distributed mode forbids legacy unsafe HTTP identity; global vector/hybrid APIs require superuser; Dashboard SQL literals and identifiers are encoded centrally

## Phase 4: Performance & Scale ✅

- [x] **P4-1: Connection pooling** — Configurable pgwire connection slots with max-connection backpressure and metrics
- [x] **P4-2: Parallel scan** — rayon-based multi-thread table scan for aggregations (>1000 rows)
- [x] **P4-3: Cost-based optimizer** — ANALYZE/EXPLAIN cardinality estimates plus stats-guided comma and INNER JOIN chain reordering
- [x] **P4-4: Page compression** — LZ4-compressed SSTable data blocks with CRC and legacy block compatibility

## Phase 5: Distributed

- [x] **P5-1: Durable deterministic Raft writes** — Persist vote/log/state-machine/snapshot files with version, length, CRC, atomic rename, and directory sync; the leader evaluates SQL once and replicates a bounded, bincode-safe mutation batch plus preconditions and exact results
- [x] **P5-2: Boundary-consistent snapshots** — Serialize build/install with state-machine apply; publish data, applied watermark, and hash/meta marker atomically behind a CRC-framed durable install intent. Data plus marker is the commit point: later metadata/cleanup failures retain the intent and return success so OpenRaft is never told a durably visible install failed; startup reconciles every crash boundary. Resumable chunked transfer remains
- [ ] **P5-3: Automatic sharding** — Hash/range routing and conservative read fan-out exist. Mixed/multi-owner atomic writes, distributed index ownership, automatic split/rebalance, and a general distributed planner remain
- [ ] **P5-4: Complete consensus protocol coverage** — Raft-mode writes currently enter only through authenticated HTTP `/query`; pgwire writes, HTTP prepared writes, and COPY fail closed until they share the same deterministic proposal path
- [ ] **P5-5: Production log/snapshot transport** — Replace full-file Raft log rewrites with segmented append-only storage and replace monolithic snapshots with checksummed resumable chunks
- [ ] **P5-6: Rolling protocol upgrades** — Add cluster feature negotiation and joint-version mutation encoding; the current pre-mutation-batch transition requires a stop-the-world upgrade and rejects legacy raw SQL

## Phase 6: Operations & Ecosystem ✅

- [x] **P6-1: Slow query log** — Configurable threshold (100ms default), ring buffer, stderr output
- [x] **P6-2: Prometheus metrics** — `/metrics/prometheus` endpoint (OpenMetrics format)
- [x] **P6-3: Slow queries API** — `/slow_queries` JSON endpoint
- [x] **P6-4: Query timing** — Every query timed, total µs tracked in metrics
- [x] **P6-5: VACUUM** — SQL manual compaction trigger backed by FusionStorage compaction
- [x] **P6-6: Admin CLI** — `fusiondb-cli` health/query/tables/metrics/checkpoint/compact operations
- [x] **P6-7: Local CDC** — Durable local committed change feed exposed via `/cdc/events` and `fusiondb-cli cdc`; replicated ordering, sink checkpoints, and failover semantics remain

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

- [x] **P8-1: GitHub Actions CI** — Parallel Rust (all-target tests, correctness clippy, fmt) and Dashboard (dependency audit, typecheck, build, lint) jobs with caching and no ignored failures
- [x] **P8-2: Multi-stage Dockerfile** — Rust builder plus slim server runtime, runtime config outside the `/data` volume, and correct port exposure (8091/8092)
- [x] **P8-3: Unified Benchmark Suite** — 31 registered parts, 10 default parts, focused matrix selection, 4 scales, HTTP/pgwire protocols, metrics deltas, checksums, and JSON claim reports

## Phase 9: Performance Optimization

- [x] **P9-1: scan_range cleanup** — Removed dead code (redundant decode_key), store last_user_key directly instead of last_internal_key, pre-allocate result vector
- [x] **P9-2: count_prefix rewrite** — Replaced HashMap-based counting with streaming merge via scan_range (3-10x faster for COUNT(*))
- [x] **P9-3: Pre-allocate vectors** — scan_table_base, nested loop join, CROSS JOIN, hash join HashMap all pre-allocated with capacity
- [x] **P9-4: Benchmark results (LARGE, 228K rows)** — Load 1.8x faster (24K rows/s), index speedup fixed (3.2x), event queries 4-5x faster, subquery 6.3x faster, concurrent throughput 1.3x (1,261 ops/s)
- [x] **P9-5: Filtered-scan LIMIT pushdown + aggregate LIMIT fix** — Push `LIMIT` into filtered, unordered, non-aggregate single-table scans so the full-table scan early-breaks after `offset+limit` matches instead of decoding/evaluating every row (guarded to exclude aggregates / window functions / DISTINCT). Also fixes a correctness bug where the outer `LIMIT`/`OFFSET` truncated the scanned rows before `COUNT(*)`/bare-aggregate computation, so `SELECT COUNT(*)/SUM(...) ... WHERE ... LIMIT n` returned an aggregate over only `n` rows
- [x] **P9-6: Parallel range-merge for full scans** — The serial phase-1 materialize (N-way MVCC heap merge + per-row key/value clones) was the unindexed full-scan floor (decode was already parallel, so it wasn't the bottleneck). Split the integer-PK key space (`prefix ++ 16-hex`) into K disjoint sub-ranges (K=min(cores, 8)) over one shared snapshot (single `read_ts`), merge them on `tokio` tasks, and concatenate in key order — identical results to the serial scan (disjoint ranges → no cross-boundary dedup). Falls back to serial for non-integer-PK / small (<8192-row) / `LIMIT`ed scans. **Large-scale benchmark: Full scan 230.9→148.6 ms (1.55×), BETWEEN 231→149 (1.55×), LIKE 202.6→147 (1.38×), GROUP BY+HAVING 144→95 (1.51×), ORDER BY val DESC 394→292 (1.35×); Base-category avg 79.4→56.6 ms.** (glibc allocator caps the speedup below linear; a faster allocator would scale further.)
- [x] **P9-8: pgwire query-result-cache parity** — The grouped-aggregate result cache (`execute_sql`) only sped up the HTTP `/query` path; pgwire autocommit queries recomputed every time (a `BENCH_PROTO=pg` A/B showed GROUP BY 8.7 ms over pgwire vs 0.4 ms cached over HTTP). Routed pgwire autocommit, param-free, cacheable grouped aggregates through the same cache (guarded to no active transaction + no bind params; writes still bump the cache epoch so reads never go stale). Also hardened the shared cacheable-join predicate to reject a volatile function in the JOIN `ON` (a pre-existing staleness hole that affected the HTTP path too). **pgwire large benchmark: GROUP BY category 8.7→0.1 ms (17.5k ops/s); whole Analytics family now ~0.1 ms, matching HTTP.**
- [x] **P9-7: jemalloc global allocator** — The per-row key/value clones on the (now-parallel) scan path made glibc malloc arena locking the measured ceiling (P9-6 stayed below linear). Switched the server binary's `#[global_allocator]` to jemalloc (`tikv-jemallocator`). **Large-scale benchmark (on top of P9-6): Full scan 148.6→84.4 ms, BETWEEN 148.9→112.1, LIKE 147→119.1, GROUP BY+HAVING 95.3→42.4, ORDER BY val DESC 291.7→106.4, COUNT(*) 21.4→6.5, SUM 21.8→8.4; Base-category avg 56.6→32.8 ms. Cumulative vs the pre-P9-6 serial+glibc baseline: Full scan 2.7×, GROUP BY+HAVING 3.4×, ORDER BY val DESC 3.7×, Base avg 2.4×.** Allocation is pervasive, so every query improved.
- [x] **P9-9: Repeated uncorrelated subquery reuse** — Cache `IN (SELECT ...)` membership by AST occurrence, preserving type information while avoiding repeated formatting, schema analysis, and subquery execution (BENCHPROD-473)

## Phase 10: Production Hardening

- [x] **P10-1: Failure atomicity pass** — Close WAL rollback/OOM, startup recovery, MVCC watermark, streaming derived-index publication, online-index DDL races, composite UNIQUE/PRIMARY identity, deterministic Raft apply/snapshot intent, and protocol-bypass risks with targeted regressions
- [ ] **P10-2: Versioned structured keyspace** — The typed Data V2 codec, exact prefix isolation, legacy-authoritative shadow writes/deletes, table cleanup, and CDC mirror suppression are landed. **P10-2.1 landed**: a durable, monotonic migration phase record (18-byte Catalog KV) now overrides the `structured_data_shadow_v2` flag and fences every data-family writer — the pin is revalidated inside the commit critical section where advances publish, replicated as a Raft precondition, guarded on apply by a monotonic step check that rejects gracefully, and refused at open/install above the phase this binary implements; `CALL fusiondb_data_migration_init/advance` and `SHOW DATA MIGRATION PHASE` are the operator surface. **P10-2.2 landed**: DROP/TRUNCATE shadow cleanup is now bounded by a route skip-scan (one probe seek per route present plus the target table's own range) instead of materializing the whole Data V2 namespace. A commit-fenced resumable backfill, exact verifier, v2 read publication, remaining key families, and legacy GC remain (9-ticket ladder in `.workflow/.csv-wave/20260720-design-p10-2-data-v2-migration-ladder/`)
- [ ] **P10-3: Deterministic simulation** — Run storage, Raft, crash, clock, and network fault schedules under seeded virtual time with byte-identical replay
- [ ] **P10-4: Serializable isolation** — Add SSI-style predicate/range conflict tracking, bounded memory, safe read-only snapshots, and serialization-failure retries
- [ ] **P10-5: Backup and PITR** — Build checksummed full/incremental backups, revision history, restore validation, retention, and scheduled RPO/RTO drills
- [ ] **P10-6: Online schema jobs** — Introduce descriptor epochs, write-before-read index states, resumable backfill checkpoints, validation, and rollback-safe publication
- [ ] **P10-7: Distributed CDC** — Replicate logical sequence positions and durable sink checkpoints with explicit at-least-once/exactly-once contracts

See [`docs/PRODUCTION_HARDENING.md`](docs/PRODUCTION_HARDENING.md) for sequencing, acceptance gates, and primary references.

## Test Coverage

| Suite | Count | Description |
|---|---|---|
| Unit/binary tests | 658 | Storage, WAL, SSTable, execution, config, encoding, protocol, distributed state, and binary helpers |
| Integration/all other tests | 542 | SQL, pgwire, indexes, scans, joins, aggregates, DDL/DML, transactions, and concurrency regressions |
| **Total tests** | **1,200** | All passed with `cargo test --locked --all-targets --quiet` on 2026-07-20 |
