# BENCHPROD Production Benchmark Gap Analysis

Date: 2026-05-26
Scope: FusionDB database core and `E:/Playground/FusionDB-bench`; dashboard/ui excluded.

## Summary

FusionDB is currently a prototype-to-benchmark-lab system, not a production database and not an official benchmark target yet.

Current benchmark lab coverage:
- Local like-workloads exist for ycsb, tpcc, tpch, search, memtier, tsbs, ldbc, ann, and chbench.
- Official/native-ready suites: 0.
- Primary blocker class: external protocol and dialect compatibility, especially PgWire extended query metadata, COPY FROM STDIN, JDBC/pgbench behavior, production scalar types, and optimizer statistics.

## Evidence

- `E:/Playground/FusionDB-bench/fusiondb_bench.py` tracks 8 production benchmark targets and reports `native_official_ready = 0`.
- `src/server/pg_server.rs` still emits all result fields as `Type::TEXT` and returns empty `ParameterDescription` / `RowDescription` for extended Describe.
- `src/server/pg_server.rs` extended-query `do_query` is still not implemented; execute path is custom and incomplete.
- `src/execution/copy.rs` supports file-based COPY, but explicitly rejects `COPY FROM STDIN`.
- `src/common/value.rs` has Null/Boolean/Integer/Float/String/Blob/Vector/Array/Object only; DATE/TIMESTAMP/DECIMAL/INTERVAL are not first-class values.
- `ROADMAP.md` still lists correlated subqueries, cost-based optimizer, and VACUUM as open.
- `src/storage/fusion.rs` has MVCC snapshot isolation, WAL, LSM/SSTable and OCC write conflict detection, but needs production soak around compaction, vacuum, crash recovery and long-running mixed workloads.

## Distance by Target

| Target | Current position | Native blocker | Distance |
|---|---|---|---|
| pgbench | local coverage only via ycsb/tpcc-like | COPY FROM STDIN, extended prepared protocol, VACUUM, metadata/type OIDs | Far |
| TPC-C / BenchBase | tpcc-like local suite | JDBC/PgWire metadata, official schema/types, transaction mix, FK details | Far |
| sysbench | no native suite | PostgreSQL/MySQL driver behavior, AUTO_INCREMENT/SERIAL, dialect compatibility | Far |
| memtier | SQL KV-like suite | Redis/Memcached protocol and pipelining, backpressure metrics | Very far unless kept as SQL-like only |
| TSBS | tsbs-like local suite | official TSBS adapter, TIMESTAMP semantics, ordered composite index/range optimization | Medium-far |
| LDBC SNB | ldbc-like local suite | official generator/driver adapter, deeper path queries, join order optimizer | Far |
| ANN benchmarks | latency-only ann-like suite | recall@k ground truth, dataset adapter, build metrics, HNSW knobs | Medium |
| CH-benCHmark | chbench-like local suite | official schema/query set, HTAP snapshot stability, cost optimizer, long-run compaction | Far |

## Production Readiness Estimate

- Benchmark lab readiness: about 50-60/100.
- Official benchmark native compatibility: about 10-20/100.
- Production database readiness: about 20-30/100.

This estimate is not based on one missing feature. It is because official benchmark tools require client protocol fidelity, SQL dialect compatibility, data-loading behavior, transaction semantics, optimizer stability, and reproducible long-run metrics at the same time.

## Locked Decisions

1. Keep official benchmark integration outside the main repo in `E:/Playground/FusionDB-bench`.
2. Continue optimizing database core only; do not touch dashboard/ui for BENCHPROD work.
3. Treat local like-workloads as engineering smoke/perf signals, not published benchmark claims.
4. Make PgWire/PostgreSQL compatibility the first native benchmark path, because pgbench, BenchBase, TSBS, and sysbench PostgreSQL mode can all leverage it.
5. Do not call ANN performance production-grade until recall@k against exact ground truth is reported.

## Recommended Phase Order

1. Compatibility gate: PgWire extended metadata, COPY FROM STDIN, production scalar types, external adapter runner.
2. Native smoke gate: pgbench initialize/run, BenchBase TPC-C schema/load, sysbench PostgreSQL read-only smoke.
3. Optimizer gate: statistics-driven index choice, join cardinality estimates, join reordering, range/composite index costing.
4. Storage soak gate: direct bulk load, compaction/VACUUM, crash-recovery runs, multi-hour mixed workload.
5. Benchmark credibility gate: environment capture, repeatability, warmup/report intervals, normalized output, correctness checks.

## Iteration Log

### BENCHPROD-006 completed

Implemented PgWire extended-query metadata parity baseline:
- Prepared statement `ParameterDescription` now reports parameter OIDs instead of an empty list.
- Statement/portal `Describe` now returns `RowDescription` for simple single-table SELECT projections.
- Extended query result rows are encoded with PostgreSQL binary layout for integer, float, boolean, bytea and text-compatible values.
- PgWire tests now fail hard on extended-query errors and assert parameterized query execution plus prepared metadata.

Verification:
- `cargo fmt --check`
- `cargo check --lib`
- `cargo test --test pg_integration`
- `cargo test --test sql_dml`
- `cargo test --test sql_ddl`
- `cargo test --test sql_index_cache`

Remaining PgWire gaps:
- Parameter type inference is still heuristic for general SQL.
- Complex JOIN/aggregate Describe metadata still falls back to text or runtime inference.
- COPY FROM STDIN is still open as `BENCHPROD-019`.
