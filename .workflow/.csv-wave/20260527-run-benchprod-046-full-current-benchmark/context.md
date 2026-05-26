# BENCHPROD-046 Current Full Benchmark Snapshot

Date: 2026-05-27
Scope: benchmark evidence capture only; dashboard/ui unchanged; database implementation unchanged by this task.

## Objective

Run a current full FusionDB benchmark snapshot on the active working tree and summarize what FusionDB can execute today, what the performance data looks like, what regressed against the prior full snapshot, and how far the project remains from native external benchmark runs.

## Important Working Tree Note

FusionDB was not clean during this run. The active working tree included uncommitted `BENCHPROD-040` changes in:

- `src/execution/query/column_scan.rs`
- `tests/sql_group_aggregate.rs`

The run therefore represents the current local working-tree binary, not a clean committed release. Trend data shows regressions versus `BENCHPROD-043`, so these uncommitted performance changes should not be committed as an optimization without redesign or rollback.

## Environment

- FusionDB HEAD: `60f42464e335f354dcafd9b64a2ec20da55c89a6`
- FusionDB-bench HEAD: `981c51cbc32a60111a7a2ca9a44751c14a5268c4`
- FusionDB binary: `E:/Playground/FusionDB/target/release/fusiondb.exe`
- Benchmark artifacts root: `E:/Playground/FusionDB-bench/runs`
- Threads: `4`

## Commands

```powershell
cd E:\Playground\FusionDB
cargo build --release --bin fusiondb

cd E:\Playground\FusionDB-bench
python fusiondb_matrix.py --scale medium --suite all --load-mode insert --run-name matrix_all_medium_insert_benchprod046_current_20260527 --threads 4 --suite-timeout 3600 --allow-failures
python fusiondb_matrix.py --scale small --suite all --load-mode insert --run-name matrix_all_small_insert_benchprod046_current_20260527 --threads 4 --suite-timeout 1800 --allow-failures
python fusiondb_matrix.py --scale small --suite all --load-mode copy --run-name matrix_all_small_copy_benchprod046_current_20260527 --threads 4 --suite-timeout 1800 --allow-failures
python external_smoke.py --target all --run-name external_smoke_benchprod046_current_20260527
python external_bootstrap.py --target all --search-root E:\Playground --run-name external_bootstrap_benchprod046_current_20260527
python bench_trend.py --baseline runs\matrix_all_medium_insert_full_20260527\matrix_summary.json --current runs\matrix_all_medium_insert_benchprod046_current_20260527\matrix_summary.json --run-name trend_benchprod046_vs_043_medium_20260527
```

## Artifacts

- Medium insert matrix: `E:/Playground/FusionDB-bench/runs/matrix_all_medium_insert_benchprod046_current_20260527/matrix_summary.md`
- Small insert matrix: `E:/Playground/FusionDB-bench/runs/matrix_all_small_insert_benchprod046_current_20260527/matrix_summary.md`
- Small copy matrix: `E:/Playground/FusionDB-bench/runs/matrix_all_small_copy_benchprod046_current_20260527/matrix_summary.md`
- External smoke: `E:/Playground/FusionDB-bench/runs/external_smoke_benchprod046_current_20260527/external_smoke_summary.md`
- External bootstrap: `E:/Playground/FusionDB-bench/runs/external_bootstrap_benchprod046_current_20260527/external_bootstrap_summary.md`
- Trend report: `E:/Playground/FusionDB-bench/runs/trend_benchprod046_vs_043_medium_20260527/bench_trend_summary.md`

## Pass Summary

| Run | Scale | Load mode | Suites | Cases | Errors |
|---|---|---|---:|---:|---:|
| Full local matrix | medium | insert | 9/9 | 39/39 | 0 |
| Full local matrix | small | insert | 9/9 | 39/39 | 0 |
| Full local matrix | small | copy | 9/9 | 39/39 | 0 |
| External readiness | n/a | n/a | 0 native-ready / 7 checked | n/a | n/a |

## Medium Insert Suite Metrics

| Suite | Status | Cases | Case errors | Load ms | Avg ms | Avg P95 ms | Avg P99 ms | Avg ops/sec |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| ycsb | passed | 6 | 0 | 176.108 | 0.674 | 0.734 | 0.750 | 1221.9 |
| tpcc | passed | 5 | 0 | 654.503 | 2.993 | 3.213 | 3.352 | 532.7 |
| tpch | passed | 5 | 0 | 2392.482 | 1.012 | 1.207 | 1.323 | 1148.4 |
| search | passed | 5 | 0 | 307.764 | 4.197 | 4.689 | 5.686 | 629.6 |
| memtier | passed | 4 | 0 | 120.952 | 0.574 | 0.630 | 0.674 | 1263.4 |
| tsbs | passed | 4 | 0 | 1491.879 | 16.619 | 17.526 | 17.756 | 496.6 |
| ldbc | passed | 4 | 0 | 723.608 | 2.528 | 3.332 | 4.222 | 845.5 |
| ann | passed | 3 | 0 | 210.942 | 17.418 | 18.073 | 18.145 | 497.9 |
| chbench | passed | 3 | 0 | 659.083 | 11.477 | 14.876 | 15.977 | 267.9 |

## Slowest Medium Cases

| Rank | Suite | Case | Avg ms | P95 ms | P99 ms | Ops/sec | Rows |
|---:|---|---|---:|---:|---:|---:|---:|
| 1 | tsbs | Fleet rollup by region | 59.960 | 63.311 | 64.196 | 16.7 | 4 |
| 2 | ann | HNSW nearest neighbor | 45.839 | 47.071 | 47.164 | 21.8 | 10 |
| 3 | chbench | Customer order join | 25.543 | 31.585 | 33.579 | 39.1 | 4 |
| 4 | search | Vector nearest neighbor | 13.809 | 14.533 | 16.952 | 72.4 | 10 |
| 5 | chbench | Warehouse revenue rollup | 8.888 | 13.043 | 14.351 | 112.5 | 5 |
| 6 | ldbc | Tag popularity | 7.328 | 10.130 | 11.488 | 136.5 | 7 |
| 7 | ann | Filtered nearest neighbor | 5.643 | 6.309 | 6.349 | 177.2 | 10 |
| 8 | tpcc | Stock level query | 5.281 | 5.654 | 5.754 | 189.4 | 1 |
| 9 | tpcc | New order transaction | 4.825 | 4.980 | 5.200 | 207.3 | 3 |
| 10 | search | LIKE prefix lookup | 2.780 | 3.950 | 4.939 | 359.7 | 20 |

## Throughput Cases

| Suite | Case | Ops/sec | Note |
|---|---|---:|---|
| ycsb | Mixed 80R/20U throughput | 1121.8 | 2000 ops, 4 threads |
| memtier | Mixed GET/SET throughput | 1123.5 | 2000 ops, 4 threads |
| chbench | Hybrid OLTP/OLAP throughput | 652.2 | 2000 ops, 4 threads |

## ANN Correctness Signal

The medium ANN suite ran 5,000 vectors at 128 dimensions.

| Case | Avg ms | P95 ms | Recall@1 | Recall@10 |
|---|---:|---:|---:|---:|
| HNSW nearest neighbor | 45.839 | 47.071 | 1.0 | 1.0 |
| Filtered nearest neighbor | 5.643 | 6.309 | 1.0 | 1.0 |
| Insert vector | 0.772 | 0.838 | n/a | n/a |

## Trend Against BENCHPROD-043

Baseline: `E:/Playground/FusionDB-bench/runs/matrix_all_medium_insert_full_20260527/matrix_summary.json`

Current: `E:/Playground/FusionDB-bench/runs/matrix_all_medium_insert_benchprod046_current_20260527/matrix_summary.json`

| Metric | Result |
|---|---:|
| Suite regressions | 2 |
| Suite improvements | 0 |
| Case regressions | 7 |
| Case improvements | 0 |
| Current median case P95 | 1.122 ms |
| Current max case P95 | 63.311 ms |

Top regressions:

| Suite | Case | Baseline P95 | Current P95 | Delta |
|---|---|---:|---:|---:|
| search | LIKE prefix lookup | 2.573 | 3.950 | 53.52% |
| chbench | Warehouse revenue rollup | 8.532 | 13.043 | 52.87% |
| ldbc | Tag popularity | 6.911 | 10.130 | 46.58% |
| ldbc | One-hop friends | 0.855 | 1.252 | 46.43% |
| tpch | Q1 pricing summary | 0.821 | 1.021 | 24.36% |
| search | FTS MATCH two terms | 2.490 | 3.060 | 22.89% |
| chbench | Customer order join | 25.721 | 31.585 | 22.80% |

TSBS fleet rollup also worsened from prior `54.84 ms` p95 to `63.311 ms` p95 in this run, although the suite-level average did not cross the configured 20% regression threshold.

## Current Capability Snapshot

FusionDB currently executes local benchmark-like workloads for:

- YCSB-style serving: primary-key read/update/insert, secondary equality lookup, short range scan, and mixed 80R/20U throughput.
- TPC-C-style simplified OLTP: NewOrder, Payment, OrderStatus, StockLevel, and Delivery.
- TPC-H-style simplified analytics: Q1/Q3/Q6/Q10/TopK query shapes.
- Search/vector: FTS `MATCH`, `LIKE`, vector nearest neighbor, filtered vector search, `EMBEDDING`, and ANN recall probes.
- memtier-style SQL KV: GET, SET, ADD, and mixed GET/SET throughput.
- TSBS-style time-series: tag/time range, fleet rollup, latest points, and ingest.
- LDBC-style relational graph: one-hop, two-hop, recent posts by friends, and tag popularity.
- CH-benCHmark-style HTAP: mixed OLTP/OLAP throughput, warehouse revenue rollup, and customer-order join.
- File-based benchmark initialization via `COPY` for all local harness suites at small scale.

## External Benchmark Readiness

| Target | Status | Reason |
|---|---|---|
| pgbench | tool_missing | `pgbench` is not on PATH and not found under `E:/Playground` |
| sysbench | tool_missing | `sysbench` is not on PATH and not found under `E:/Playground` |
| memtier | tool_missing | `memtier_benchmark` is not on PATH and not found under `E:/Playground` |
| BenchBase TPC-C | artifact_missing | Java exists, but `BENCHBASE_HOME` or `BENCHBASE_JAR` is not configured and no local candidate was found |
| TSBS | tool_missing | `tsbs_generate_data` is not on PATH and not found under `E:/Playground` |
| LDBC SNB | artifact_missing | Java exists, but `LDBC_SNB_HOME` or `LDBC_DRIVER_HOME` is not configured and no local candidate was found |
| CH-benCHmark | artifact_missing | Java exists, but no CH benchmark artifact env is configured and no local candidate was found |

## Production Distance

This is a healthy local benchmark lab snapshot, not official production benchmark readiness.

- Local harness breadth is good: all 9 suites and 39 cases pass at medium insert, small insert, and small copy.
- External benchmark readiness remains blocked at the tool/artifact/adapter layer: 0/7 native external targets are runnable in this environment.
- Current active working tree has performance regressions compared with `BENCHPROD-043`; do not treat the uncommitted `BENCHPROD-040` code as a successful optimization.
- Main performance hotspots are TSBS grouped rollup, ANN nearest neighbor, CH-benCHmark customer-order join, search vector/LIKE paths, and LDBC tag aggregation.

## Next TASK Signals

- `BENCHPROD-040`: Redesign TSBS fleet rollup optimization; prefer streaming/visitor table scan or aggregate-specific scan over per-row decode HashMap caching.
- `BENCHPROD-047`: Cleanly rollback or isolate unproven BENCHPROD-040 changes, then rerun medium trend to restore a clean baseline.
- `BENCHPROD-020`: Install/configure pgbench under `E:/Playground/tools/postgresql-client` and capture first native pgbench gap report.
- `BENCHPROD-021`: Configure BenchBase TPC-C artifact under `E:/Playground/benchbase` and run Java/JDBC smoke.
- `BENCHPROD-028`: Add cost/cardinality-based join ordering for LDBC/CH/TPC-H scale-up.

## Post-Run Hygiene

- `cargo build --release --bin fusiondb`: passed.
- No `fusiondb` server remained listening on `127.0.0.1:8091` or `127.0.0.1:8092` after the matrix runs.
