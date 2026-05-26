# BENCHPROD-043 Current Full Benchmark Snapshot

Date: 2026-05-27
Scope: benchmark evidence capture only; dashboard/ui unchanged.

## Objective

Run the full current FusionDB benchmark surface and summarize what FusionDB can execute today, what the performance data looks like, and how far the project still is from real external benchmark compatibility.

This run uses the current working-tree release binary. The working tree includes the completed but not-yet-committed `BENCHPROD-007` production scalar type changes, so this snapshot represents the current local database body rather than the last committed `main` only.

## Environment

- FusionDB HEAD: `f64838e8dfb549eb099cbcefc187ae94855b1fef`
- FusionDB-bench HEAD: `8ece94fe3cd8f4c360ca8cbbab5019b649d13433`
- FusionDB binary: `E:/Playground/FusionDB/target/release/fusiondb.exe`
- Host CPU: `AMD EPYC 9654 96-Core Processor`, 16 logical processors visible
- Memory: `68718481408` bytes
- Rust: `rustc 1.94.1`
- Cargo: `cargo 1.94.1`
- Python: `3.14.2`

## Commands

```powershell
cd E:\Playground\FusionDB
cargo build --release --bin fusiondb

cd E:\Playground\FusionDB-bench
python fusiondb_matrix.py --scale small --suite all --load-mode insert --run-name matrix_all_small_insert_full_20260527 --threads 4 --suite-timeout 1800 --allow-failures
python fusiondb_matrix.py --scale medium --suite all --load-mode insert --run-name matrix_all_medium_insert_full_20260527 --threads 4 --suite-timeout 3600 --allow-failures
python fusiondb_matrix.py --scale small --suite all --load-mode copy --run-name matrix_all_small_copy_full_20260527 --threads 4 --suite-timeout 1800 --allow-failures
python external_smoke.py --target all --run-name external_smoke_full_20260527
```

## Artifacts

- Medium insert matrix JSON: `E:/Playground/FusionDB-bench/runs/matrix_all_medium_insert_full_20260527/matrix_summary.json`
- Medium insert matrix Markdown: `E:/Playground/FusionDB-bench/runs/matrix_all_medium_insert_full_20260527/matrix_summary.md`
- Small insert matrix JSON: `E:/Playground/FusionDB-bench/runs/matrix_all_small_insert_full_20260527/matrix_summary.json`
- Small copy matrix JSON: `E:/Playground/FusionDB-bench/runs/matrix_all_small_copy_full_20260527/matrix_summary.json`
- External readiness JSON: `E:/Playground/FusionDB-bench/runs/external_smoke_full_20260527/external_smoke_summary.json`
- External readiness Markdown: `E:/Playground/FusionDB-bench/runs/external_smoke_full_20260527/external_smoke_summary.md`

## Pass Summary

| Run | Scale | Load mode | Suites | Cases | Errors |
|---|---|---|---:|---:|---:|
| Full local matrix | small | insert | 9/9 | 39/39 | 0 |
| Full local matrix | medium | insert | 9/9 | 39/39 | 0 |
| Full local matrix | small | copy | 9/9 | 39/39 | 0 |
| External readiness | n/a | n/a | 0 native-ready / 7 checked | n/a | n/a |

## Medium Insert Suite Metrics

| Suite | Status | Cases | Case errors | Load ms | Avg ms | Avg P95 ms | Avg P99 ms | Avg ops/sec |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| ycsb | passed | 6 | 0 | 164.34 | 0.64 | 0.69 | 0.73 | 1273.9 |
| tpcc | passed | 5 | 0 | 638.26 | 2.89 | 3.09 | 3.13 | 565.2 |
| tpch | passed | 5 | 0 | 2408.33 | 1.04 | 1.18 | 1.22 | 1132.0 |
| search | passed | 5 | 0 | 298.45 | 4.01 | 4.17 | 4.27 | 666.9 |
| memtier | passed | 4 | 0 | 132.89 | 0.57 | 0.62 | 0.67 | 1267.9 |
| tsbs | passed | 4 | 0 | 1484.56 | 15.06 | 15.38 | 15.79 | 498.9 |
| ldbc | passed | 4 | 0 | 678.76 | 2.27 | 2.46 | 2.51 | 931.5 |
| ann | passed | 3 | 0 | 222.22 | 17.02 | 17.57 | 17.69 | 482.2 |
| chbench | passed | 3 | 0 | 709.72 | 11.00 | 11.42 | 11.50 | 262.8 |

## Slowest Medium Cases

| Rank | Suite | Case | Avg ms | P95 ms | P99 ms | Ops/sec | Rows |
|---:|---|---|---:|---:|---:|---:|---:|
| 1 | tsbs | Fleet rollup by region | 53.83 | 54.84 | 55.96 | 18.6 | 4 |
| 2 | ann | HNSW nearest neighbor | 44.48 | 45.28 | 45.30 | 22.5 | 10 |
| 3 | chbench | Customer order join | 24.87 | 25.72 | 25.94 | 40.2 | 4 |
| 4 | search | Vector nearest neighbor | 13.45 | 13.94 | 14.22 | 74.4 | 10 |
| 5 | chbench | Warehouse revenue rollup | 8.13 | 8.53 | 8.58 | 122.9 | 5 |
| 6 | ldbc | Tag popularity | 6.56 | 6.91 | 7.02 | 152.4 | 7 |
| 7 | ann | Filtered nearest neighbor | 5.78 | 6.58 | 6.90 | 172.9 | 10 |
| 8 | tpcc | Stock level query | 5.16 | 5.28 | 5.30 | 193.8 | 1 |
| 9 | tpcc | New order transaction | 4.71 | 4.97 | 5.03 | 212.3 | 3 |
| 10 | tsbs | Tag-filtered time range | 3.11 | 3.25 | 3.69 | 321.8 | 100 |

## Throughput Cases

| Suite | Case | Ops | Wall ms | Ops/sec | Note |
|---|---|---:|---:|---:|---|
| ycsb | Mixed 80R/20U throughput | 2000 | 1787.85 | 1118.7 | 2000 ops, 4 threads |
| memtier | Mixed GET/SET throughput | 2000 | 1772.99 | 1128.0 | 2000 ops, 4 threads |
| chbench | Hybrid OLTP/OLAP throughput | 2000 | 3199.11 | 625.2 | 2000 ops, 4 threads |

## Copy Load Smoke

Small-scale `COPY` load mode passed every local suite: 9/9 suites and 39/39 cases. This proves the current file-based copy path can initialize all benchmark-like schemas in the harness.

Representative small copy suite metrics:

| Suite | Load ms | Avg P95 ms | Avg ops/sec |
|---|---:|---:|---:|
| ycsb | 68.19 | 0.69 | 1281.4 |
| tpcc | 197.66 | 2.53 | 608.1 |
| tpch | 788.72 | 35.30 | 68.8 |
| search | 103.87 | 1.63 | 978.1 |
| memtier | 49.26 | 0.63 | 1289.8 |
| tsbs | 232.09 | 2.29 | 706.0 |
| ldbc | 206.49 | 1.18 | 1040.0 |
| ann | 47.20 | 3.89 | 649.2 |
| chbench | 184.89 | 1.70 | 656.3 |

## ANN Correctness Signal

The medium ANN suite ran 5,000 vectors at 128 dimensions.

| Case | Avg ms | P95 ms | Recall@1 | Recall@10 |
|---|---:|---:|---:|---:|
| HNSW nearest neighbor | 44.48 | 45.28 | 1.0 | 1.0 |
| Filtered nearest neighbor | 5.78 | 6.58 | 1.0 | 1.0 |
| Insert vector | 0.80 | 0.84 | n/a | n/a |

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
- File-based benchmark initialization via `COPY` for the local harness suites.

## External Benchmark Readiness

| Target | Status | Reason |
|---|---|---|
| pgbench | tool_missing | `pgbench` is not on PATH |
| sysbench | tool_missing | `sysbench` is not on PATH |
| memtier | tool_missing | `memtier_benchmark` is not on PATH |
| BenchBase TPC-C | artifact_missing | Java exists, but `BENCHBASE_HOME` or `BENCHBASE_JAR` is not configured |
| TSBS | tool_missing | `tsbs_generate_data` is not on PATH |
| LDBC SNB | artifact_missing | Java exists, but `LDBC_SNB_HOME` or `LDBC_DRIVER_HOME` is not configured |
| CH-benCHmark | artifact_missing | Java exists, but no CH benchmark artifact env is configured |

## Production Distance

This is a stable local benchmark lab snapshot, not official production benchmark readiness.

- Local harness readiness is good: full small, full medium, and copy-smoke all pass without benchmark case errors.
- Official benchmark readiness is still blocked at the adapter/tooling layer: no external target is native-ready in the current environment.
- The main database-performance hotspots are grouped rollups, vector nearest-neighbor search, and join-heavy HTAP/graph analytics.
- The next production-grade step is to connect at least one real external workload end-to-end, preferably BenchBase TPC-C or pgbench, while continuing optimizer work on TSBS rollup, CH/LDBC joins, and ANN index metrics.

## Next TASK Signals

- `BENCHPROD-021`: BenchBase TPC-C native smoke adapter.
- `BENCHPROD-020`: pgbench native smoke once `pgbench` is installed and protocol gaps are pinned down.
- `BENCHPROD-040`: TSBS fleet rollup optimizer pass; current medium p95 `54.84 ms`.
- `BENCHPROD-028`: Cost-based optimizer and join ordering for LDBC/CH/TPC-H scale-up.
- `BENCHPROD-044`: External benchmark artifact bootstrap script for BenchBase, TSBS, LDBC, and CH-benCHmark env discovery.
- `BENCHPROD-045`: Benchmark result trend report that compares full-matrix runs across commits and flags regressions.

## Post-Run Hygiene

- `cargo build --release --bin fusiondb`: passed.
- No `fusiondb` server remained listening on `127.0.0.1:8091` or `127.0.0.1:8092` after the matrix runs.
