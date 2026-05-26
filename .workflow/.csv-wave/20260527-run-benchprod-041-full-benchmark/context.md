# BENCHPROD-041 Full Benchmark Capability Snapshot

Date: 2026-05-27
Scope: benchmark evidence capture only; FusionDB database core and dashboard/ui unchanged.

## Objective

Run the full FusionDB benchmark matrix at medium scale and summarize what FusionDB can currently execute, the measured latency/throughput profile, and the remaining gap to production-grade external benchmark coverage.

## Environment

- FusionDB commit: `df82a2bb6ef89d10cd2c2f5657242577fec5215b`
- FusionDB-bench commit: `b94c891e5e9d21bd2c9e36953a5854392817a585`
- Binary: `E:/Playground/FusionDB/target/release/fusiondb.exe`
- Host CPU: `AMD EPYC 9654 96-Core Processor`
- Memory: `68718481408` bytes
- Rust: `rustc 1.94.1`
- Cargo: `cargo 1.94.1`
- Python: `3.14.2`

## Command

```powershell
cd E:\Playground\FusionDB
cargo build --release --bin fusiondb

cd E:\Playground\FusionDB-bench
python fusiondb_matrix.py --scale medium --suite all --load-mode insert --allow-failures --run-name matrix_all_medium_full_benchmark_20260527
```

## Result Artifacts

- Matrix Markdown: `E:/Playground/FusionDB-bench/runs/matrix_all_medium_full_benchmark_20260527/matrix_summary.md`
- Matrix JSON: `E:/Playground/FusionDB-bench/runs/matrix_all_medium_full_benchmark_20260527/matrix_summary.json`
- Per-suite logs and reports: `E:/Playground/FusionDB-bench/runs/matrix_all_medium_full_benchmark_20260527/<suite>/`

## Summary

- Scale: `medium`
- Load mode: `insert`
- Threads: `4`
- Suite pass rate: `9/9`
- Case pass rate: `39/39`
- Failed suites: none
- Failed cases: none

## Suite Metrics

| Suite | Status | Cases | Case errors | Load ms | Avg ms | Avg P50 ms | Avg P95 ms | Avg P99 ms | Avg ops/sec |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ycsb | passed | 6 | 0 | 168.268 | 1.645 | 1.640 | 2.419 | 2.524 | 1087.1 |
| tpcc | passed | 5 | 0 | 763.970 | 2.862 | 2.837 | 3.043 | 3.199 | 537.1 |
| tpch | passed | 5 | 0 | 2760.452 | 1.051 | 1.002 | 1.334 | 1.405 | 1107.8 |
| search | passed | 5 | 0 | 334.147 | 4.656 | 4.472 | 6.137 | 6.260 | 626.1 |
| memtier | passed | 4 | 0 | 138.315 | 0.543 | 0.537 | 0.582 | 0.597 | 1314.2 |
| tsbs | passed | 4 | 0 | 1436.520 | 15.252 | 15.003 | 16.327 | 16.566 | 508.2 |
| ldbc | passed | 4 | 0 | 655.253 | 2.284 | 2.261 | 2.418 | 2.492 | 937.7 |
| ann | passed | 3 | 0 | 140.457 | 16.786 | 16.726 | 17.135 | 17.709 | 483.5 |
| chbench | passed | 3 | 0 | 635.661 | 11.454 | 11.344 | 12.153 | 12.701 | 266.3 |

## Slowest Case Signals

| Rank | Suite | Case | Avg ms | P50 ms | P95 ms | P99 ms | Ops/sec |
|---:|---|---|---:|---:|---:|---:|---:|
| 1 | tsbs | Fleet rollup by region | 55.054 | 54.127 | 58.834 | 59.428 | 18.2 |
| 2 | ann | HNSW nearest neighbor | 44.249 | 44.078 | 44.907 | 46.506 | 22.6 |
| 3 | chbench | Customer order join | 25.832 | 25.528 | 27.557 | 29.072 | 38.7 |
| 4 | search | Vector nearest neighbor | 16.110 | 15.405 | 22.237 | 22.583 | 62.1 |
| 5 | ycsb | Short range scan | 6.834 | 6.852 | 11.167 | 11.538 | 146.3 |
| 6 | chbench | Warehouse revenue rollup | 8.529 | 8.503 | 8.903 | 9.032 | 117.2 |
| 7 | ldbc | Tag popularity | 6.631 | 6.603 | 6.934 | 6.980 | 150.8 |

## Current Capability Snapshot

FusionDB currently executes medium-scale local benchmark-like workloads for:

- YCSB-style serving paths: primary-key read/update/insert, secondary equality lookup, ordered short range scan, and mixed 80R/20U throughput.
- TPC-C-style simplified OLTP transactions: NewOrder, Payment, OrderStatus, StockLevel, and Delivery.
- TPC-H-style simplified analytical queries: Q1/Q3/Q6/Q10/TopK shapes over local harness tables.
- Search/vector paths: FTS `MATCH`, `LIKE`, vector nearest neighbor, filtered vector search, and `EMBEDDING`.
- memtier-style SQL KV paths: GET, SET, ADD, and mixed GET/SET throughput.
- TSBS-style time-series paths: tag/time range, region rollup, latest points, and ingest.
- LDBC-style relational graph paths: one-hop, two-hop, recent posts by friends, and tag popularity.
- ANN-style vector paths: HNSW nearest neighbor, filtered nearest neighbor, and vector insert.
- CH-benCHmark-style HTAP paths: mixed OLTP/OLAP throughput, warehouse rollup, and customer-order join.

## Production Distance

This run proves the local medium matrix is stable across all current harness suites. It does not yet prove official benchmark readiness.

- TPC-C / BenchBase: local TPC-C-like shapes pass, but official BenchBase/JDBC execution still needs schema/query/dialect parity and transaction accounting.
- memtier: SQL KV-like paths pass, but Redis/Memcached protocol compatibility is not implemented.
- TSBS: local integer-timestamp workload passes, but official TSBS adapter, native TIMESTAMP semantics, partition/time-index planning, and ingest pipeline pressure are still open.
- LDBC SNB: relational graph-like probes pass, but full SNB dataset loading, query set coverage, path semantics, and join-order/cardinality optimization remain open.
- ANN benchmarks: latency probes pass, but recall@k ground truth, build metrics, index size, and real dataset adapters are still missing.
- CH-benCHmark: local hybrid shapes pass, but official schema/query mix and long-running HTAP correctness/soak coverage remain open.
- sysbench / pgbench: native external smoke coverage remains open despite the database now having PgWire work from previous tasks.

## Next TASK Signals

- `BENCHPROD-040`: TSBS fleet rollup optimizer pass; current p95 `58.834 ms`.
- `BENCHPROD-037`: ANN recall and HNSW metric expansion; current HNSW p95 `44.907 ms`.
- `BENCHPROD-034`: LDBC tag aggregation fast path; current tag popularity p95 `6.934 ms`.
- `BENCHPROD-038`: YCSB range scan pushdown optimization; current short range p95 `11.167 ms`.
- `BENCHPROD-021`: BenchBase TPC-C native smoke; local TPC-C-like suite is passing but not official.
- `BENCHPROD-020`: pgbench native smoke; still missing external pgbench result.

