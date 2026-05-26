# BENCHPROD-039 Full Medium Capability Snapshot

Date: 2026-05-27
Scope: benchmark evidence capture only; FusionDB database core and dashboard/ui unchanged.

## Objective

Run the full FusionDB benchmark matrix at medium scale after the latest optimizer/storage fixes, and record what FusionDB can currently execute plus the remaining production benchmark gaps.

## Environment

- FusionDB commit: `3ede2f8dab510f05629902f05b5bf1a740b78155`
- FusionDB-bench commit: `d10715dce32c0d0d1022c2cb34d2f742c2c542a3`
- Binary: `E:/Playground/FusionDB/target/release/fusiondb.exe`
- Host CPU: `AMD EPYC 9654 96-Core Processor`, 16 logical processors visible
- Memory: `68718481408` bytes
- Rust: `rustc 1.94.1`
- Cargo: `cargo 1.94.1`
- Python: `3.14.2`

## Command

```powershell
cargo build --release --bin fusiondb
cd E:\Playground\FusionDB-bench
python fusiondb_matrix.py --scale medium --suite all --load-mode insert --allow-failures --run-name matrix_all_medium_full_observe_20260527
```

## Result

Report:

- `E:/Playground/FusionDB-bench/runs/matrix_all_medium_full_observe_20260527/matrix_summary.md`
- `E:/Playground/FusionDB-bench/runs/matrix_all_medium_full_observe_20260527/matrix_summary.json`

Summary:

- Suite pass rate: `9/9`
- Passed suites: `ycsb`, `tpcc`, `tpch`, `search`, `memtier`, `tsbs`, `ldbc`, `ann`, `chbench`
- Failed suites: none
- Total cases: `39`
- Passing cases: `39`
- Failing cases: `0`

## Suite Metrics

| Suite | Status | Cases | Case errors | Load ms | Avg P95 ms | Avg ops/sec |
|---|---|---:|---:|---:|---:|---:|
| ycsb | passed | 6 | 0 | 166.542 | 2.514 | 1074.2 |
| tpcc | passed | 5 | 0 | 612.240 | 3.087 | 556.6 |
| tpch | passed | 5 | 0 | 2386.297 | 1.275 | 1082.5 |
| search | passed | 5 | 0 | 322.528 | 4.093 | 677.5 |
| memtier | passed | 4 | 0 | 115.893 | 0.619 | 1290.0 |
| tsbs | passed | 4 | 0 | 1552.426 | 16.438 | 524.2 |
| ldbc | passed | 4 | 0 | 680.033 | 2.451 | 948.0 |
| ann | passed | 3 | 0 | 122.788 | 17.214 | 489.9 |
| chbench | passed | 3 | 0 | 620.779 | 16.758 | 280.8 |

## Slowest Case Signals

| Rank | Suite | Case | Avg ms | P95 ms | Ops/sec | Rows |
|---:|---|---|---:|---:|---:|---:|
| 1 | tsbs | Fleet rollup by region | 54.492 | 59.256 | 18.4 | 4 |
| 2 | ann | HNSW nearest neighbor | 43.736 | 45.222 | 22.9 | 10 |
| 3 | chbench | Customer order join | 27.160 | 36.301 | 36.8 | 4 |
| 4 | chbench | Warehouse revenue rollup | 9.067 | 13.972 | 110.3 | 5 |
| 5 | search | Vector nearest neighbor | 13.305 | 13.587 | 75.2 | 10 |
| 6 | ycsb | Short range scan | 6.901 | 11.563 | 144.9 | 100 |
| 7 | ldbc | Tag popularity | 6.734 | 7.125 | 148.5 | 7 |

## Capability Snapshot

FusionDB currently runs medium-scale like workloads for:

- YCSB-style primary-key read/update/insert, secondary equality lookup, short range scan, and mixed 80R/20U throughput.
- TPC-C-style simplified transaction shapes: NewOrder, Payment, OrderStatus, StockLevel, Delivery.
- TPC-H-style simplified analytical query shapes: pricing summary, segment revenue, revenue filter, returned-item revenue, and top orders.
- Search/vector queries: FTS `MATCH`, `LIKE`, vector nearest-neighbor, and embedding function.
- memtier-style SQL KV GET/SET/ADD and mixed throughput.
- TSBS-style tag/time range, fleet rollup, latest point, and ingest.
- LDBC-style one-hop, two-hop, recent posts, and tag aggregation over relational graph tables.
- ANN-style HNSW nearest-neighbor, filtered nearest-neighbor, and vector insert latency.
- CH-benCHmark-style hybrid write/read throughput plus rollup and join probes.

## Production Distance

This is a credible local medium-scale harness, not yet an official production-grade benchmark surface.

- Official TPC-C/BenchBase still needs JDBC/PgWire compatibility coverage, official schema/load/query mix, and transaction correctness accounting.
- memtier remains SQL KV-like, not Redis/Memcached protocol native.
- TSBS still uses harness-native integer timestamps and simplified query shapes, not the official TSBS adapter.
- LDBC is relational graph-like coverage, not full SNB dataset, query set, or path semantics.
- ANN has latency coverage but still lacks recall@k ground truth, build metrics, index size, and dataset adapters.
- CH-benCHmark is hybrid-shape coverage, not full official CH schema/query set or long mixed HTAP soak.

## Next TASK Signals

- `BENCHPROD-033`: CH-benCHmark join and rollup optimizer pass; current join p95 `36.301 ms`, rollup p95 `13.972 ms`.
- `BENCHPROD-037`: ANN recall and HNSW metric expansion; current HNSW nearest-neighbor p95 `45.222 ms`.
- `BENCHPROD-040`: TSBS fleet rollup optimizer pass; current rollup p95 `59.256 ms`.
- `BENCHPROD-038`: YCSB range scan pushdown optimization; current short range scan p95 `11.563 ms`.
- `BENCHPROD-034`: LDBC tag aggregation fast path; current tag popularity p95 `7.125 ms`.
- `BENCHPROD-027`: Benchmark credibility report; promote environment, commit hashes, warmup, correctness checks, and normalized per-suite pass criteria.

## Post-Run Hygiene

- `fusiondb` process check: no process remained after matrix completion.
- Port check: `127.0.0.1:8091` and `127.0.0.1:8092` were free after the run.
