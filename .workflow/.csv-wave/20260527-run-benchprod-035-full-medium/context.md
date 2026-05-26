# BENCHPROD-035 Full Medium Benchmark Matrix

Date: 2026-05-27
Scope: benchmark evidence capture only; FusionDB database core and dashboard/ui unchanged.

## Objective

Run the full FusionDB benchmark matrix at medium scale to answer what FusionDB can currently execute and where the next production gaps are.

## Environment

- FusionDB commit: `a497a0fbe83ae67f42a3c59a38af0adf194d1b42`
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
python fusiondb_matrix.py --scale medium --suite all --load-mode insert --allow-failures --run-name matrix_all_medium_insert_20260527_full
```

## Result

Report:

- `E:/Playground/FusionDB-bench/runs/matrix_all_medium_insert_20260527_full/matrix_summary.md`
- `E:/Playground/FusionDB-bench/runs/matrix_all_medium_insert_20260527_full/matrix_summary.json`

Summary:

- Suite pass rate: `8/9`
- Passed: `ycsb`, `tpcc`, `search`, `memtier`, `tsbs`, `ldbc`, `ann`, `chbench`
- Failed: `tpch`
- Total cases: `39`
- Passing cases: `34`
- Failing cases: `5`

## Suite Metrics

| Suite | Status | Cases | Case errors | Load ms | Avg P95 ms | Avg ops/sec |
|---|---|---:|---:|---:|---:|---:|
| ycsb | passed | 6 | 0 | 160.593 | 2.334 | 1089.9 |
| tpcc | passed | 5 | 0 | 624.950 | 10.177 | 394.9 |
| tpch | failed | 5 | 5 | 2337.873 | 0.000 | 0.0 |
| search | passed | 5 | 0 | 298.751 | 4.362 | 664.2 |
| memtier | passed | 4 | 0 | 118.611 | 0.612 | 1297.2 |
| tsbs | passed | 4 | 0 | 1562.598 | 15.697 | 494.5 |
| ldbc | passed | 4 | 0 | 673.450 | 2.479 | 958.6 |
| ann | passed | 3 | 0 | 122.662 | 16.853 | 485.7 |
| chbench | passed | 3 | 0 | 657.152 | 13.082 | 263.9 |

## Capability Snapshot

FusionDB currently runs medium-scale like workloads for:

- YCSB-style primary-key read/update/insert, secondary equality lookup, short range scan, and mixed 80R/20U throughput.
- TPC-C-style simplified transaction shapes: NewOrder, Payment, OrderStatus, StockLevel, Delivery.
- Search/vector queries: FTS `MATCH`, `LIKE`, vector nearest-neighbor, embedding function.
- memtier-style SQL KV GET/SET/ADD and mixed throughput.
- TSBS-style tag/time range, rollup, latest point, and ingest.
- LDBC-style one-hop, two-hop, recent posts, and tag aggregation over relational graph tables.
- ANN-style nearest-neighbor, filtered nearest-neighbor, and vector insert latency.
- CH-benCHmark-style hybrid write/read throughput plus rollup and join probes.

## Failure Notes

TPC-H-like query cases all failed with missing-table errors after setup:

- `bench_tpch_lineitem not found`
- `bench_tpch_customer not found`
- `bench_tpch_orders not found`

The setup phase reported `tpch_setup_ms = 2337.873` and server logs showed index creation slow-query entries, so this is not a slow query result. Treat it as a high-priority persistence/catalog/setup correctness gap before using TPC-H performance numbers.

## Next TASK Signals

- `BENCHPROD-036`: TPC-H catalog/setup persistence gap. Reproduce with isolated create/insert/index/query flow and fix missing table visibility before measuring TPC-H performance.
- `BENCHPROD-032`: TPC-C OrderStatus remains the slowest TPC-C case, p95 `37.314 ms`; optimize indexed top-k/subquery path.
- `BENCHPROD-033`: CH-benCHmark customer order join remains p95 `25.314 ms`; improve join/rollup planning.
- `BENCHPROD-037`: ANN HNSW nearest neighbor p95 `44.102 ms`; add recall/build metrics and inspect vector index scan path.
- `BENCHPROD-038`: YCSB short range scan p95 `10.750 ms`; improve primary-key range scan pushdown and ordered limit handling.
- `BENCHPROD-027`: Promote benchmark credibility reporting: environment, commit hashes, run parameters, correctness checks, and normalized per-suite pass criteria.
