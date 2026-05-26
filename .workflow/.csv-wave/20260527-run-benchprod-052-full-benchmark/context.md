# BENCHPROD-052 Full Benchmark And Production Repeat

Date: 2026-05-27
Scope: benchmark evidence only; database code, dashboard, and benchmark harness code unchanged.

## Objective

Run a current full benchmark so we know what FusionDB can execute today, what the numbers look like, and which results are stable enough to use as a performance gate.

## Commands

Repository: `E:/Playground/FusionDB`

```powershell
cargo build --release --bin fusiondb
```

Repository: `E:/Playground/FusionDB-bench`

```powershell
python bench_repeat.py --scale medium --suite production --repeats 3 --threads 4 --run-name repeat_benchprod052_medium_production_3x_20260527 --suite-timeout 3600 --matrix-timeout 7200
python fusiondb_matrix.py --scale medium --suite all --threads 4 --run-name matrix_benchprod052_all_medium_insert_20260527 --suite-timeout 3600
```

## Artifacts

- Production repeat summary: `E:/Playground/FusionDB-bench/runs/repeat_benchprod052_medium_production_3x_20260527/bench_repeat_summary.md`
- Production stability report: `E:/Playground/FusionDB-bench/runs/repeat_benchprod052_medium_production_3x_20260527/stability/bench_stability_summary.md`
- Production trend 1 to 2: `E:/Playground/FusionDB-bench/runs/repeat_benchprod052_medium_production_3x_20260527/trends/trend_01_to_02/bench_trend_summary.md`
- Production trend 2 to 3: `E:/Playground/FusionDB-bench/runs/repeat_benchprod052_medium_production_3x_20260527/trends/trend_02_to_03/bench_trend_summary.md`
- All-suite matrix: `E:/Playground/FusionDB-bench/runs/matrix_benchprod052_all_medium_insert_20260527/matrix_summary.md`

## Production Medium Repeat Result

Profile:

| Field | Value |
|---|---:|
| Scale | medium |
| Suites | tpcc, memtier, tsbs, ldbc, chbench |
| Repeats | 3 |
| Threads | 4 |
| Load mode | insert |
| Matrix runs passed | 3 |
| Matrix runs failed | 0 |
| Cases per matrix | 20 |
| Case errors | 0 |

Suite stability:

| Suite | Status | P95 median ms | P95 CV | P95 spread | Ops/sec median |
|---|---|---:|---:|---:|---:|
| chbench | stable | 13.666 | 10.64% | 23.60% | 251.2 |
| ldbc | unstable | 2.697 | 16.35% | 37.23% | 919.7 |
| memtier | stable | 0.692 | 5.86% | 11.96% | 1216.6 |
| tpcc | stable | 3.614 | 11.05% | 24.25% | 522.6 |
| tsbs | stable | 12.580 | 3.78% | 7.41% | 463.3 |

Unstable production cases:

| Suite | Case | P95 median ms | P95 CV | P95 spread | Ops/sec median |
|---|---|---:|---:|---:|---:|
| ldbc | Tag popularity | 8.101 | 21.71% | 52.41% | 136.7 |
| tsbs | Ingest one point | 1.060 | 19.75% | 48.09% | 1115.4 |
| tpcc | Stock level query | 7.329 | 18.03% | 42.90% | 173.9 |
| memtier | ADD new key | 0.948 | 14.93% | 35.24% | 1227.6 |
| chbench | Warehouse revenue rollup | 11.507 | 14.06% | 30.15% | 113.3 |
| tpcc | Delivery status update | 0.919 | 13.59% | 27.89% | 1223.0 |

Adjacent trend notes:

- Repeat 1 to 2: no suite regressions; 1 case regression, 5 case improvements.
- Repeat 2 to 3: 1 suite regression in chbench; 3 case regressions; no case errors.
- Interpretation: this is a usable execution/error gate and provisional performance baseline. Latency gates should use suite medians plus exception handling for unstable cases until variance is reduced.

## All-Suite Medium Matrix Result

Profile:

| Field | Value |
|---|---:|
| Scale | medium |
| Suites | ycsb, tpcc, tpch, search, memtier, tsbs, ldbc, ann, chbench |
| Threads | 4 |
| Load mode | insert |
| Suites passed | 9/9 |
| Cases | 39 |
| Case errors | 0 |

Suite summary:

| Suite | Cases | Load ms | Avg ms | Avg P95 ms | Avg ops/sec |
|---|---:|---:|---:|---:|---:|
| ycsb | 6 | 182.720 | 0.651 | 0.706 | 1255.5 |
| tpcc | 5 | 726.887 | 3.042 | 3.358 | 523.0 |
| tpch | 5 | 2425.801 | 0.999 | 1.060 | 1152.0 |
| search | 5 | 328.239 | 4.035 | 4.156 | 648.1 |
| memtier | 4 | 123.399 | 0.573 | 0.621 | 1264.9 |
| tsbs | 4 | 1480.761 | 11.648 | 12.101 | 499.8 |
| ldbc | 4 | 691.600 | 2.345 | 2.578 | 909.1 |
| ann | 3 | 213.078 | 15.210 | 15.621 | 494.3 |
| chbench | 3 | 644.309 | 11.256 | 11.864 | 263.3 |

Slowest all-suite cases by p95:

| Suite | Case | P95 ms | Avg ms | Ops/sec |
|---|---|---:|---:|---:|
| tsbs | Fleet rollup by region | 41.240 | 40.155 | 24.9 |
| ann | HNSW nearest neighbor | 40.304 | 39.417 | 25.4 |
| chbench | Customer order join | 26.503 | 25.323 | 39.5 |
| search | Vector nearest neighbor | 13.801 | 13.484 | 74.2 |
| chbench | Warehouse revenue rollup | 9.088 | 8.446 | 118.4 |
| ldbc | Tag popularity | 7.430 | 6.791 | 147.2 |
| ann | Filtered nearest neighbor | 5.727 | 5.429 | 184.2 |
| tpcc | Stock level query | 5.528 | 5.300 | 188.7 |
| tpcc | New order transaction | 5.335 | 4.867 | 205.4 |
| tsbs | Tag-filtered time range | 3.593 | 3.172 | 315.3 |

ANN additional metrics:

- HNSW nearest neighbor: `recall_at_1_avg=1.000`, `recall_at_10_avg=1.000`, `hnsw_registered_vectors=5000`, `embedding_dim=128`.
- Filtered nearest neighbor: `recall_at_1_avg=1.000`, `recall_at_10_avg=1.000`, average candidate count 500.

## Current Capability

FusionDB can currently run local benchmark-like coverage for:

- YCSB-style point/range/index/mixed read-write operations.
- TPC-C-style transaction fragments for new order, payment, order status, stock level, and delivery.
- TPC-H-style analytical query fragments.
- Search workloads: FTS MATCH, LIKE prefix/contains, vector search, and embedding.
- memtier-like SQL key/value operations.
- TSBS-style time-series query and ingest fragments.
- LDBC-style graph traversal and tag popularity fragments.
- ANN HNSW vector search with recall metrics.
- CH-benCHmark-like hybrid OLTP/OLAP fragments.

This is still local benchmark-like coverage rather than official benchmark compatibility. The readiness report continues to classify production readiness as `prototype-to-benchmark-lab; not official-production-ready`.

## Assessment

The current benchmark suite is strong enough to detect functional failures and broad performance drift across 9 internal suites. For production-grade credibility, the main gaps are still official adapters/protocol fidelity, larger and longer runs, realistic data import, richer type compatibility, and optimizer/statistics maturity.

The immediate performance hotspots are TSBS fleet rollup, ANN HNSW nearest neighbor, CH-benCHmark customer order join, and LDBC tag popularity. The immediate benchmark-process hotspot is variance in the LDBC suite and 6 production micro-cases.

## Next TASK Signals

- `BENCHPROD-048`: Optimize and stabilize LDBC Tag popularity using repeat/stability gates.
- `BENCHPROD-053`: Convert production medium repeat x3 into a formal benchmark gate with thresholds for errors, suite latency, and known noisy cases.
- `BENCHPROD-054`: Add official benchmark adapter inventory and runnable smoke for BenchBase TPC-C or pgbench under `E:/Playground`.
- `BENCHPROD-055`: Add TSBS official data/query adapter path and timestamp-native coverage.
- `BENCHPROD-056`: Add ANN real-dataset import plus external ground-truth validation.
- `BENCHPROD-057`: Add CH-benCHmark official-shape schema/query coverage and mixed HTAP snapshot checks.
