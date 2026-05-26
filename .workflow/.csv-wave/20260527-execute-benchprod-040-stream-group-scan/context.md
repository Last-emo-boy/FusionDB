# BENCHPROD-040 Streaming Group Aggregate Scan

Date: 2026-05-27
Scope: database body only; dashboard/ui unchanged.

## Objective

Optimize the TSBS medium `Fleet rollup by region` workload by removing full prefix materialization from the group aggregate fast path.

The workload query is:

```sql
SELECT region, AVG(usage_user), MAX(usage_system)
FROM bench_tsbs_cpu
WHERE ts >= 1000 AND ts < 50000
GROUP BY region
ORDER BY region
```

## Implementation

- Added `ScanVisitor` in `src/storage/mod.rs`.
- Added `Transaction::scan_prefix_for_each` to visit visible prefix rows without allocating a full `Vec<(Vec<u8>, Vec<u8>)>`.
- Implemented `scan_prefix_for_each` for:
  - `MemoryTransaction`
  - `FusionTransaction`
- Changed `group_by_column_aggregate_scan` in `src/execution/query/column_scan.rs` to stream rows through `GroupAggregateScanVisitor`.
- Removed the previous per-row predicate `HashMap` cache attempt from `matches_data`; it had not improved benchmark results.
- Kept the multi-predicate partial decode SQL test because it protects the fast path from decoding unused columns.

## Correctness Verification

| Command | Result |
|---|---|
| `cargo fmt --check` | passed |
| `cargo test test_scan_prefix_for_each_merges_write_buffer_without_materializing -- --nocapture` | passed |
| `cargo test fusion_scan_prefix_for_each_matches_scan_prefix_after_overwrite_delete_and_write_buffer -- --nocapture` | passed |
| `cargo test --test sql_group_aggregate -- --nocapture` | passed, 35 tests |
| `cargo test --lib -- --nocapture` | passed, 107 tests |
| `cargo build --release --bin fusiondb` | passed |

## Benchmark Verification

Artifacts:

- TSBS medium targeted: `E:/Playground/FusionDB-bench/runs/matrix_tsbs_medium_after_benchprod040_stream_scan_20260527/matrix_summary.md`
- Full medium matrix: `E:/Playground/FusionDB-bench/runs/matrix_all_medium_insert_benchprod040_stream_scan_20260527/matrix_summary.md`
- Trend vs `BENCHPROD-043`: `E:/Playground/FusionDB-bench/runs/trend_benchprod040_stream_vs_043_full_20260527/bench_trend_summary.md`
- Trend vs `BENCHPROD-046`: `E:/Playground/FusionDB-bench/runs/trend_benchprod040_stream_vs_046_full_20260527/bench_trend_summary.md`

Full medium matrix:

| Run | Scale | Load mode | Suites | Cases | Errors |
|---|---|---|---:|---:|---:|
| `matrix_all_medium_insert_benchprod040_stream_scan_20260527` | medium | insert | 9/9 | 39/39 | 0 |

TSBS medium suite:

| Case | Avg ms | P95 ms | P99 ms | Ops/sec |
|---|---:|---:|---:|---:|
| Tag-filtered time range | 3.060 | 3.152 | 3.172 | 326.8 |
| Fleet rollup by region | 39.321 | 40.633 | 41.193 | 25.4 |
| Latest points for host | 2.466 | 2.513 | 2.534 | 405.6 |
| Ingest one point | 0.743 | 0.787 | 0.799 | 1345.7 |

Target case trend:

| Baseline | P95 ms | Delta |
|---|---:|---:|
| `BENCHPROD-043` full medium | 54.845 | baseline |
| `BENCHPROD-046` dirty working tree | 63.311 | +15.43% vs `BENCHPROD-043` |
| `BENCHPROD-040` streaming scan | 40.633 | -25.91% vs `BENCHPROD-043`; -35.82% vs `BENCHPROD-046` |

Suite-level trend vs `BENCHPROD-043`:

| Suite | Baseline P95 | Current P95 | Delta | Status |
|---|---:|---:|---:|---|
| tsbs | 15.383 | 11.771 | -23.48% | improvement |
| ldbc | 2.464 | 3.238 | +31.41% | regression/noise to investigate separately |

Suite-level trend vs `BENCHPROD-046`:

| Suite | Baseline P95 | Current P95 | Delta | Status |
|---|---:|---:|---:|---|
| tsbs | 17.526 | 11.771 | -32.84% | improvement |
| chbench | 14.876 | 10.806 | -27.36% | improvement |

## Current Assessment

`BENCHPROD-040` is now a proven optimization for the intended TSBS rollup path. It also repairs the broader performance regression seen in `BENCHPROD-046` by removing the unsuccessful predicate decode-cache attempt and replacing the core scan shape with streaming aggregation.

Remaining trend noise is real enough to track but outside this task:

- LDBC `Tag popularity` and `Recent posts by friends` show variance/regression against `BENCHPROD-043`.
- Search `FTS MATCH two terms` regressed against `BENCHPROD-043` but improved versus `BENCHPROD-046`.
- TPC-H `Q6 revenue filter` is a micro-latency case with small absolute values and should be confirmed with repeated runs before treating as a core regression.

## Next TASK Signals

- `BENCHPROD-047`: Extend streaming visitors to simple aggregate/count/distinct fast paths.
- `BENCHPROD-048`: Investigate LDBC tag aggregation and join-order/cardinality model.
- `BENCHPROD-049`: Configure first native external benchmark smoke under `E:/Playground`, preferably pgbench or BenchBase TPC-C.
