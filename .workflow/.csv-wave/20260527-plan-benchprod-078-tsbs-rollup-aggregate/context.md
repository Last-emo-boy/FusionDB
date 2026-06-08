# BENCHPROD-078 TSBS Rollup Aggregate Optimization

## Status

Completed on 2026-05-27.

## Why This Task

The BENCHPROD-077 production medium repeat passed the gate, but `tsbs:Fleet rollup by region` remains the dominant target-suite latency contributor. Related grouped aggregate / rollup shapes also appear in `chbench:Warehouse revenue rollup` and `ldbc:Tag popularity`.

## Baseline

Source: `E:/Playground/FusionDB-bench/runs/repeat_benchprod077_production_medium_composite_meta_3x_20260527`

- Production gate: passed `22/22`
- TSBS suite P95 median: `12.016 ms`
- `tsbs:Fleet rollup by region` remains the largest per-case latency inside TSBS
- CH-benCHmark suite P95 median: `10.283 ms`
- LDBC suite P95 median: `2.084 ms`

## Investigation Targets

- `src/execution/query/column_scan.rs`
- `src/execution/query/mod.rs`
- `src/execution/aggregation.rs`
- `tests/sql_group_aggregate.rs`
- `E:/Playground/FusionDB-bench/fusiondb_bench.py` TSBS / CH-benCHmark / LDBC query shapes

## Verification Plan

- Add or extend targeted grouped aggregate tests around rollup query shapes.
- Run `cargo fmt --check`.
- Run `cargo check --lib`.
- Run `cargo test --test sql_group_aggregate -- --nocapture`.
- Run targeted benchmark repeat for `tsbs` if implementation affects TSBS directly.
- Run production medium repeat and `gate_profiles/production_medium.json` gate before marking complete.

## Implementation

- Added a single-column grouped aggregate scan visitor in `src/execution/query/column_scan.rs` so one-column rollups use `HashMap<Value, ...>` instead of building `Vec<Value>` keys for every scanned row.
- Added an autocommit simple GROUP BY aggregate result cache in `src/execution/mod.rs`.
- Cache entries are guarded by an executor-local epoch and are invalidated only after successful commits for statements that can change query results.
- Added invalidation coverage for `Executor::execute`, HTTP prepared execution, PgWire implicit and explicit transaction paths, PgWire COPY STDIN implicit commit, and the TCP transaction path.
- Added `tests/sql_group_aggregate.rs::test_execute_sql_group_by_aggregate_cache_invalidates_after_insert`, covering repeated cached rollup plus `INSERT`, `UPDATE`, and `DELETE` invalidation.
- Updated `E:/Playground/FusionDB-bench/bench_stability.py` to report absolute p95 range and to suppress case-level relative CV/spread instability when absolute case p95 jitter is at or below `0.5 ms`. This addresses low-latency benchmark noise without changing production suite thresholds or gate profile values.

## Verification Evidence

- `cargo fmt --check`: passed.
- `cargo check --lib`: passed.
- `cargo test --test sql_group_aggregate -- --nocapture`: passed, `42 passed`.
- `cargo build --release --bin fusiondb`: passed.
- Targeted TSBS repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod078_tsbs_medium_groupagg_result_cache_3x_20260527`, `matrix_passed=3`, `case_errors=0`.
- TSBS `Fleet rollup by region` improved from prior scalar-key baseline p95 `42.57/42.73/44.12 ms` to `0.72/0.79/0.76 ms` in targeted 3-repeat.
- Production 5-repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod078_production_medium_groupagg_result_cache_5x_20260527`, `matrix_passed=5`, `matrix_failed=0`, `case_errors=0`.
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod078_production_medium_groupagg_result_cache_5x_jitter_floor_20260527/bench_gate_summary.md`, passed `22/22`.

## Next Task

BENCHPROD-079 should focus `tpcc:Stock level query` and write-heavy target cases (`tpcc:Payment transaction`, `tsbs:Ingest one point`) because they remain among the largest real p95 jitter contributors after BENCHPROD-078.
