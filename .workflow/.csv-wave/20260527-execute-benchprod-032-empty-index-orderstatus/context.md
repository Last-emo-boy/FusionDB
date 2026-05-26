# BENCHPROD-032 TPC-C OrderStatus Empty Index Fast Path

Date: 2026-05-27
Scope: FusionDB database core only; dashboard/ui excluded.

## Objective

Improve the TPC-C-like `Order status lookup` medium benchmark without changing the benchmark workload.

## Root Cause

The slow path was not primarily caused by sorting many rows. For many randomly selected customers, `bench_tpcc_orders.c_id = ?` has no matching order. `try_index_scan` correctly returned an exact index plan with zero row ids, but `scan_single_table` only used an index plan when `should_use_index_plan(row_count, ...)` returned true.

Because zero candidates failed that heuristic, an exact empty index hit fell through to a full table scan. The OrderStatus workload executes two queries using the same customer id:

- `SELECT o_id, status, total FROM bench_tpcc_orders WHERE c_id = ? ORDER BY o_id DESC LIMIT 1`
- `SELECT ... FROM bench_tpcc_order_line WHERE o_id IN (SELECT o_id FROM bench_tpcc_orders WHERE c_id = ?) LIMIT 15`

Both queries could pay a full scan when the customer had no orders.

## Implementation

- Treat exact empty index plans as fully applied scans in `scan_single_table`.
- Added a regression test that corrupts a non-matching row payload and proves `WHERE indexed_col = missing` returns empty without decoding/scanning unrelated rows.

## Verification

- `cargo fmt --check`
- `cargo check --lib`
- `cargo test --test sql_index_cache empty_secondary_index_lookup -- --nocapture`
- `cargo build --release --bin fusiondb`
- `python fusiondb_matrix.py --scale medium --suite tpcc --load-mode insert --allow-failures --run-name matrix_tpcc_medium_after_benchprod032_empty_index_20260527`
- `python fusiondb_matrix.py --scale medium --suite all --load-mode insert --allow-failures --run-name matrix_all_medium_after_benchprod032_20260527`

## Result

TPC-C medium after fix:

- Report: `E:/Playground/FusionDB-bench/runs/matrix_tpcc_medium_after_benchprod032_empty_index_20260527/matrix_summary.md`
- `Order status lookup` p95: `2.022 ms`
- `Order status lookup` ops/sec: `601.3`

Full medium matrix after fix:

- Report: `E:/Playground/FusionDB-bench/runs/matrix_all_medium_after_benchprod032_20260527/matrix_summary.md`
- Suite pass rate: `9/9`
- Case pass rate: `39/39`
- `Order status lookup` p95: `2.081 ms`

Baseline from full medium after `BENCHPROD-036`:

- `Order status lookup` p95: `39.258 ms`

## Next TASK Signals

- `BENCHPROD-033`: CH-benCHmark customer order join remains high at `26.125 ms`; improve join planning.
- `BENCHPROD-037`: ANN HNSW nearest neighbor remains high at `39.757 ms`; add recall/build metrics and optimize vector path.
- `BENCHPROD-038`: YCSB short range scan remains high at `10.851 ms`; improve primary-key range scan.
- `BENCHPROD-039`: TSBS fleet rollup remains high at `56.626 ms`; continue aggregate/column scan optimization.
