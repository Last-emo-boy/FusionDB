# BENCHPROD-031 TSBS Ordered Range And Rollup Optimization

Date: 2026-05-27
Scope: FusionDB database core only; dashboard/ui excluded.

## Objective

Use the production matrix benchmark evidence to improve TSBS-like medium performance for:

- `Tag-filtered time range`
- `Latest points for host`
- `Fleet rollup by region`

## Baseline

Baseline report:

- `E:/Playground/FusionDB-bench/runs/matrix_production_medium_insert_20260527_012553/matrix_summary.md`

Baseline TSBS medium p95:

- Tag-filtered time range: `68.715 ms`
- Fleet rollup by region: `102.344 ms`
- Latest points for host: `67.151 ms`

## Implementation

- Extended composite BTree index scan planning to use the longest available equality prefix instead of requiring equality on every indexed column.
- This lets `(host_id, ts)` serve `host_id = ? AND ts >= ... AND ts < ...` by narrowing candidates before row filtering.
- Extended column-scan aggregate predicates to support simple conjunctive `AND` predicates.
- This lets TSBS rollup shape `ts >= low AND ts < high GROUP BY region` stay on the column aggregate path.

## Verification

- `cargo fmt --check`
- `cargo check --lib`
- `cargo test --test sql_index_cache composite -- --nocapture`
- `cargo test --test sql_group_aggregate group_by_aggregates -- --nocapture`
- `cargo test --test sql_group_aggregate group_by_reuses -- --nocapture`
- `cargo build --release --bin fusiondb`
- `python fusiondb_matrix.py --scale medium --suite tsbs --load-mode insert --allow-failures --run-name matrix_tsbs_medium_after_benchprod031_20260527`
- `python fusiondb_matrix.py --scale medium --suite production --load-mode insert --allow-failures --run-name matrix_production_medium_after_benchprod031_20260527`

## Result

Production medium matrix after optimization:

- `E:/Playground/FusionDB-bench/runs/matrix_production_medium_after_benchprod031_20260527/matrix_summary.md`

TSBS medium p95 after optimization:

- Tag-filtered time range: `2.914 ms`
- Fleet rollup by region: `59.907 ms`
- Latest points for host: `2.779 ms`

The production target suite matrix still passed 5/5 suites.

