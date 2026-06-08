# BENCHPROD-194 ORDER BY Window Row Preallocation

## Goal

Avoid implicit vector growth while preparing and restoring rows in the `ORDER BY` limit/offset Top-N window path.

## Implementation

- `src/execution/query/order.rs`
  - Replaced iterator `collect()` for indexed Top-N input rows with `Vec::with_capacity(taken_rows.len())`.
  - Replaced final row restoration `collect()` with a preallocated vector sized from the truncated window.
  - Preserved original-index tie ordering and the existing `select_nth_unstable_by` plus final stable ordering behavior.

## Verification

- `cargo test --test sql_select test_select_order_by_limit_offset -- --nocapture`
  - Passed.
- `cargo test --test sql_index_cache test_select_order_by_primary_key_limit_offset -- --nocapture`
  - Passed.
- `cargo test --test sql_group_aggregate test_group_by_aggregate_order_by_limit_offset_topn_window -- --nocapture`
  - Passed.
- `cargo test --test sql_select`
  - Passed: 27/27.
- `cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `cargo test --test sql_index_cache`
  - Passed: 37/37.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-194` is complete. `ORDER BY` limit/offset Top-N window sorting now uses explicitly preallocated intermediate row vectors where the required capacities are known.
