# BENCHPROD-188 Join Base Projection Preallocation

## Goal

Avoid implicit growth while deriving base-table projection columns from a join stage projection.

## Implementation

- `src/execution/scan/join.rs`
  - Replaced iterator `collect()` with `Vec::with_capacity(stage_projection.len())`.
  - Filled the vector with resolved base column names in stage projection order.
  - Base column resolution, unresolved-column filtering, and fallback decisions remain unchanged.

## Verification

- `cargo test --test sql_join test_join_projection_pushdown_with_group_by -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_join_left_filter_projection_skips_unused_left_column_decode -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_join_index_probe_projection_skips_unused_right_column_decode -- --nocapture`
  - Passed.
- `cargo test --test sql_join`
  - Passed: 31/31.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-188` is complete. Join base projection construction now sizes its output vector from the stage projection count.
