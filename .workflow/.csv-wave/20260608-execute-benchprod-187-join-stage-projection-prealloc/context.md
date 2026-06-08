# BENCHPROD-187 Join Stage Projection Preallocation

## Goal

Avoid implicit growth while building the join stage projection column list.

## Implementation

- `src/execution/scan/join.rs`
  - Replaced iterator `collect()` with `Vec::with_capacity(required_indices.len())`.
  - Filled the vector by iterating schema columns in order and pushing required columns.
  - Required index filtering, schema-order output, and projection fallback behavior remain unchanged.

## Verification

- `cargo test --test sql_join test_join_projection_pushdown_with_group_by -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_join_left_filter_projection_skips_unused_left_column_decode -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_comma_join_projection_keeps_right_columns_without_join_pushdown -- --nocapture`
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

`BENCHPROD-187` is complete. Join stage projection construction now sizes its output vector from the required index count.
