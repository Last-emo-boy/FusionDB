# BENCHPROD-186 Join Required Indices Preallocation

## Goal

Avoid growth reallocations while collecting required column indices for join stage projection pushdown.

## Implementation

- `src/execution/scan/join.rs`
  - Computed `required_capacity` from projected columns, pending predicates, and join column references.
  - Capped the capacity by `schema.columns.len()`.
  - Initialized `required_indices` with `HashSet::with_capacity(required_capacity)`.
  - Index resolution, required column collection, and projection decision behavior remain unchanged.

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

`BENCHPROD-186` is complete. Join stage projection now sizes the required-index set from known inputs before collection.
