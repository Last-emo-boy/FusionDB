# BENCHPROD-192 Right-Stage Join Predicate Preallocation

## Goal

Avoid implicit growth while building the predicate vector used to derive right-side join stage projection.

## Implementation

- `src/execution/scan/join.rs`
  - Replaced chained iterator `collect()` for `right_stage_predicates` with an explicit `Vec::with_capacity`.
  - Capacity is computed from `pending_predicates.len()` plus optional `right_selection` and `join_expr` inputs.
  - Predicate ordering and cloning behavior remain unchanged.

## Verification

- `cargo test --test sql_join test_join_index_probe_projection_skips_unused_right_column_decode -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_inner_join_with_left_filter_and_indexed_right_probe -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_two_hop_join_probe_skips_guaranteed_right_key_decode -- --nocapture`
  - Passed.
- `cargo test --test sql_join`
  - Passed: 31/31.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-192` is complete. Right-stage join predicate collection now uses an explicitly preallocated vector.
