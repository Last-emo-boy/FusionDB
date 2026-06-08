# BENCHPROD-191 Join Optional Predicate Preallocation

## Goal

Avoid implicit growth while merging optional join predicate fragments.

## Implementation

- `src/execution/scan/join.rs`
  - Replaced `flatten().collect()` with `Vec::with_capacity(predicates.len())`.
  - Filled the vector with present optional predicates in input order.
  - Optional filtering and final predicate combination behavior remain unchanged.

## Verification

- `cargo test --test sql_join test_inner_join_with_left_filter_and_indexed_right_probe -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_implicit_join_where_equi_predicate_matches_chbenchmark_q16_shape -- --nocapture`
  - Passed.
- `cargo test --test sql_join`
  - Passed: 31/31.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-191` is complete. Optional join predicate merging now uses an explicitly preallocated vector.
