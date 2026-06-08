# BENCHPROD-193 First-Relation Local Predicate Preallocation

## Goal

Avoid implicit growth while building the local predicate vector for first-relation join projection planning.

## Implementation

- `src/execution/scan/join.rs`
  - Replaced chained iterator `collect()` for `local_predicates` with explicit `Vec::with_capacity`.
  - Capacity is computed from `pending_predicates.len()` plus optional `first_selection`.
  - Predicate ordering and projection planning inputs remain unchanged.

## Verification

- `cargo test --test sql_join test_join_left_filter_projection_skips_unused_left_column_decode -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_join_projection_pushdown_with_group_by -- --nocapture`
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

`BENCHPROD-193` is complete. First-relation join projection planning now uses an explicitly preallocated local predicate vector.
