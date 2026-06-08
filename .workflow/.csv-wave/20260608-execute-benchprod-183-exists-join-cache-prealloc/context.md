# BENCHPROD-183 EXISTS Join Cache Value Preallocation

## Goal

Avoid implicit growth while building the cached probe-value set for two-table correlated EXISTS membership plans.

## Implementation

- `src/execution/expr/subquery.rs`
  - Replaced `filter_map(...).collect()` cache construction with explicit `HashSet::with_capacity`.
  - Sized the cache from the scanned row count of the side selected by `probe_side`.
  - Preserved join-key membership checks, probe value extraction, duplicate handling, and cache insertion behavior.

## Verification

- `cargo test --test sql_set_subquery test_correlated_exists_two_table_membership_matches_ldbc_q6_shape -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery test_correlated_not_exists_membership_filter_with_alias -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_comma_join_reorder_preserves_ldbc_q4_shape_with_deferred_exists -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 48/48.
- `cargo test --test sql_join`
  - Passed: 31/31.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-183` is complete. EXISTS join membership cache values now use an explicitly preallocated `HashSet`.
