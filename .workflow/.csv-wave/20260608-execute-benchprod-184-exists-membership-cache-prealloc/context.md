# BENCHPROD-184 EXISTS Membership Cache Preallocation

## Goal

Avoid implicit growth while building the cached value set for single-table correlated EXISTS membership plans.

## Implementation

- `src/execution/expr/subquery.rs`
  - Replaced `filter_map(...).collect()` cache construction with explicit `HashSet::with_capacity(local_rows.len())`.
  - Inserted values in a loop, preserving missing-column skip behavior and duplicate handling.
  - Column lookup, cache key generation, and cache insertion remain unchanged.

## Verification

- `cargo test --test sql_set_subquery test_correlated_not_exists_membership_filter_with_alias -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery test_correlated_not_exists_against_cte -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery test_correlated_not_exists_filters_before_limit -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 48/48.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-184` is complete. Single-table EXISTS membership cache values now use an explicitly preallocated `HashSet`.
