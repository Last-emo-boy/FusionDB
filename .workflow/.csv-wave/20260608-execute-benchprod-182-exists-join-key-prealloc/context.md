# BENCHPROD-182 EXISTS Join Matching Key Preallocation

## Goal

Avoid repeated growth of the temporary matching-key set used while evaluating two-table correlated EXISTS membership plans.

## Implementation

- `src/execution/expr/subquery.rs`
  - Replaced empty `HashSet` construction for `matching_left_keys` and `matching_right_keys` with side-aware `HashSet::with_capacity`.
  - The side selected by `filter_side` is sized from its scanned row count.
  - The unused side keeps capacity zero.
  - Filter evaluation, join-key insertion, probe-side filtering, and EXISTS cache insertion remain unchanged.

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

`BENCHPROD-182` is complete. EXISTS join membership now preallocates the matching-key set for the side that will be populated.
