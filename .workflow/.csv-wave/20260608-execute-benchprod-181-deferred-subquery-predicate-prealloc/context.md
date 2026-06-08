# BENCHPROD-181 Deferred Subquery Predicate Preallocation

## Goal

Avoid growth reallocations while splitting conjunctive WHERE predicates into eager and deferred subquery buckets.

## Implementation

- `src/execution/expr/subquery.rs`
  - Captured `predicate_count` from `collect_conjunctive_predicates`.
  - Initialized both `eager` and `deferred` vectors with `Vec::with_capacity(predicate_count)`.
  - Deferred EXISTS detection, predicate ordering, and predicate recombination remain unchanged.

## Verification

- `cargo test --test sql_join test_comma_join_reorder_preserves_ldbc_q4_shape_with_deferred_exists -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery test_correlated_not_exists_membership_filter_with_alias -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery test_correlated_exists_two_table_membership_matches_ldbc_q6_shape -- --nocapture`
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

`BENCHPROD-181` is complete. Deferred subquery predicate splitting now sizes both output buckets from the known predicate count.
