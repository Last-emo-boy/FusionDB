# BENCHPROD-190 Join Schema Members Preallocation

## Goal

Avoid implicit growth while collecting and sorting schema membership indices for join predicates.

## Implementation

- `src/execution/scan/join.rs`
  - Initialized the schema-member `HashSet` with `HashSet::with_capacity(schemas.len())`.
  - Replaced collect-based vector creation with `Vec::with_capacity(members.len())` and `extend`.
  - Unmatched-column handling, member deduplication, and sorted output behavior remain unchanged.

## Verification

- `cargo test --test sql_join test_implicit_join_where_equi_predicate_matches_chbenchmark_q16_shape -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_implicit_join_common_or_equi_predicate_matches_chbenchmark_q19_shape -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_three_table_join_with_alias_projection -- --nocapture`
  - Passed.
- `cargo test --test sql_join`
  - Passed: 31/31.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-190` is complete. Join predicate schema membership now uses preallocated set and vector containers.
