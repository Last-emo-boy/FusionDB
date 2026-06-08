# BENCHPROD-267 Deferred Subquery Cache Preallocation

## Goal

Avoid initial `HashMap` growth while deferred `EXISTS` filtering builds per-expression membership caches across rows.

## Implementation

- `src/execution/expr/subquery.rs`
  - Added `deferred_subquery_cache_capacity`.
  - The helper counts `EXISTS` nodes along the same recursive expression shapes used by deferred subquery evaluation.
  - `filter_rows_with_subqueries` now initializes `membership_caches` with that capacity hint.
  - Deferred subquery evaluation and cache key behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_correlated_exists_two_table_membership_matches_ldbc_q6_shape -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_correlated_not_exists_with_join_alias_shape -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery -- --nocapture`
  - Passed: 48/48.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-267` is complete. Deferred subquery filtering now preallocates membership cache maps from the predicate AST shape.
