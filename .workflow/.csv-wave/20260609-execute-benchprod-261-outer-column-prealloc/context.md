# BENCHPROD-261 Outer Column Extraction Preallocation

## Goal

Avoid initial `HashSet` growth while extracting outer column references from deferred correlated subqueries.

## Implementation

- `src/execution/query/mod.rs`
  - Added `select_relation_name_capacity`.
  - Added `select_outer_reference_capacity`.
  - `extract_select_outer_columns` now preallocates the local relation set from `FROM` and `JOIN` counts.
  - `extract_select_outer_columns` now preallocates the referenced-column set from selection, having, and projection expression counts.
  - Outer-column filtering behavior is unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_correlated_not_exists_with_join_alias_shape -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_correlated_exists_two_table_membership_matches_ldbc_q6_shape -- --nocapture`
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

`BENCHPROD-261` is complete. Deferred subquery outer-column extraction now preallocates temporary sets from the SELECT AST shape.
