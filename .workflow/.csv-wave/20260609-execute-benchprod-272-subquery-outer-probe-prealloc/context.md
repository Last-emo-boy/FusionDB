# BENCHPROD-272 Subquery Outer-Probe Column Set Preallocation

## Goal

Avoid initial `HashSet` growth while classifying correlated `EXISTS` join probe expressions.

## Implementation

- `src/execution/expr/subquery.rs`
  - `exists_join_expr_is_outer_probe` now creates its extracted-column set with `HashSet::with_capacity`.
  - The capacity hint is derived from `left_schema.columns.len().saturating_add(right_schema.columns.len())`.
  - Existing `EXISTS` and `NOT EXISTS` correlated join membership behavior is unchanged.

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

`BENCHPROD-272` is complete. Correlated subquery join probe classification now preallocates the extracted-column set from known local schema width.
