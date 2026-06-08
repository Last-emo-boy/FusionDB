# BENCHPROD-255 Subquery Scope Preallocation

## Goal

Avoid initial growth in correlated subquery local-scope buffers by using the known number of `FROM` and `JOIN` relations from the query AST.

## Implementation

- `src/execution/expr/subquery.rs`
  - Added `relation_capacity` in `subquery_local_scope`.
  - The capacity is computed from each `TableWithJoins` relation plus its joined relations.
  - `relation_names` now uses `HashSet::with_capacity(relation_capacity)`.
  - `columns` now uses `Vec::with_capacity(relation_capacity)`.
  - Relation collection, schema loading, prefixed column handling, and duplicate-name filtering are unchanged.

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

`BENCHPROD-255` is complete. Correlated subquery local-scope construction now preallocates relation and column buffers from the AST relation count.
