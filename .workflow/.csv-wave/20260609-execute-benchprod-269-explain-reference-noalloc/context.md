# BENCHPROD-269 EXPLAIN Reference Check Without Allocation

## Goal

Avoid temporary `HashSet` allocation when EXPLAIN only needs to know whether an expression contains any column reference.

## Implementation

- `src/execution/ddl/explain.rs`
  - `explain_expr_has_column_reference` now delegates to the shared `expr_has_column_reference` predicate.
  - This removes the previous extract-columns-into-set path.
  - EXPLAIN access path, primary-key range, composite-index, and index-use behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test predicate_schema_members_for_explain_tracks_local_and_join_predicates -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_create_composite_btree_index_and_lookup -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-269` is complete. EXPLAIN column-reference checks now avoid temporary column-set allocation.
