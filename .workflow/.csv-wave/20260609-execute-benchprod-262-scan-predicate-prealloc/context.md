# BENCHPROD-262 Scan Predicate Column Set Preallocation

## Goal

Avoid initial `HashSet` growth while checking whether extracted scan predicates only reference a known relation set or table schema.

## Implementation

- `src/execution/scan/predicate.rs`
  - `predicate_uses_only_relations` now initializes the temporary extracted-column set with `relation_names.len()`.
  - `predicate_uses_only_schema` now initializes the temporary extracted-column set with `schema.columns.len()`.
  - Predicate extraction, empty-column handling, and relation/schema membership checks are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_inner_join_with_left_filter_and_indexed_right_probe -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_implicit_join_where_equi_predicate_matches_chbenchmark_q16_shape -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_join_projection_pushdown_with_group_by -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join -- --nocapture`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-262` is complete. Scan predicate relation/schema checks now preallocate temporary extracted-column sets from known input bounds.
