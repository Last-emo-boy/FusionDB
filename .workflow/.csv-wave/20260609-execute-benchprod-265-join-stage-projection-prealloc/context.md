# BENCHPROD-265 Join Stage Projection Predicate Preallocation

## Goal

Avoid initial `HashSet` growth while join stage projection collects columns from pending predicates.

## Implementation

- `src/execution/scan/join.rs`
  - `build_stage_join_projection` now initializes each pending-predicate extracted-column set with `schema.columns.len()`.
  - Required index collection, projection membership checks, and projection pushdown behavior are unchanged.

## Verification

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

`BENCHPROD-265` is complete. Join stage projection now preallocates pending-predicate extracted-column sets from active schema width.
