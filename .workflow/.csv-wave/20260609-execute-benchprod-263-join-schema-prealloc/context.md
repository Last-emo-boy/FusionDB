# BENCHPROD-263 Join Schema Predicate Column Set Preallocation

## Goal

Avoid initial `HashSet` growth while join predicate helpers extract column references for schema-membership analysis.

## Implementation

- `src/execution/scan/join.rs`
  - `expr_uses_only_schema` now preallocates extracted columns from the target/other schema pair size.
  - `predicate_schema_members` now preallocates extracted columns from the saturating total column count across participating schemas.
  - `predicate_schema_membership` now preallocates extracted columns from `schema.columns.len()`.
  - `predicate_uses_only_target_schema` now preallocates extracted columns from the target/other schema pair size.
  - Predicate extraction and schema membership behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join -- --nocapture`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-263` is complete. Join predicate schema-membership helpers now preallocate temporary extracted-column sets from known schema bounds.
