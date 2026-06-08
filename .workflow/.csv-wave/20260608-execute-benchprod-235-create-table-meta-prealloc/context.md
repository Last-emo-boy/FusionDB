# BENCHPROD-235 CREATE TABLE Metadata Preallocation

## Goal

Avoid implicit vector growth while building `CREATE TABLE` schema metadata and table-level primary-key metadata.

## Implementation

- `src/execution/ddl/table.rs`
  - Replaced CREATE TABLE column metadata iterator `collect()` with `Vec::with_capacity(columns.len())` and explicit pushes.
  - Replaced table-level primary-key constraint `collect()` with `Vec::with_capacity(constraints.len())` and explicit pushes.
  - Preserved column order, primary-key detection, composite primary-key handling, default/check expression formatting, nullability/unique flags, and constraint error messages.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl test_create_table_table_level_single_primary_key -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl test_create_table_table_level_composite_primary_key -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl`
  - Passed: 29/29.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_view_show_constraints`
  - Passed: 16/16.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-235` is complete. `CREATE TABLE` schema metadata construction now preallocates column and primary-key constraint buffers from known AST slice lengths.
