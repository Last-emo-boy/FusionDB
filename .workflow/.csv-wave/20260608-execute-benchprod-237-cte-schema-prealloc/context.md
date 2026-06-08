# BENCHPROD-237 Materialized CTE Schema Preallocation

## Goal

Avoid implicit vector growth while building temporary schema column metadata for materialized CTEs.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced materialized CTE column metadata iterator `collect()` with `Vec::with_capacity(columns.len())` and explicit pushes.
  - Preserved CTE output column order, alias column names, inferred temporary column types, nullable/index flags, schema serialization, and temporary row writes.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_cte_basic -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_recursive_cte_alias_can_rename_prefix_columns -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery`
  - Passed: 48/48.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-237` is complete. Materialized CTE temporary schema construction now preallocates column metadata from the known CTE output width.
