# BENCHPROD-240 GROUP BY Result Schema Preallocation

## Goal

Avoid implicit vector growth while building temporary schema column metadata for projected `GROUP BY` results.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced projected `GROUP BY` result schema column `collect()` with `Vec::with_capacity(columns.len())` and explicit pushes.
  - Preserved projected column order, column names, temporary `UNKNOWN` type metadata, nullable/index flags, grouped result rows, and downstream `ORDER BY` resolution.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_projection_alias_and_ordinal_expressions -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_column_aggregates_fast_path_order_by_limit -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-240` is complete. Projected `GROUP BY` temporary schema construction now preallocates column metadata from the known output width.
