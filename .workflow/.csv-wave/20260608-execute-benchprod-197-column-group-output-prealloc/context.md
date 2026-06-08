# BENCHPROD-197 Grouped Aggregate Output Preallocation

## Goal

Avoid implicit vector growth while grouped column-scan count and aggregate fast paths materialize result rows.

## Implementation

- `src/execution/query/column_scan.rs`
  - Replaced `collect()` in `group_by_count_column_scan` with a result vector preallocated from `counts.len()`.
  - Replaced `collect()` in multi-column `group_by_column_aggregate_scan` with a result vector preallocated from `groups.len()`.
  - Replaced `collect()` in `group_by_single_column_aggregate_scan` with a result vector preallocated from `groups.len()`.
  - Preserved row contents, group value ordering semantics, aggregate finalization, and output schema behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_count_with_simple_where_uses_column_scan -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_column_aggregates_fast_path_uses_only_group_and_aggregate_columns -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_multi_column_group_by_aggregates_fast_path_order_by_limit -- --nocapture`
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

`BENCHPROD-197` is complete. Grouped column-scan aggregate output paths now preallocate result row vectors from known group counts.
