# BENCHPROD-238 Aggregate Output Column Preallocation

## Goal

Avoid implicit vector growth while constructing output column names for simple column aggregate fast paths.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced filtered simple aggregate output-name `collect()` with `Vec::with_capacity(plans.len())` and explicit pushes.
  - Replaced bare simple aggregate output-name `collect()` with `Vec::with_capacity(plans.len())` and explicit pushes.
  - Preserved aggregate output order, aliases, predicate handling, fast-path selection, and result row construction.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_filtered_bare_aggregates_stream_required_columns_only -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_bare_aggregate_sum_avg -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-238` is complete. Simple column aggregate fast paths now preallocate output column-name buffers from the known aggregate plan count.
