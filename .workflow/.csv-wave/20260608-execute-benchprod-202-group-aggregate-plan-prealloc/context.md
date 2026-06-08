# BENCHPROD-202 GROUP BY Aggregate Plan Preallocation

## Goal

Avoid implicit vector growth while compiling GROUP BY aggregate execution plans.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced iterator `collect()` in `compile_group_aggregate_plans` with a vector preallocated from `aggregates.len()`.
  - Preserved aggregate expression cloning, function names, and row value source selection.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_column_aggregates_fast_path_preserves_alias_and_nulls -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_bare_aggregate_sum_multiply_expr -- --nocapture`
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

`BENCHPROD-202` is complete. GROUP BY aggregate planning now uses explicit capacity for the plan vector where the aggregate count is known.
