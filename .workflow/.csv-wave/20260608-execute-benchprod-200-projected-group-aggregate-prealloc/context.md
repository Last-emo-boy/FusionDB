# BENCHPROD-200 Projected GROUP BY Aggregate Preallocation

## Goal

Avoid implicit vector growth while projected GROUP BY aggregate execution initializes per-group accumulators and materializes final result rows.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced per-group `AggregateAccumulator` `collect()` with a vector preallocated from `aggregate_plans.len()`.
  - Replaced final grouped result row `collect()` with a result vector preallocated from `groups.len()`.
  - Preserved accumulator update order, aggregate finalization, and output row shape.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_sum -- --nocapture`
  - Passed: 3/3 filtered tests.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_array_agg_over_generated_subscripts -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery`
  - Passed: 48/48.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-200` is complete. Projected GROUP BY aggregate execution now preallocates accumulator and result row vectors where required capacities are known.
