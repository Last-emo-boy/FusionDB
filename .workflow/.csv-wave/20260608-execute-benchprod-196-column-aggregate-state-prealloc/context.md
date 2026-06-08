# BENCHPROD-196 Column Aggregate State Preallocation

## Goal

Avoid implicit vector growth while column aggregate fast paths initialize aggregate states and produce finalized aggregate values.

## Implementation

- `src/execution/query/column_scan.rs`
  - Added `column_aggregate_states` to build `ColumnAggregateState` vectors with capacity from `plans.len()`.
  - Added `finalize_column_aggregate_states` to produce aggregate output values with capacity from `states.len()`.
  - Added `group_column_aggregate_states` for single-column and multi-column group aggregate state initialization.
  - Preserved aggregate state ordering, update logic, and finalize semantics.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_bare_aggregate_sum_avg -- --nocapture`
  - Passed: 2/2 filtered tests.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_bare_sum_avg_with_simple_where_column_scan -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_column_aggregates_fast_path_uses_only_group_and_aggregate_columns -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_aggregates_with_simple_where_uses_column_scan -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

The default C:/TEMP drive reported 0 GB free during verification. Cargo and linker commands were run with `TEMP` and `TMP` redirected to an E: workspace temp directory. Cargo still emitted a global cache last-use warning, but all tests and checks exited successfully.

## Result

`BENCHPROD-196` is complete. Column aggregate fast paths now use explicitly preallocated state and finalized value vectors where the required capacities are known.
