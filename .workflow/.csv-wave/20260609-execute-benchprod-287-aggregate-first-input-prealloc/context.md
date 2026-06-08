# BENCHPROD-287 Aggregate First Input Preallocation

## Goal

Avoid the first growth step for collecting aggregate accumulators created by `AggregateAccumulator::new`.

## Implementation

- `src/execution/aggregation.rs`
  - `COUNT_DISTINCT` now creates its distinct set with capacity 1.
  - `ARRAY_AGG` now creates its value vector with capacity 1.
  - `STRING_AGG` and `GROUP_CONCAT` now create their string vectors with capacity 1.
  - Added unit coverage for first-input preallocation through `AggregateAccumulator::new`.
  - Update logic, NULL handling, finalization, and SQL aggregate results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test test_collecting_accumulators_preallocate_first_input -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test execution::aggregation::tests -- --nocapture`
  - Passed: 15/15.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_group_aggregate test_string_agg -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_set_subquery array_agg -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-287` is complete. Collecting aggregate accumulators now reserve space for their first input value when created through `AggregateAccumulator::new`.
