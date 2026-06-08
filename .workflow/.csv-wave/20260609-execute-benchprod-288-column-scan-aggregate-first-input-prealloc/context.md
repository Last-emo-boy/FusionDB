# BENCHPROD-288 Column-Scan Aggregate First Input Preallocation

## Goal

Avoid the first growth step for collecting aggregate states in the column-scan fast paths.

## Implementation

- `src/execution/query/column_scan.rs`
  - Bare column-scan `StringAgg` states now create their string vector with capacity 1.
  - Grouped column-scan `CountDistinct` states now create their distinct set with capacity 1.
  - Grouped column-scan `StringAgg` states now create their string vector with capacity 1.
  - Added unit coverage for first-input preallocation in both aggregate state types.
  - Decoding, NULL handling, grouping, finalization, and SQL result behavior are unchanged.

## Verification

- Initial attempt at `cargo test column_aggregate_state_preallocates_string_agg_first_value group_column_aggregate_state_preallocates_collecting_first_value -- --nocapture`
  - Failed before test execution because `cargo test` accepts a single test filter.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test execution::query::column_scan::tests -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_group_aggregate test_bare_string_agg_column_scan_uses_only_aggregate_columns -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_group_aggregate test_string_agg_group_by_fast_path_uses_only_group_and_value_columns -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_group_aggregate -- --nocapture`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-288` is complete. Column-scan aggregate states now reserve space for the first collected distinct or string value.
