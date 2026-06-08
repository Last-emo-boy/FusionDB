# BENCHPROD-305 Column-Scan Index Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when column-scan aggregate index probes build `index:<table>:<column>:<value>:` prefixes from known component lengths.

## Implementation

- `src/execution/query/column_scan.rs`
  - Added `column_scan_index_prefix_for_value()` with explicit capacity for `index:<table>:<column>:<value>:`.
  - Replaced `format!` prefix construction in column-scan aggregate indexed predicate probes.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, index scans, row-id extraction, row fetches, aggregate state updates, predicate filtering, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test execution::query::column_scan::tests -- --nocapture`
  - Passed: 5/5.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_group_aggregate test_filtered_count_uses_index_candidates_and_required_columns_only -- --nocapture`
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

`BENCHPROD-305` is complete. Column-scan aggregate index-probe prefix construction now reserves capacity from known component lengths.
