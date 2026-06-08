# BENCHPROD-298 Column-Scan Data Key Preallocation

## Goal

Avoid generic `format!` allocation work when column-scan aggregate index probes build `data:<table>:<row_id>` keys from known component lengths.

## Implementation

- `src/execution/query/column_scan.rs`
  - Added `column_scan_data_key_for_row_id()` with explicit capacity for `data:<table>:<row_id>`.
  - Replaced `format!` key construction in primary-key aggregate index-probe row fetches.
  - Replaced `format!` key construction in indexed-column aggregate index-probe row fetches.
  - Added unit coverage for helper output and capacity.
  - Generated key bytes, transaction get lookups, aggregate state updates, predicate filtering, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test execution::query::column_scan::tests -- --nocapture`
  - Passed: 3/3.
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

`BENCHPROD-298` is complete. Column-scan aggregate index-probe data-key construction now reserves capacity from known table and row-id lengths.
