# BENCHPROD-302 Column-Scan Data Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when column-scan fast paths build `data:<table>:` scan prefixes from known table-name length.

## Implementation

- `src/execution/query/column_scan.rs`
  - Added `column_scan_data_prefix_for_table()` with explicit capacity for `data:<table>:`.
  - Replaced `format!` prefix construction in bare aggregate scans.
  - Replaced `format!` prefix construction in `COUNT DISTINCT` scans.
  - Replaced `format!` prefix construction in `SELECT DISTINCT` scans.
  - Replaced `format!` prefix construction in `GROUP BY COUNT` and grouped aggregate scans.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, transaction scan prefixes, predicate filtering, aggregate state updates, distinct tracking, grouping, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test execution::query::column_scan::tests -- --nocapture`
  - Passed: 4/4.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_select test_select_distinct_with_simple_where_uses_column_scan -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_select test_select_distinct_order_limit_uses_column_scan -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_group_aggregate -- --nocapture`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

Focused `sql_select` tests were launched in parallel and printed Cargo file-lock wait messages before passing.

## Result

`BENCHPROD-302` is complete. Column-scan data-prefix construction now reserves capacity from known table-name length.
