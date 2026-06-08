# BENCHPROD-315 EXPLAIN Data Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when EXPLAIN join-order fallback row counting builds `data:<table>:` prefixes from known table-name length.

## Implementation

- `src/execution/ddl/explain.rs`
  - Added `explain_data_prefix_for_table()` with explicit capacity for `data:<table>:`.
  - Replaced `format!` prefix construction in EXPLAIN comma-join row-count fallback.
  - Added unit coverage for helper output and capacity.
- `tests/sql_ddl.rs`
  - Added `test_explain_join_order_counts_rows_without_analyze_statistics` to exercise fallback row counting without ANALYZE statistics.
  - Generated prefix bytes, `count_prefix` inputs, fallback row estimates, join-order text, and SQL DDL behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test explain_data_prefix -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl test_explain_join_order_counts_rows_without_analyze_statistics -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl -- --nocapture`
  - Passed: 32/32.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-315` is complete. EXPLAIN fallback data-prefix construction now reserves capacity from known table-name length.
