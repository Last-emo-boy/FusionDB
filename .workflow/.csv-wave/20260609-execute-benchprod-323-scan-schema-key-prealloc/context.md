# BENCHPROD-323 Scan Schema Key Preallocation

## Goal

Avoid generic `format!` allocation work when scan paths build `schema:<table>` lookup keys from known table-name length.

## Implementation

- `src/execution/scan/mod.rs`
  - Added `scan_schema_key_for_table()` with explicit capacity for `schema:<table>`.
  - Replaced schema lookup key construction in `scan_table_base()`.
  - Replaced schema lookup key construction in `scan_table_filtered()`.
  - Added unit coverage for helper output and capacity.
  - Generated schema key bytes, table/view resolution, full scans, index fallbacks, row-cache behavior, and SQL query results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test scan_schema_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache test_full_table_scan_reuses_row_cache -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_select -- --nocapture`
  - Passed: 27/27.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

Focused scan/select tests printed Cargo file-lock waits before passing.

## Result

`BENCHPROD-323` is complete. Scan schema-key construction now reserves capacity from known table-name length.
