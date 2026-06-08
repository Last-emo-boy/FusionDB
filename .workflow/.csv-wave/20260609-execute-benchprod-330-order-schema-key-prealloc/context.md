# BENCHPROD-330 Order Schema Key Preallocation

## Goal

Avoid generic `format!` allocation work when ORDER BY primary-key LIMIT pushdown builds `schema:<table>` lookup keys from known table-name length.

## Implementation

- `src/execution/query/order.rs`
  - Added `order_schema_key_for_table()` with explicit capacity for `schema:<table>`.
  - Replaced schema lookup key construction in `primary_key_order_scan_limit()`.
  - Added unit coverage for helper output and capacity.
  - Generated schema key bytes, ORDER BY primary-key LIMIT pushdown, range pushdown, index-cache behavior, and general SELECT ORDER BY behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test order_schema_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache test_select_order_by_primary_key_limit_offset -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache test_primary_key_range_order_limit_offset_pushdown -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_select -- --nocapture`
  - Passed: 27/27.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

The first focused helper test rebuilt dependencies and took longer than later test runs.

`git diff --check` printed the existing CRLF normalization warning for `src/execution/query/order.rs` while exiting successfully.

## Result

`BENCHPROD-330` is complete. ORDER schema-key construction now reserves capacity from known table-name length.
