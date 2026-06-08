# BENCHPROD-327 Index Schema Key Preallocation

## Goal

Avoid generic `format!` allocation work when CREATE INDEX and DROP INDEX build `schema:<table>` lookup keys from known table-name length.

## Implementation

- `src/execution/ddl/index.rs`
  - Added `index_schema_key_for_table()` with explicit capacity for `schema:<table>`.
  - Replaced schema lookup key construction in `handle_create_index()`.
  - Replaced single-column schema update key construction in `handle_drop_index()`.
  - Added unit coverage for helper output and capacity.
  - Generated schema key bytes, index creation, index backfill, DROP INDEX schema updates, and index-cache behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test index_schema_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache test_create_btree_index -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache test_drop_index -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

`cargo fmt` was run after `cargo fmt --check` requested import wrapping in the test module.

`git diff --check` printed the existing CRLF normalization warning for `src/execution/ddl/index.rs` while exiting successfully.

## Result

`BENCHPROD-327` is complete. INDEX schema-key construction now reserves capacity from known table-name length.
