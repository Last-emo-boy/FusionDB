# BENCHPROD-295 Scan Data Key Preallocation

## Goal

Avoid generic `format!` allocation work when single-table scan hot paths build `data:<table>:<row_id>` keys from known component lengths.

## Implementation

- `src/execution/scan/mod.rs`
  - Added `scan_data_key_for_row_id()` with explicit capacity for `data:<table>:<row_id>`.
  - Replaced `format!` key construction in vector-search row fetch.
  - Replaced `format!` key construction in primary-key point lookup.
  - Replaced `format!` key construction in small index result fetch and concurrent stream fetch.
  - Replaced `format!` key construction when inserting rows fetched by the stream path into the row cache.
  - Added unit coverage for helper output and capacity.
  - Generated key bytes, row-cache keys, storage lookups, key-only projection, vector result fetch, index fetch ordering, row decoding, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test scan_data_key -- --nocapture`
  - Initial run failed at compile time because a `String` table name needed borrowing for `&str`.
  - Passed after the local borrow fix: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_returning_upsert_vector_rbac -- --nocapture`
  - Passed: 14/14.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Initial check reported one formatting diff.
  - Passed after `cargo fmt`.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-295` is complete. Single-table scan data-key construction now reserves capacity from known table and row-id lengths.
