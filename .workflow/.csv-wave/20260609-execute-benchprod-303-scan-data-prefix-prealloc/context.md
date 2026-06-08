# BENCHPROD-303 Single-Table Scan Data Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when single-table scan paths build `data:<table>:` scan prefixes from known table-name length.

## Implementation

- `src/execution/scan/mod.rs`
  - Added `scan_data_prefix_for_table()` with explicit capacity for `data:<table>:`.
  - Replaced `format!` prefix construction in materialized query scans.
  - Replaced `format!` prefix construction in statistics-driven primary-key range bounds.
  - Replaced `format!` prefix construction in key-only stream projection prefix stripping.
  - Replaced `format!` prefix construction in full table scans.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, transaction scan prefixes, range bounds, key-only primary-key projection, row-cache reuse, row decoding, vector fetches, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test scan_data_ -- --nocapture`
  - Initial compile failed on missing `&str` borrows.
  - Passed after borrow fix: 4/4.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_returning_upsert_vector_rbac -- --nocapture`
  - Passed: 14/14.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

Focused suite tests were launched in parallel and printed Cargo file-lock wait messages before passing.

## Result

`BENCHPROD-303` is complete. Single-table scan data-prefix construction now reserves capacity from known table-name length.
