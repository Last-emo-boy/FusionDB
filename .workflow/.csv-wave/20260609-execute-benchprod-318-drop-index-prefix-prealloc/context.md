# BENCHPROD-318 DROP INDEX Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when DROP INDEX single-column cleanup builds `index:<table>:<column>:` prefixes from known table and column name lengths.

## Implementation

- `src/execution/ddl/index.rs`
  - Added `drop_index_prefix_for_column()` with explicit capacity for `index:<table>:<column>:`.
  - Replaced `format!` prefix construction in DROP INDEX single-column index-entry cleanup.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, `scan_prefix` inputs, deleted index entries, schema index flags, and SQL index behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test drop_index_prefix -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache test_drop_index -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-318` is complete. DROP INDEX single-column prefix construction now reserves capacity from known table and column name lengths.
