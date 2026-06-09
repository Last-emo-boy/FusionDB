# BENCHPROD-333 Table Stats Key Preallocation

## Goal

Avoid generic `format!` allocation work when ANALYZE statistics paths build `stats:table:<table>` keys from known table-name length.

## Implementation

- `src/execution/analyze.rs`
  - Added `table_stats_key_for_table()` with explicit capacity for `stats:table:<table>`.
  - Replaced key construction in `load_table_stats()`.
  - Replaced key construction in `store_table_stats()`.
  - Added unit coverage for helper output and capacity.
  - Generated stats key bytes, ANALYZE statistics storage, EXPLAIN statistics consumption, and DDL behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test table_stats_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl test_analyze_table_collects_statistics -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl test_explain_includes_analyze_statistics -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl -- --nocapture`
  - Passed: 33/33.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

`git diff --check` printed the existing CRLF normalization warning for `src/execution/analyze.rs` while exiting successfully.

## Result

`BENCHPROD-333` is complete. ANALYZE stats-key construction now reserves capacity from known table-name length.
