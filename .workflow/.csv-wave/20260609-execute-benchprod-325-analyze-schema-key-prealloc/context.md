# BENCHPROD-325 Analyze Schema Key Preallocation

## Goal

Avoid generic `format!` allocation work when ANALYZE builds `schema:<table>` lookup keys from known table-name length.

## Implementation

- `src/execution/analyze.rs`
  - Added `analyze_schema_key_for_table()` with explicit capacity for `schema:<table>`.
  - Replaced schema lookup key construction in `load_analyze_schema()`.
  - Added unit coverage for helper output and capacity.
  - Generated schema key bytes, ANALYZE statistics collection, EXPLAIN statistics consumption, and DDL behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test analyze_schema_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl test_analyze_table_collects_statistics -- --nocapture`
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

`BENCHPROD-325` is complete. ANALYZE schema-key construction now reserves capacity from known table-name length.
