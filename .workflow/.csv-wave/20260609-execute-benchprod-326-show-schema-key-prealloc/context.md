# BENCHPROD-326 Show Schema Key Preallocation

## Goal

Avoid generic `format!` allocation work when DESCRIBE and SHOW CREATE TABLE build `schema:<table>` lookup keys from known table-name length.

## Implementation

- `src/execution/ddl/show.rs`
  - Added `show_schema_key_for_table()` with explicit capacity for `schema:<table>`.
  - Replaced schema lookup key construction in `handle_describe_table()`.
  - Replaced schema lookup key construction in `handle_show_create_table()`.
  - Added unit coverage for helper output and capacity.
  - Generated schema key bytes, DESCRIBE output, SHOW CREATE TABLE output, and broader DDL behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test show_schema_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl test_describe_table -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl test_show_create_table -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl -- --nocapture`
  - Passed: 33/33.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

`git diff --check` printed the existing CRLF normalization warning for `src/execution/ddl/show.rs` while exiting successfully.

## Result

`BENCHPROD-326` is complete. SHOW schema-key construction now reserves capacity from known table-name length.
