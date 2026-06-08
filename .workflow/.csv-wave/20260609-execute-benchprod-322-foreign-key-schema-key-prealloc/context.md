# BENCHPROD-322 Foreign-Key Schema Key Preallocation

## Goal

Avoid generic `format!` allocation work when foreign-key validation builds `schema:<table>` lookup keys from known table-name length.

## Implementation

- `src/execution/foreign_key.rs`
  - Added `foreign_key_schema_key_for_table()` with explicit capacity for `schema:<table>`.
  - Replaced `format!` key construction in `load_table_schema()`.
  - Added unit coverage for helper output and capacity.
  - Generated schema key bytes, referenced-table schema loads, child and parent FK checks, and SQL constraint behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test foreign_key_schema_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_view_show_constraints test_foreign_key_insert_update_and_parent_delete_checks -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_view_show_constraints test_composite_foreign_key_insert_update_and_parent_checks -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_view_show_constraints -- --nocapture`
  - Passed: 16/16.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

Cargo printed global cache last-use warnings about database or disk being full; commands exited successfully and tests passed.

## Result

`BENCHPROD-322` is complete. Foreign-key schema-key construction now reserves capacity from known table-name length.
