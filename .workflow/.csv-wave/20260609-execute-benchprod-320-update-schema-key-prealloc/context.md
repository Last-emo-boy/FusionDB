# BENCHPROD-320 UPDATE Schema Key Preallocation

## Goal

Avoid generic `format!` allocation work when UPDATE builds `schema:<table>` lookup keys from known table-name length.

## Implementation

- `src/execution/dml/update.rs`
  - Added `update_schema_key_for_table()` with explicit capacity for `schema:<table>`.
  - Replaced `format!` schema lookup key construction in UPDATE.
  - Added unit coverage for helper output and capacity.
  - Generated schema key bytes, schema loads, update planning, index maintenance, row-cache invalidation, and SQL DML results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test update_schema_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml test_update_primary_key_simple_table_fast_path -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml -- --nocapture`
  - Passed: 44/44.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

Full `sql_dml` passed while printing existing sstable retry warnings.

## Result

`BENCHPROD-320` is complete. UPDATE schema-key construction now reserves capacity from known table-name length.
