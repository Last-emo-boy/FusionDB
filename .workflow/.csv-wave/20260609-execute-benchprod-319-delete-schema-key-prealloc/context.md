# BENCHPROD-319 DELETE Schema Key Preallocation

## Goal

Avoid generic `format!` allocation work when DELETE builds `schema:<table>` lookup keys from known table-name length.

## Implementation

- `src/execution/dml/delete.rs`
  - Added `delete_schema_key_for_table()` with explicit capacity for `schema:<table>`.
  - Replaced `format!` schema lookup key construction in DELETE.
  - Added unit coverage for helper output and capacity.
  - Generated schema key bytes, schema loads, delete planning, secondary index cleanup, row-cache invalidation, and SQL DML results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test delete_schema_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml test_delete_primary_key_updates_secondary_index -- --nocapture`
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

`BENCHPROD-319` is complete. DELETE schema-key construction now reserves capacity from known table-name length.
