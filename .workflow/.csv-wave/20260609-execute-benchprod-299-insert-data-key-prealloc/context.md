# BENCHPROD-299 DML Insert Data Key Preallocation

## Goal

Avoid generic `format!` allocation work when DML insert paths build `data:<table>:<row_id>` keys from known component lengths.

## Implementation

- `src/execution/dml/insert.rs`
  - Added `insert_data_key_for_row_id()` with explicit capacity for `data:<table>:<row_id>`.
  - Replaced `format!` key construction in single-row insert/upsert.
  - Replaced `format!` key construction in values-list insert/upsert.
  - Replaced `format!` key construction in `INSERT ... SELECT`.
  - Added unit coverage for helper output and capacity.
  - Generated key bytes, duplicate primary-key checks, `ON CONFLICT` behavior, row-cache lookups, row writes, indexes, `RETURNING`, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test insert_data_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml -- --nocapture`
  - Passed: 44/44.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_returning_upsert_vector_rbac -- --nocapture`
  - Passed: 14/14.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

The full `sql_dml` run printed a transient SSTable open retry warning before passing; final test result was green.

## Result

`BENCHPROD-299` is complete. DML insert data-key construction now reserves capacity from known table and row-id lengths.
