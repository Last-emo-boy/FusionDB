# BENCHPROD-307 DML Update/Delete Data Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when DML update and delete paths build `data:<table>:` prefixes from known table-name length.

## Implementation

- `src/execution/dml/update.rs`
  - Added `update_data_prefix_for_table()` with explicit capacity for `data:<table>:`.
  - Replaced `format!` prefix construction in UPDATE point-lookup/full-scan setup.
  - Added unit coverage for helper output and capacity.
- `src/execution/dml/delete.rs`
  - Added `delete_data_prefix_for_table()` with explicit capacity for `data:<table>:`.
  - Replaced `format!` prefix construction in DELETE point-lookup/full-scan setup.
  - Added unit coverage for helper output and capacity.
- Generated prefix bytes, point lookup keys, full-scan prefixes, row-cache invalidation keys, `RETURNING` rows, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test data_prefix_for_table -- --nocapture`
  - Passed: 6/6.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml test_update_primary_key_simple_table_fast_path -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml test_delete_primary_key_without_secondary_index_skips_row_decode -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_returning_upsert_vector_rbac test_update_returning -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_returning_upsert_vector_rbac test_delete_returning -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml -- --nocapture`
  - Passed: 44/44.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

Focused tests were launched in parallel and printed Cargo file-lock wait messages before passing. The full `sql_dml` run printed transient SSTable open retry warnings before passing; final test result was green.

## Result

`BENCHPROD-307` is complete. DML update/delete data-prefix construction now reserves capacity from known table-name length.
