# BENCHPROD-306 DML Insert Data Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when DML insert paths build `data:<table>:` scan prefixes from known table-name length.

## Implementation

- `src/execution/dml/insert.rs`
  - Added `insert_data_prefix_for_table()` with explicit capacity for `data:<table>:`.
  - Replaced `format!` prefix construction in SERIAL default scans.
  - Replaced `format!` prefix construction in single-row insert/upsert non-primary UNIQUE checks.
  - Replaced `format!` prefix construction in values-list insert/upsert non-primary UNIQUE checks.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, transaction scan prefixes, row-cache lookups, duplicate checks, SERIAL values, insert/upsert behavior, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test insert_data_ -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml test_insert_omitted_serial_primary_key_generates_ids -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml test_copy_from_csv_with_header_and_index_lookup -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_returning_upsert_vector_rbac -- --nocapture`
  - Passed: 14/14.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml -- --nocapture`
  - Passed: 44/44.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

Focused DML tests were launched in parallel and printed Cargo file-lock wait messages before passing. The full `sql_dml` run printed transient SSTable open retry warnings before passing; final test result was green.

## Result

`BENCHPROD-306` is complete. DML insert data-prefix construction now reserves capacity from known table-name length.
