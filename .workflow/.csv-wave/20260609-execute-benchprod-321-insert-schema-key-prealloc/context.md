# BENCHPROD-321 INSERT Schema Key Preallocation

## Goal

Avoid generic `format!` allocation work when INSERT builds `schema:<table>` lookup keys from known table-name length.

## Implementation

- `src/execution/dml/insert.rs`
  - Added `insert_schema_key_for_table()` with explicit capacity for `schema:<table>`.
  - Replaced schema lookup key construction in `build_insert_rows_context()`.
  - Replaced schema lookup key construction in regular INSERT handling.
  - Added unit coverage for helper output and capacity.
  - Generated schema key bytes, schema loads, insert planning, COPY direct-path context, constraints, indexes, and SQL DML results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test insert_schema_key -- --nocapture`
  - Passed: 1/1 after borrowing `table_name_str`.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml test_insert_single_row -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml test_copy_from_csv_enforces_constraints_on_direct_path -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml -- --nocapture`
  - Passed: 44/44.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed after `cargo fmt`.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

Focused INSERT tests run in parallel printed Cargo file-lock waits before passing. Full `sql_dml` passed while printing existing sstable retry and slow-query logs.

## Result

`BENCHPROD-321` is complete. INSERT schema-key construction now reserves capacity from known table-name length.
