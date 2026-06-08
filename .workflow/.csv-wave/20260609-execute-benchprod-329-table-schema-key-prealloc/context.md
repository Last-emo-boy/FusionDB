# BENCHPROD-329 Table Schema Key Preallocation

## Goal

Avoid generic `format!` allocation work when TABLE DDL paths build `schema:<table>` lookup keys from known table-name length.

## Implementation

- `src/execution/ddl/table.rs`
  - Added `table_schema_key_for_table()` with explicit capacity for `schema:<table>`.
  - Replaced schema key construction in CREATE TABLE existence checks and schema storage.
  - Replaced foreign-key parent schema validation key construction.
  - Replaced DROP TABLE and TRUNCATE TABLE schema lookup key construction.
  - Replaced ALTER TABLE schema lookup key construction.
  - Added unit coverage for helper output and capacity.
  - Generated schema key bytes, table creation, foreign-key validation, drop/truncate cleanup, row-cache invalidation, ALTER TABLE behavior, and DDL outputs are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test table_schema_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl test_create_table -- --nocapture`
  - Passed: 6/6.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl test_drop_table -- --nocapture`
  - Passed: 3/3.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl test_truncate_table -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl test_alter_table -- --nocapture`
  - Passed: 10/10.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl -- --nocapture`
  - Passed: 33/33.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

An attempted multi-filter `cargo test` command failed before running tests because `cargo test` accepts a single `TESTNAME` filter; focused filters were then run independently.

`git diff --check` printed the existing CRLF normalization warning for `src/execution/ddl/table.rs` while exiting successfully.

## Result

`BENCHPROD-329` is complete. TABLE DDL schema-key construction now reserves capacity from known table-name length.
