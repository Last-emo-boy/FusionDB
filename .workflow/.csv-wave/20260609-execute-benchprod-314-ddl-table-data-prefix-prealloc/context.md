# BENCHPROD-314 DDL Table Data Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when DDL table operations build `data:<table>:` prefixes from known table-name length.

## Implementation

- `src/execution/ddl/table.rs`
  - Added `table_data_prefix_for_table()` with explicit capacity for `data:<table>:`.
  - Replaced `format!` prefix construction in DROP TABLE data cleanup.
  - Replaced `format!` prefix construction in TRUNCATE TABLE data cleanup.
  - Replaced `format!` prefix construction in ALTER TABLE DROP COLUMN row rewrite.
  - Replaced `format!` prefix construction in ALTER TABLE ADD PRIMARY KEY row rewrite.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, `scan_prefix` inputs, row-cache invalidation keys, index cleanup, row rewrites, and SQL DDL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test table_data_prefix -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl -- --nocapture`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-314` is complete. DDL table-operation data-prefix construction now reserves capacity from known table-name length.
