# BENCHPROD-301 DDL Table Data Key Preallocation

## Goal

Avoid generic `format!` allocation work when `ALTER TABLE` primary-key row rewrites build `data:<table>:<row_id>` keys from known component lengths.

## Implementation

- `src/execution/ddl/table.rs`
  - Added `table_data_key_for_row_id()` with explicit capacity for `data:<table>:<row_id>`.
  - Replaced `format!` key construction in `ALTER TABLE ADD PRIMARY KEY` row rewrite new-key construction.
  - Added unit coverage for helper output and capacity.
  - Generated key bytes, duplicate storage-key checks, old-row deletion, new-row writes, row-cache invalidation, index rewrites, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test table_data_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl test_alter_table_only_add_primary_key_pgbench_shape -- --nocapture`
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

`BENCHPROD-301` is complete. DDL table rewrite data-key construction now reserves capacity from known table and row-id lengths.
