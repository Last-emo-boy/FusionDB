# BENCHPROD-316 DDL Table Index Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when DDL table operations build `index:<table>:` and `index:<table>:<column>:` prefixes from known name lengths.

## Implementation

- `src/execution/ddl/table.rs`
  - Added `table_index_prefix_for_table()` with explicit capacity for `index:<table>:`.
  - Added `table_index_prefix_for_column()` with explicit capacity for `index:<table>:<column>:`.
  - Replaced `format!` prefix construction in DROP TABLE index cleanup.
  - Replaced `format!` prefix construction in TRUNCATE TABLE index cleanup.
  - Replaced `format!` prefix construction in ALTER TABLE ADD PRIMARY KEY secondary BTree index row-id rewrite.
  - Added unit coverage for helper outputs and capacities.
- `tests/sql_ddl.rs`
  - Added coverage for ADD PRIMARY KEY after a secondary BTree index already exists.
  - Generated prefix bytes, `scan_prefix` inputs, deleted index keys, rewritten secondary index row IDs, and SQL DDL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test table_index_prefix -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl test_alter_table_add_primary_key_rewrites_secondary_btree_index_row_ids -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl -- --nocapture`
  - Passed: 33/33.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-316` is complete. DDL table index-prefix construction now reserves capacity from known table and column name lengths.
