# BENCHPROD-259 Permission Query Table Preallocation

## Goal

Avoid initial vector growth while statement permission resolution collects source tables from query ASTs.

## Implementation

- `src/execution/mod.rs`
  - Added `query_table_capacity` and `table_factor_table_capacity`.
  - `Statement::Query` now preallocates the source table list.
  - `CREATE VIEW` now preallocates its source table list before adding view permissions.
  - `COPY (query)` now preallocates its source table list.
  - Added a derived-table assertion to the existing `statement_permissions` unit test.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test statement_permissions_preserve_preallocated_entries -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac test_rbac_permission_check -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-259` is complete. Query-source table collection during permission resolution now preallocates from the AST shape while preserving table order and permission semantics.
