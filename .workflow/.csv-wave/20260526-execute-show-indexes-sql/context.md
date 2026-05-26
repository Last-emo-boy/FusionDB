# TASK-150 Execution Context

## Scope

- `src/execution/mod.rs`
- `src/execution/ddl.rs`
- `tests/sql_integration.rs`
- Database core only; `dashboard/` and `ui/` untouched.

## Change

- Added custom SQL routing for `SHOW INDEXES` and `SHOW INDEXES FROM <table>`.
- Added a read-only handler that scans `index_meta:<index_name>` records and returns `Index`, `Table`, `Column`.
- Added integration coverage for listing all indexes, table filtering, and `DROP INDEX` metadata removal.

## Expected Impact

- Makes index lifecycle observable through SQL.
- Uses metadata-only KV scans, so it does not scan table rows.
