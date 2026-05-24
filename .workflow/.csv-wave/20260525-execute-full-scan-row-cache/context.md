# Execute Context

Implemented TASK-030.

Changes:
- `src/execution/scan.rs`: full-row table scans now check and populate `row_cache` when no projection indices are active.
- `src/execution/dml.rs`: normal INSERT writes invalidate an existing cached row for the same data key.
- `tests/sql_integration.rs`: added regressions for repeated full-table scan cache reuse and INSERT overwrite cache invalidation.

Constraint honored: no `dashboard/` changes.
