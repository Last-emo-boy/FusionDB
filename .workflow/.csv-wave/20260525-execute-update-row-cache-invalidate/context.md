# Execute Context

Implemented TASK-029.

Changes:
- `src/execution/dml.rs`: invalidates `row_cache` after normal UPDATE writes and upsert conflict updates.
- `tests/sql_integration.rs`: added regressions proving indexed full-row lookups return updated values after UPDATE and upsert writes.

Constraint honored: no `dashboard/` changes.
