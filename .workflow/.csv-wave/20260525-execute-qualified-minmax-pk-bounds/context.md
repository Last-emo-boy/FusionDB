# Execute Context

Implemented TASK-027.

Changes:
- `src/execution/query.rs`: added a primary-key aggregate argument helper with table/alias qualifier checks and reused it for `COUNT`, `MIN`, and `MAX`.
- `tests/sql_integration.rs`: added regression coverage for `MIN(table.id)`, `MAX(table.id)`, and alias-qualified primary-key extrema with corrupt encoded row values.

Constraint honored: no `dashboard/` changes.
