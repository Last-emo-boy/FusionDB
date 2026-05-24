# Execute Context

Implemented TASK-023.

Changes:
- `src/execution/dml.rs`: added a primary-key point-delete fast path when `RETURNING` is absent and the table has no non-primary indexes.
- `tests/sql_integration.rs`: added regression coverage with a corrupt non-primary column payload, proving the fast path does not decode the row.

Constraint honored: no `dashboard/` changes.
