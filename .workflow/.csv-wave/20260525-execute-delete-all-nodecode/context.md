# Execute Context

Implemented TASK-024.

Changes:
- `src/execution/dml.rs`: added an unconditional delete fast path when `RETURNING` is absent and the table has no non-primary indexes.
- `tests/sql_integration.rs`: added regression coverage with corrupt non-primary column payloads, proving the fast path does not decode deleted rows.

Constraint honored: no `dashboard/` changes.
