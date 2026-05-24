# Execute Context

Implemented TASK-028.

Changes:
- `src/execution/query.rs`: split aggregate argument resolution into column and primary-key helpers, and allowed `COUNT` over non-nullable columns to use the prefix-count fast path.
- `tests/sql_integration.rs`: added regression coverage with corrupt encoded `NOT NULL` column payloads, proving `COUNT(code)` and alias-qualified `COUNT(c.code)` avoid row decoding.

Constraint honored: no `dashboard/` changes.
