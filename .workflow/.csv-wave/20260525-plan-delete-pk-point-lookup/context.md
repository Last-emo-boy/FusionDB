# DELETE primary-key point lookup plan

Goal: continue database-core performance iteration by avoiding full table scans for primary-key equality deletes.

Scope:
- Include: `src/execution/dml.rs`, focused SQL integration tests.
- Exclude: `dashboard/`.

Findings:
- `UPDATE ... WHERE id = ...` already used a point lookup when the predicate targeted the first primary-key column.
- `DELETE ... WHERE id = ...` always scanned the full table and decoded every row before evaluating the predicate.
- DELETE still needs full row decode for the matched row to clean secondary indexes and support `RETURNING`.

Plan:
- TASK-015: extract reusable primary-key equality row-id derivation from DML predicates.
- TASK-016: use the helper for DELETE point lookup and verify secondary index cleanup still works.
