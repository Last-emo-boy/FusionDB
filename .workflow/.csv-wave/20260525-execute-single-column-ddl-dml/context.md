# Single-column DDL/DML decode execution

Executed TASK-017 and TASK-018.

Changes:
- `src/execution/dml.rs`: UNIQUE duplicate checks now decode only the target unique column.
- `src/execution/ddl.rs`: `CREATE INDEX` backfill now decodes only the indexed column.
- Existing BTree/FTS/HNSW index-entry creation branches remain unchanged after the decoded value is obtained.

Dashboard files were not modified.
