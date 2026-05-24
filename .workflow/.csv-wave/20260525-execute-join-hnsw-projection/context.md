# Execute Context

Implemented TASK-019 and TASK-020.

Changes:
- `src/execution/scan.rs`: added `scan_join_base` for first join relation projection-aware decoding.
- `src/execution/scan.rs`: HNSW fetch path now uses `decode_row_for_projection` and avoids caching partial rows.
- `tests/sql_integration.rs`: added HNSW projection test for `ORDER BY VECTOR_DISTANCE(...) LIMIT`.

Constraint honored: no `dashboard/` changes.
