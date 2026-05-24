# Execute Context

Implemented TASK-021 and TASK-022.

Changes:
- `src/storage/fusion.rs`: HNSW rebuild now extracts row id from the data key and decodes only HNSW vector columns.
- `src/storage/fusion.rs`: rebuild accumulates vectors per index and uses `batch_insert`.
- `src/storage/fusion.rs`: added regression test proving rebuild does not depend on decoding a corrupted non-HNSW column.

Constraint honored: no `dashboard/` changes.
