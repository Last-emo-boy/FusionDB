# Plan Context

Goal: continue database core performance iteration without touching `dashboard/`.

Findings:
- `FusionStorage::rebuild_vector_index` scanned all rows for HNSW-indexed tables and decoded each full row.
- The rebuild path only needs HNSW vector columns and the row id from the data key.
- `VectorIndex::batch_insert` already exists and can build an index once per column instead of rebuilding lazily after many single inserts.

Decision:
- Use `RowDecoder::decode_column` for each HNSW column during rebuild.
- Accumulate vectors per HNSW index name and call `batch_insert` once per index.
