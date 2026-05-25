# TASK-084 Execution Context

## Scope

- `src/storage/fusion.rs`
- Database core only; `dashboard/` untouched.

## Change

- `FusionStorage::update_columnar_store` now updates HNSW first from borrowed `ids` / `vectors`.
- The owned `ids` and `vectors` are then moved into `ColumnarVectorStore::new`.
- This removes the previous full-batch clone for the legacy columnar store path.

## Expected Impact

- Lower allocation and clone cost during vector batch refreshes.
- HNSW update behavior remains unchanged.
