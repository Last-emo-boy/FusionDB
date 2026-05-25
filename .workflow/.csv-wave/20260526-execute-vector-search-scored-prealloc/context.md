# TASK-096 Execution Context

- Scope: `src/storage/vector_index.rs`
- Change: `VectorIndex::search` now preallocates scored candidates from the HNSW returned id count.
- Change: iterator `filter_map(...).collect()` was replaced with explicit push into the preallocated vector.
- Semantics preserved: missing ids are still skipped, exact Euclidean distances are still recomputed, and limit/sort behavior is unchanged.
