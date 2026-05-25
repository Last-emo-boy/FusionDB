# TASK-117 Execution Context

- Target: `src/storage/fusion.rs`.
- Change: HNSW column collection now preallocates from `schema.columns.len()`.
- Change: rebuild batch map now preallocates from the discovered HNSW column count.
- Rationale: vector index rebuild scans each schema once and later builds one batch bucket per HNSW column.
