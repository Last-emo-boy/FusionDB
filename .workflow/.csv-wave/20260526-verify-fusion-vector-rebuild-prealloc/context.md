# TASK-117 Verification Context

- `cargo fmt --check`: passed.
- `cargo check --lib`: passed.
- `cargo test --lib fusion_rebuild_vector_index_decodes_only_hnsw_columns -- --nocapture`: passed.
- Coverage includes Fusion vector index rebuild for HNSW-indexed columns.
