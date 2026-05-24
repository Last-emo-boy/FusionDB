# Verify Context

Verification status: passed.

Checks:
- `cargo fmt --check`
- `cargo test fusion_rebuild_vector_index_decodes_only_hnsw_columns --lib -- --nocapture`
- `cargo check --lib`

Notes:
- The regression test corrupts a non-HNSW column payload while leaving the HNSW vector column decodable. Rebuild still restores the vector index, which guards the new single-column decode behavior.
