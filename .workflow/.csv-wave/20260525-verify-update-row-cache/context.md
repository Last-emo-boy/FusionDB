# Verify Context

Verification status: passed.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_update_primary_key_reuses_row_cache_for_secondary_index -- --nocapture`
- `cargo test --test sql_integration test_update_invalidates_row_cache_for_index_lookup -- --nocapture`
- `cargo check --lib`

Notes:
- A corrupted-storage test verifies UPDATE can use a cached row instead of decoding storage bytes.
- Existing row-cache invalidation after UPDATE remains covered by regression.
