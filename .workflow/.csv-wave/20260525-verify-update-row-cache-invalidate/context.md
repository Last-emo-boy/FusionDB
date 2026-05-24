# Verify Context

Verification status: passed.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_update_invalidates_row_cache_for_index_lookup -- --nocapture`
- `cargo test --test sql_integration test_upsert_do_update_invalidates_row_cache_for_index_lookup -- --nocapture`
- `cargo check --lib`

Notes:
- This prepares the cache layer for broader scan cache reuse without stale-row regressions.
