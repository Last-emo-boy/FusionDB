# Verify Context

Verification status: passed.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_full_table_scan_reuses_row_cache -- --nocapture`
- `cargo test --test sql_integration test_insert_overwrite_invalidates_full_scan_row_cache -- --nocapture`
- `cargo check --lib`

Notes:
- Projection scans still bypass full-row cache population.
- INSERT overwrite invalidation keeps full-scan cache reuse coherent.
