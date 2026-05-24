# Verify Context

Verification status: passed.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_primary_key_point_lookup_reuses_row_cache -- --nocapture`
- `cargo test --test sql_integration test_primary_key_equality_projection_skips_unused_column_decode -- --nocapture`
- `cargo check --lib`

Notes:
- Repeated full-row primary-key point lookup now uses `row_cache`.
- Existing primary-key partial projection decode remains covered by regression.
