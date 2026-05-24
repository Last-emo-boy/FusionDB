# Verify Context

Verification status: passed.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_hnsw_order_by_projection -- --nocapture`
- `cargo test --test sql_integration test_join_projection_pushdown_with_group_by -- --nocapture`
- `cargo test --test sql_integration test_inner_join_with_left_filter_and_indexed_right_probe -- --nocapture`
- `cargo test --test sql_integration test_inner_join_multi_key_uses_indexed_probe_column -- --nocapture`
- `cargo check --lib`

Notes:
- HNSW projected rows are only cached when decoded as full rows.
- Right-side join projection pushdown was intentionally deferred because indexed join probe paths still assume full right rows.
