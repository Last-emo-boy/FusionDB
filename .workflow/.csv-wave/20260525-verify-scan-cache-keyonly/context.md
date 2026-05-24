# Scan cache and key-only verification

Verification target:
- TASK-007 index projection cache correctness.
- TASK-008 primary-key-only equality lookup.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_index_projection_does_not_poison_row_cache -- --nocapture`
- `cargo test --test sql_integration test_primary_key_only_equality_projection -- --nocapture`
- `cargo check --lib`

All checks passed in the local workspace.
