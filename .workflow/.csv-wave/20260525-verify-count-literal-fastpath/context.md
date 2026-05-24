# COUNT literal fast path verification

Verification target:
- TASK-011 COUNT fast path eligibility.
- TASK-012 no-filter COUNT literal fast path and COUNT(NULL) guard.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_select_count_literal -- --nocapture`
- `cargo test --test sql_integration test_select_count_null_literal -- --nocapture`
- `cargo check --lib`

All checks passed in the local workspace.
