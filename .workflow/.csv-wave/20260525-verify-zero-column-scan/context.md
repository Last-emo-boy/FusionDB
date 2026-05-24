# Zero-column scan verification

Verification target:
- TASK-009 zero-column projection hint preservation.
- TASK-010 zero-column scan decode bypass.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_select_constant_projection_from_table -- --nocapture`
- `cargo test --test sql_integration test_select_count_literal -- --nocapture`
- `cargo check --lib`

All checks passed in the local workspace.
