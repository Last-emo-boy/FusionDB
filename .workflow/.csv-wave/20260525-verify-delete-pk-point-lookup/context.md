# DELETE primary-key point lookup verification

Verification target:
- TASK-015 shared DML primary-key row-id helper.
- TASK-016 DELETE primary-key point lookup.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_delete_with_where -- --nocapture`
- `cargo test --test sql_integration test_delete_primary_key_updates_secondary_index -- --nocapture`
- `cargo check --lib`

All checks passed in the local workspace.
