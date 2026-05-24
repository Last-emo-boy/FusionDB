# MIN/MAX primary-key key-derived verification

Verification target:
- TASK-013 primary-key value derivation from data keys.
- TASK-014 MIN/MAX(primary key) value path.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_select_min_max_primary_key -- --nocapture`
- `cargo check --lib`

All checks passed in the local workspace.
