# Single-column aggregate decoding verification

Verification target:
- TASK-005 single-column row decoder.
- TASK-006 primary-key MIN/MAX aggregate fast path.

Checks:
- `cargo fmt --check`
- `cargo test test_row_encoding_single_column --lib -- --nocapture`
- `cargo test --test sql_integration test_select_min_max_primary_key -- --nocapture`
- `cargo check --lib`

All checks passed in the local workspace.
