# Verify Context

Verification status: passed.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_delete_all_without_secondary_index_skips_row_decode -- --nocapture`
- `cargo test --test sql_integration test_delete_all -- --nocapture`
- `cargo test --test sql_integration test_delete_primary_key_updates_secondary_index -- --nocapture`
- `cargo check --lib`

Notes:
- Secondary index cleanup remains on the old decode path and is covered by the regression test.
