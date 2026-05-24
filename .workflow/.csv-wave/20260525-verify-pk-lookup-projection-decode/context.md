# Verify Context

Verification status: passed.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_primary_key_equality_projection_skips_unused_column_decode -- --nocapture`
- `cargo test --test sql_integration test_primary_key_only_equality_projection -- --nocapture`
- `cargo check --lib`

Notes:
- Key-only primary-key lookup remains on the existing key-derived row path.
