# Verify Context

Verification status: passed.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_select_count_not_null_column_uses_prefix_count -- --nocapture`
- `cargo test --test sql_integration test_select_count_primary_key_uses_prefix_count -- --nocapture`
- `cargo test --test sql_integration test_select_count_null_literal -- --nocapture`
- `cargo check --lib`

Notes:
- `COUNT(NULL)` remains excluded from the prefix-count path.
- Nullable columns still use existing row-evaluation semantics.
