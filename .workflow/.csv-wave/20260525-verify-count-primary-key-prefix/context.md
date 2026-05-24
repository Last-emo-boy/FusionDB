# Verify Context

Verification status: passed.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_select_count_primary_key_uses_prefix_count -- --nocapture`
- `cargo test --test sql_integration test_select_count_literal -- --nocapture`
- `cargo test --test sql_integration test_select_count_null_literal -- --nocapture`
- `cargo check --lib`

Notes:
- `COUNT(NULL)` remains excluded from the prefix-count path.
- `COUNT(DISTINCT ...)` remains on the existing aggregate path.
