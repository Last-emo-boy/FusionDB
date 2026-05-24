# Verify Context

Verification status: passed.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration commuted -- --nocapture`
- `cargo test --test sql_integration qualified_primary_key -- --nocapture`
- `cargo test --test sql_integration test_explain -- --nocapture`
- `cargo check --lib`

Notes:
- `value = primary_key` now uses the same DML point lookup as `primary_key = value`.
- `value = indexed_column` is reflected in EXPLAIN as an index scan.
- Fast-path extraction rejects expressions where the value side references another column.
