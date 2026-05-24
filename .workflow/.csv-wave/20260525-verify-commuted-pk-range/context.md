# Verify Context

Verification status: passed.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration commuted_primary_key_range -- --nocapture`
- `cargo test --test sql_integration test_select_with_where_gt -- --nocapture`
- `cargo check --lib`

Notes:
- Tests cover all four commuted comparison directions.
- A nonmatching corrupted-row case verifies the range scan avoids decoding rows outside the normalized range.
- EXPLAIN now reports `Primary Key Range Scan` for `1 < id` style predicates.
