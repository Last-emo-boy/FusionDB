# TASK-093 Verification Context

- `cargo fmt --check`: passed.
- `cargo check --lib`: passed.
- `cargo test --test sql_integration test_select_projection -- --nocapture`: passed.
- `cargo test --test sql_integration test_select_constant_projection_from_table -- --nocapture`: passed.
- `cargo test --test sql_integration test_arithmetic_expression -- --nocapture`: passed.
- `cargo test --test sql_integration test_window -- --nocapture`: passed.
- Coverage includes column projection, constant projection, arithmetic expression projection, and window projection substitution.
