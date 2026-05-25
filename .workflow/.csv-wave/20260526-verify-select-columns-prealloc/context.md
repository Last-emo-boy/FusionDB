# TASK-108 Verification Context

- `cargo fmt --check`: passed.
- `cargo check --lib`: passed.
- `cargo test --test sql_integration test_select_projection -- --nocapture`: passed.
- `cargo test --test sql_integration test_select_constant_projection_from_table -- --nocapture`: passed.
- `cargo test --test sql_integration test_select_count_star -- --nocapture`: passed.
- Coverage includes SELECT output column naming for ordinary projection, constant projection, and COUNT(*).
