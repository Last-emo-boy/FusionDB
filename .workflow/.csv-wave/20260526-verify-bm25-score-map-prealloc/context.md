# TASK-094 Verification Context

- `cargo fmt --check`: passed.
- `cargo check --lib`: passed.
- `cargo test --lib storage::inverted_index -- --nocapture`: passed.
- `cargo test --test sql_integration test_fts_match_against_multi_token_intersects_index_hits -- --nocapture`: passed.
- `cargo test --test sql_integration test_parameter_placeholder_match_against -- --nocapture`: passed.
- Coverage includes score ordering, result limiting, literal MATCH queries, and parameterized MATCH queries.
