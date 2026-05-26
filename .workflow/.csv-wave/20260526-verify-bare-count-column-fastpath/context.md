# Verify: bare COUNT(nullable column) ordinary-column scan fast path

## Result

`TASK-142` passed focused verification.

## Checks

- `cargo fmt -- src/execution/query.rs tests/sql_integration.rs`
- `cargo test count_nullable --test sql_integration`: 2 passed
- `cargo test select_count --test sql_integration`: 7 passed
- `cargo test bare_aggregate --test sql_integration`: 2 passed
- `cargo test bare_sum_avg --test sql_integration`: 2 passed

All Cargo commands used `CARGO_TARGET_DIR=E:\Playground\FusionDB\target` and `CARGO_PROFILE_TEST_DEBUG=0`.
