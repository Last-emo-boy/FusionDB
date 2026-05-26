# DECOUPLE-002 Execution Context

## Scope

- `tests/sql_integration.rs`
- `tests/sql/common.rs`

## Change

- Moved shared `setup`, `query`, `exec_ok`, and `cleanup` helpers into `tests/sql/common.rs`.
- Kept direct `Executor`, `MemoryStorage`, `Storage`, and `Arc` imports in `tests/sql_integration.rs` because several tests manually use storage transactions.
- Did not move any test cases yet.
