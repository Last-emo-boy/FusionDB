# DECOUPLE-001 Execution Context

## Scope

- `src/execution/query.rs`
- `src/execution/query/mod.rs`
- Database core only; `dashboard/` and `ui/` untouched.

## Change

- Mechanically moved `src/execution/query.rs` to `src/execution/query/mod.rs`.
- Kept the existing `mod query;` declaration in `src/execution/mod.rs`.
- No query behavior or helper boundaries changed.
