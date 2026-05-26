# DECOUPLE-009 Execution Context

## Scope

- `src/execution/ddl.rs`
- `src/execution/ddl/mod.rs`
- `src/execution/ddl/show.rs`
- `src/execution/ddl/explain.rs`
- `src/execution/ddl/index.rs`
- `src/execution/ddl/table.rs`
- `src/execution/ddl/view.rs`

## Change

- Converted `ddl.rs` into a module directory.
- Moved SHOW/DESCRIBE handlers to `show.rs`.
- Moved EXPLAIN helpers to `explain.rs`.
- Moved index DDL handlers to `index.rs`.
- Moved table DDL handlers to `table.rs`.
- Moved view handlers to `view.rs`.

## Resulting Size

- `src/execution/ddl/mod.rs`: 5 lines
- `src/execution/ddl/show.rs`: 183 lines
- `src/execution/ddl/explain.rs`: 236 lines
- `src/execution/ddl/index.rs`: 209 lines
- `src/execution/ddl/table.rs`: 328 lines
- `src/execution/ddl/view.rs`: 63 lines
