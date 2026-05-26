# DECOUPLE-010 Execution Context

## Scope

- `src/execution/dml.rs`
- `src/execution/dml/mod.rs`
- `src/execution/dml/insert.rs`
- `src/execution/dml/update.rs`
- `src/execution/dml/delete.rs`
- `src/execution/dml/returning.rs`
- `src/execution/dml/constraints.rs`

## Change

- Converted `dml.rs` into a module directory.
- Kept shared primary-key targeting helpers in `dml/mod.rs`.
- Moved INSERT/UPSERT handling to `insert.rs`.
- Moved UPDATE handling to `update.rs`.
- Moved DELETE handling to `delete.rs`.
- Moved RETURNING projection to `returning.rs`.
- Moved CHECK/default helpers to `constraints.rs`.

## Resulting Size

- `src/execution/dml/mod.rs`: 148 lines
- `src/execution/dml/insert.rs`: 422 lines
- `src/execution/dml/update.rs`: 212 lines
- `src/execution/dml/delete.rs`: 204 lines
- `src/execution/dml/returning.rs`: 54 lines
- `src/execution/dml/constraints.rs`: 90 lines
