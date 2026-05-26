# DECOUPLE-003 Execution Context

## Scope

- `src/execution/query/mod.rs`
- `src/execution/query/column_scan.rs`

## Change

- Added `query::column_scan` submodule.
- Moved column aggregate, distinct, count distinct, and GROUP BY column-scan fast path structs/helpers into `column_scan.rs`.
- Kept common query/order helpers in `query/mod.rs` for later `DECOUPLE-004`.

## Resulting Size

- `src/execution/query/mod.rs`: 2108 lines
- `src/execution/query/column_scan.rs`: 1056 lines
