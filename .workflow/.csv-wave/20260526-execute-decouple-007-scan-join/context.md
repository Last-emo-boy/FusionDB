# DECOUPLE-007 Execution Context

## Scope

- `src/execution/scan/mod.rs`
- `src/execution/scan/join.rs`

## Change

- Added `scan::join` submodule.
- Moved join table scanning, join predicate extraction, hash/nested-loop/indexed probe join execution, join projection pushdown, and join result projection helpers into `join.rs`.
- Kept `Executor::execute_join` available for query execution.

## Resulting Size

- `src/execution/scan/mod.rs`: 854 lines
- `src/execution/scan/join.rs`: 947 lines
