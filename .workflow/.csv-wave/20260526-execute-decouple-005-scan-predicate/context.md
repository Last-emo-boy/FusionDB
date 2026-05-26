# DECOUPLE-005 Execution Context

## Scope

- `src/execution/scan.rs`
- `src/execution/scan/mod.rs`
- `src/execution/scan/predicate.rs`

## Change

- Converted `scan.rs` into the `scan` module directory as `scan/mod.rs`.
- Added `scan::predicate` submodule.
- Moved conjunctive predicate split/combine helpers, relation/schema predicate ownership checks, and schema column-name/index helpers into `predicate.rs`.
- Kept `Executor` call sites unchanged by exposing only the needed helpers as `pub(super)`.

## Resulting Size

- `src/execution/scan/mod.rs`: 2429 lines
- `src/execution/scan/predicate.rs`: 189 lines
