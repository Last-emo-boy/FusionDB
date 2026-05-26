# DECOUPLE-006 Execution Context

## Scope

- `src/execution/scan/mod.rs`
- `src/execution/scan/index_plan.rs`

## Change

- Added `scan::index_plan` submodule.
- Moved `IndexScanPlan`, `try_index_scan`, row-id conversion/parsing, primary-key row reconstruction, projected row decode, full-row fetch, and index candidate cap helpers into `index_plan.rs`.
- Kept existing scan execution call sites in `scan/mod.rs`.

## Resulting Size

- `src/execution/scan/mod.rs`: 854 lines
- `src/execution/scan/index_plan.rs`: 654 lines
