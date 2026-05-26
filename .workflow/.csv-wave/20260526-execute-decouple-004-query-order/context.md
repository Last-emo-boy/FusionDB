# DECOUPLE-004 Execution Context

## Scope

- `src/execution/query/mod.rs`
- `src/execution/query/order.rs`

## Change

- Added `query::order` submodule.
- Moved ORDER BY value-source resolution, row sorting, and primary-key ORDER BY LIMIT pushdown helpers into `order.rs`.
- Imported `SortOrderKey` in `query/mod.rs` for the existing sort-key construction site.

## Resulting Size

- `src/execution/query/mod.rs`: 1702 lines
- `src/execution/query/order.rs`: 417 lines
