# Execute: bare COUNT(nullable column) ordinary-column scan fast path

## Summary

Implemented `TASK-142` in `src/execution/query.rs`.

The bare aggregate column-scan fast path now supports `COUNT(column)` when a column scan is the correct path, including simple predicate filtering. Existing prefix-count shortcuts remain in place for `COUNT(*)`, non-null literals, primary keys, and `NOT NULL` columns without a predicate.

## Files

- `src/execution/query.rs`
- `tests/sql_integration.rs`

## Scope Guard

No dashboard/UI files were touched. Cargo verification used `E:\Playground\FusionDB\target`.
