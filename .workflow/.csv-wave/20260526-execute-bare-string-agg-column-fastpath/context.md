# Execute: bare STRING_AGG/GROUP_CONCAT ordinary-column scan fast path

## Summary

Implemented `TASK-141` in `src/execution/query.rs`.

The bare aggregate column-scan fast path now supports ordinary-column `STRING_AGG(column)` and `GROUP_CONCAT(column)`, including simple predicate filtering. The implementation follows the existing aggregate accumulator semantics: skip `NULL`, stringify primitive scalar values, and join values with `,`.

## Files

- `src/execution/query.rs`
- `tests/sql_integration.rs`

## Scope Guard

No dashboard/UI files were touched. Cargo verification used `E:\Playground\FusionDB\target`.
