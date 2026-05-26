# Execute: bare MIN/MAX ordinary-column scan fast path

## Summary

Implemented `TASK-140` in `src/execution/query.rs`.

The bare aggregate column-scan fast path now supports ordinary-column `MIN(column)` and `MAX(column)`, including simple predicate filtering. Primary-key `MIN/MAX` keeps its existing key-range shortcut.

## Files

- `src/execution/query.rs`
- `tests/sql_integration.rs`

## Scope Guard

No dashboard/UI files were touched. Cargo verification used `E:\Playground\FusionDB\target`.
