# Execute: single-column DISTINCT ORDER/LIMIT column scan

## Summary

Implemented `TASK-147` in `src/execution/query.rs`.

The single-column `DISTINCT` fast path now supports simple ordering and pagination when `ORDER BY` references the output column or ordinal. Unsupported order expressions still fall back to the general evaluator.

## Files

- `src/execution/query.rs`
- `tests/sql_integration.rs`

## Scope Guard

No dashboard/UI files were touched. Cargo verification used `E:\Playground\FusionDB\target`.
