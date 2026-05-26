# Execute: COUNT(DISTINCT column) with simple WHERE column scan

## Summary

Implemented `TASK-144` in `src/execution/query.rs`.

The `COUNT(DISTINCT column)` fast path now accepts a simple column predicate and decodes only the predicate column plus the distinct counted column. Complex predicates continue to fall back to the general row evaluator.

## Files

- `src/execution/query.rs`
- `tests/sql_integration.rs`

## Scope Guard

No dashboard/UI files were touched. Cargo verification used `E:\Playground\FusionDB\target`.
