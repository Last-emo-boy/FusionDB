# Execute: GROUP BY aggregates with simple WHERE column scan

## Summary

Implemented `TASK-146` in `src/execution/query.rs`.

The single-column `GROUP BY` fast paths now accept a simple column predicate and decode only predicate, group, and aggregate columns. Complex predicates continue to fall back to the general evaluator.

## Files

- `src/execution/query.rs`
- `tests/sql_integration.rs`

## Scope Guard

No dashboard/UI files were touched. Cargo verification used `E:\Playground\FusionDB\target`.
