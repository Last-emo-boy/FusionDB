# TASK-086 Execution Context

## Scope

- `src/execution/scan.rs`
- Database core only; `dashboard/` untouched.

## Change

- `filter_rows_with_expr` now preallocates its output from the input row count.
- Join pending predicate filtering preallocates from `min(rows.len(), limit)` when a limit exists.
- Late scan selection filtering uses the same bounded capacity pattern.

## Expected Impact

- Lower allocation churn on common WHERE and join filter paths.
- Query behavior remains unchanged because only vector capacities changed.
