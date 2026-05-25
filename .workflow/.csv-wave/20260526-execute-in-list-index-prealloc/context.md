# TASK-088 Execution Context

## Scope

- `src/execution/scan.rs`
- `tests/sql_integration.rs`
- Database core only; `dashboard/` untouched.

## Change

- `Expr::InList` index scan now initializes `all_row_ids` with `list.len()`.
- Secondary-index IN list scans reserve capacity from each `scan_prefix` result length before inserting row ids.
- `test_select_in_list` now covers a secondary BTree index `IN` query in addition to primary-key `IN`.

## Expected Impact

- Lower HashSet allocation churn in indexed `IN (...)` scans.
- Query behavior remains unchanged.
