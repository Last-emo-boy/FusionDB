# TASK-100 Execution Context

- Scope: `src/execution/dml.rs`
- Change: `build_returning_result` now preallocates column names from `ret_items.len()`.
- Change: returned row storage is preallocated from `rows.len()`.
- Change: non-wildcard per-row RETURNING projections are preallocated from `ret_items.len()`.
- Semantics preserved: wildcard RETURNING, aliased expressions, expression evaluation, and returned row order are unchanged.
