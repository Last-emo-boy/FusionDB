# TASK-125 execution

- Target: `src/execution/query.rs`
- Change: wildcard `SELECT * ... ORDER BY column/ordinal` now resolves sort keys to schema row indices before sorting.
- Change: row-index sort comparison uses borrowed `Value` references instead of cloning both sides.
- Safety: alias, projection, computed expression, GROUP BY, and set-operation sorting stay on their existing paths.
- Constraint: database core only; no `dashboard/` changes.
