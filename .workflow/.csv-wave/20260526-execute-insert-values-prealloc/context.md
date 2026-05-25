# TASK-101 Execution Context

- Scope: `src/execution/dml.rs`
- Change: explicit column-list mapping now uses `Vec::with_capacity(columns.len())`.
- Change: `INSERT ... VALUES ... RETURNING` now reserves inserted row storage from `values.rows.len()`.
- Change: each VALUES row now preallocates `raw_values` from the expression count.
- Semantics preserved: value evaluation, defaults, constraints, conflict handling, index maintenance, and RETURNING behavior remain unchanged.
