# TASK-104 Execution Context

- Target: `src/execution/query.rs`.
- Change: no-FROM `SELECT` expression evaluation now preallocates `col_names` and `result_row` from `select.projection.len()`.
- Rationale: this path builds one output name and value per projection item, so projection count is the natural capacity bound.
