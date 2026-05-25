# TASK-093 Execution Context

- Scope: `src/execution/query.rs`
- Change: non-GROUP BY projection now preallocates the outer projected row vector from `rows.len()`.
- Change: each projected row now preallocates from `select.projection.len()`.
- Semantics preserved: value evaluation, window result substitution, projection ordering, and DISTINCT handling remain unchanged.
