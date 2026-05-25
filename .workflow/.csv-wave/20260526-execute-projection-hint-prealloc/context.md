# TASK-111 Execution Context

- Target: `src/execution/query.rs`.
- Change: projection hint column sets now preallocate from `select.projection.len()` in both normal and materialized-subquery paths.
- Rationale: projection expressions are the first and common lower-bound contributor to the hint set, so this avoids the first growth in common projection pushdown paths without changing hint contents.
