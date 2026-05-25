# TASK-108 Execution Context

- Target: `src/execution/query.rs`.
- Change: ordinary SELECT output column name collection now preallocates from `select.projection.len()`.
- Rationale: non-wildcard SELECT naming emits at most one output name per projection item; wildcard branches can still replace the vector with schema columns.
