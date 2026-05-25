# TASK-112 Execution Context

- Target: `src/execution/scan.rs`.
- Change: scan projection index collection now preallocates from `projection.len()`.
- Rationale: each requested projection column can contribute at most one schema index.
