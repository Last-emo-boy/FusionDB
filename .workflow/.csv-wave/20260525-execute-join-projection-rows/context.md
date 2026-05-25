# TASK-059 Join Projection Rows

Scope: `src/execution/scan.rs`

Implemented:
- Replaced per-row `collect::<Vec<_>>()` in `project_join_rows` with explicit `Vec::with_capacity` and `push`.
- Preserved projection index order, projected schema construction, and row value cloning semantics.

Validation is recorded in `.workflow/.csv-wave/20260525-verify-join-projection-rows/verification.json`.
