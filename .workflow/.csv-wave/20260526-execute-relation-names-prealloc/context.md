# TASK-114 Execution Context

- Target: `src/execution/scan.rs`.
- Change: `relation_names` now preallocates its `HashSet` with capacity for table name and alias.
- Rationale: relation predicate routing only needs at most those two relation identifiers per table factor.
