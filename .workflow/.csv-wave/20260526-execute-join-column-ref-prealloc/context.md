# TASK-119 Execution

Target: `src/execution/scan.rs`

Change:
- Added a `join_count` pass in `collect_join_column_references`.
- Initialized the column reference `HashSet` with `join_count.saturating_mul(2)`.

Behavior:
- JOIN predicate discovery and expression traversal are unchanged.
- The optimization only reduces avoidable HashSet growth during JOIN projection planning.
