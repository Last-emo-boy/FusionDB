# TASK-091 Execution Context

- Scope: `src/execution/query.rs`
- Change: `compute_window_function` now avoids per-row empty partition key construction when `PARTITION BY` is absent by using a single all-row partition.
- Partitioned windows now preallocate the partition map from `rows.len()` and each partition key from `spec.partition_by.len()`.
- Semantics preserved: sorting, ranking, `LAG`, `LEAD`, defaults, and partition grouping remain unchanged.
