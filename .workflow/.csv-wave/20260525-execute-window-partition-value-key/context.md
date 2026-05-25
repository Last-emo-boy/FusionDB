# TASK-066 Window Partition Value Keys

Scope: `src/execution/query.rs`

Implemented:
- Replaced `HashMap<String, Vec<usize>>` partition grouping with `HashMap<Vec<Value>, Vec<usize>>`.
- Removed the intermediate `partition_keys` vector.
- Grouped row indices as partition keys are evaluated.

Validation is recorded in `.workflow/.csv-wave/20260525-verify-window-partition-value-key/verification.json`.
