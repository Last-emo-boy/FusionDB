# TASK-050 Execution

Added `row_id_from_key` and reused it across BTree, FTS, `IN`, `LIKE`, and primary-key range scan paths in `src/execution/scan.rs`.

Files changed: `src/execution/scan.rs`.
