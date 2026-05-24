# TASK-041 Execution

Changed `src/execution/scan.rs` so the primary-key range scan path checks `row_cache` even when projection indices are present.

The implementation only reuses already cached full rows; partial projection rows are still not inserted into `row_cache`, preserving the projection-cache safety invariant.
