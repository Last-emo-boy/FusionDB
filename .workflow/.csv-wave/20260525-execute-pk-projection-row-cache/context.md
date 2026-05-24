# TASK-040 Execution

Changed `src/execution/scan.rs` so the primary-key equality lookup path checks `row_cache` even when scan projection indices are present.

The implementation only reuses already cached full rows; partial projection rows are still not inserted into `row_cache`, preserving the existing projection-cache safety invariant.
