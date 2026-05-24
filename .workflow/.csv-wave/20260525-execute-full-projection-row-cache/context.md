# TASK-042 Execution

Changed `src/execution/scan.rs` so the full table scan path checks `row_cache` before partial projection decoding when data keys are valid UTF-8.

The implementation only reuses already cached full rows; projection rows are still not inserted into `row_cache`, preserving projection-cache safety.
