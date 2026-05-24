# TASK-039 Execution

Changed `src/execution/dml.rs` so non-PK UNIQUE constraint scans check `row_cache` by data key before decoding a single column from existing row bytes.

Added `test_insert_unique_check_reuses_row_cache` to cover the cache-hit path during INSERT constraint validation.
