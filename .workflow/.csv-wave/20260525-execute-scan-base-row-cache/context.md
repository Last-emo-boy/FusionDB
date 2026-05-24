# TASK-045 Execution

Changed `src/execution/scan.rs` so `scan_table_base` checks `row_cache` by data key before decoding stored row bytes.

Added a join regression test because `scan_table_base` is used by join base scans.
