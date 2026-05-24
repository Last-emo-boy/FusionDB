# TASK-046 Execution

Changed `src/execution/scan.rs` so `scan_table_base` inserts decoded full rows into `row_cache` after a cache miss.

Added `test_join_base_scan_populates_row_cache` to prove join base scans can warm cache for subsequent full-row queries.
