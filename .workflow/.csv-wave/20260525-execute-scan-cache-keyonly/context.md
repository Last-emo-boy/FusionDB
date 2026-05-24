# Scan cache and key-only execution

Executed TASK-007 and TASK-008.

Changes:
- `src/execution/scan.rs`: added `primary_key_row_from_id` helper for key-derived sparse rows.
- `src/execution/scan.rs`: made PK equality key-only scans avoid full row decoding after confirming the data key exists.
- `src/execution/scan.rs`: gated `row_cache` inserts so projection-decoded rows are not cached as full rows.
- `src/execution/scan.rs`: threaded `from_cache`, `cacheable`, and `read_storage` flags through streamed index fetches.
- `tests/sql_integration.rs`: added cache pollution and PK-only equality regression tests.

Dashboard files were not modified.
