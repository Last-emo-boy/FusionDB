# TASK-044 Execution

Changed `src/execution/ddl.rs` so `ALTER TABLE DROP COLUMN` row rewrites check `row_cache` before decoding stored row bytes.

The rewrite path invalidates each updated data key after writing the new row shape, preventing stale cached rows from surviving the schema change.
