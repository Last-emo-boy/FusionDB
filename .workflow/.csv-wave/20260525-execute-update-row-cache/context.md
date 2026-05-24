# Execute Context

Execution status: completed.

Changes:
- `UPDATE` checks `row_cache` before decoding row bytes.
- Cache hits are reused for WHERE evaluation, assignment evaluation, constraint checks, and secondary index maintenance.
- Existing cache invalidation after UPDATE remains unchanged.
