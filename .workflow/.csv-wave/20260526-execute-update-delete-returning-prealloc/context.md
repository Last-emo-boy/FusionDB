# TASK-102 Execution Context

- Scope: `src/execution/dml.rs`
- Change: DELETE RETURNING row buffer now preallocates from candidate `kv_pairs.len()`.
- Change: UPDATE RETURNING row buffer now preallocates from candidate `kv_pairs.len()`.
- Semantics preserved: selection filtering, row decoding, index maintenance, cache invalidation, and returned row order remain unchanged.
