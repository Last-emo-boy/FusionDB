# MIN/MAX primary-key key-derived plan

Goal: continue database-core performance iteration by avoiding row value decoding in the no-filter `MIN/MAX(primary key)` aggregate fast path.

Scope:
- Include: `src/execution/query.rs`, focused SQL integration tests.
- Exclude: `dashboard/`.

Findings:
- `MIN/MAX(pk)` already uses `Transaction::first` and `Transaction::last` to find the boundary data key.
- The fast path still decoded the row value to extract the primary-key column.
- FusionDB data keys are `data:<table>:<row_id>`, and integer primary keys use comparable row-id encoding, so the aggregate value can be restored from the key suffix.

Plan:
- TASK-013: add a helper that restores a primary-key `Value` from a data key suffix.
- TASK-014: use the helper in `MIN/MAX(pk)` and extend the regression test to include a negative integer primary key.
