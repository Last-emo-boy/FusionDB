# Plan Context

Goal: continue database core performance iteration without touching `dashboard/`.

Findings:
- `DELETE FROM table` with no `RETURNING` deletes every row but previously decoded each row before deletion.
- When the table has no non-primary indexed columns, there are no secondary index entries that require row values for cleanup.
- Tables with secondary, FTS, or HNSW indexes still need the existing full decode path.

Decision:
- Add a conservative fast path only for unconditional delete without `RETURNING` and without secondary indexes.
- Reuse prefix scan keys and delete the encoded rows directly.
- Keep the full decode path for any indexed cleanup or `RETURNING` case.
