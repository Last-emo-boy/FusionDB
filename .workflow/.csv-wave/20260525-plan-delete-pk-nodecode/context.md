# Plan Context

Goal: continue database core performance iteration without touching `dashboard/`.

Findings:
- `DELETE ... WHERE primary_key = value` already uses a point lookup, but still decoded the row before deleting it.
- If there is no `RETURNING` clause and no non-primary indexed column, deletion does not need row values.
- Secondary, FTS, and HNSW indexes still require row values for cleanup, so those tables must keep the existing path.

Decision:
- Add a conservative fast path only for primary-key point delete without `RETURNING` and without secondary indexes.
- Keep the full decode path for all indexed cleanup or `RETURNING` cases.
