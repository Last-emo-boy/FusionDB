# Plan Context

Goal: continue database core performance iteration without touching `dashboard/`.

Findings:
- Index and primary-key lookup paths can cache full rows in `row_cache`.
- DELETE paths already invalidate cached rows, but UPDATE and `ON CONFLICT DO UPDATE` wrote new row bytes without invalidating the cached full row.
- Keeping cache invalidation complete is required before extending cache reuse to more scan paths.

Decision:
- Invalidate the row cache after UPDATE writes a row.
- Invalidate the row cache after upsert conflict updates an existing row.
- Cover both index lookup paths with regressions that would read stale cached values without invalidation.
