# Plan Context

Goal: continue database core performance iteration without touching `dashboard/`.

Findings:
- Full-row index and primary-key lookup paths already use `row_cache`.
- Full-row table scans decoded every row on every scan, even when a full row had already been cached.
- Projection scans must not populate the full-row cache because projected rows contain only partial data.
- UPDATE and upsert cache invalidation was completed in TASK-029, making broader cache reuse safer.

Decision:
- Reuse `row_cache` only when full-row table scans have no projection indices.
- Decode and insert into `row_cache` on full-row scan cache misses.
- Invalidate the cache on normal INSERT writes to avoid stale values when a key is overwritten.
