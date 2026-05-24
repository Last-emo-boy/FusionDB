# Plan Context

Goal: continue database core performance iteration without touching `dashboard/`.

Findings:
- Primary-key equality lookup already avoids a table scan, but the non key-only path still decoded the whole row.
- The query planner already provides a projection hint that includes projected, filter, grouping, having, and ordering columns.
- For `WHERE pk = value`, the selection can still be evaluated after partial decoding because the primary key column is included by the selection hint.

Decision:
- Use the existing projection-aware decoder for primary-key equality lookup.
- Keep key-only and full-row behavior unchanged when projection hints are absent.
