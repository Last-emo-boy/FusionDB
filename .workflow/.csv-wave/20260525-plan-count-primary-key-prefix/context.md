# Plan Context

Goal: continue database core performance iteration without touching `dashboard/`.

Findings:
- Existing aggregate fast path already uses `count_prefix` for `COUNT(*)` and non-null literal counts.
- `COUNT(primary_key)` has the same no-NULL semantics because primary keys are stored as non-null columns.
- This optimization is only safe for single-table queries without `WHERE`, without joins, and without `GROUP BY`.

Decision:
- Extend the existing `count_prefix` eligibility check to primary-key identifiers.
- Keep `COUNT(DISTINCT ...)`, non-primary columns, and filtered queries on the existing row evaluation path.
