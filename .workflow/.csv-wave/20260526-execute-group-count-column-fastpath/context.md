# TASK-136 Execution Context

Extended the simple GROUP BY column aggregate fast path with `COUNT(column)` support.

Behavior:
- `COUNT(column)` decodes only the target aggregate column.
- `NULL` values are not counted.
- `COUNT(*)` remains row-count based.
- Existing grouped-result `ORDER BY` and `LIMIT` support continues to work.

Non-goals:
- `COUNT(DISTINCT column)` in GROUP BY
- expression arguments such as `COUNT(a + b)`
- complex GROUP BY shapes
