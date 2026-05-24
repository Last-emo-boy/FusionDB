# Plan Context

Goal: continue database core performance iteration without touching `dashboard/`.

Findings:
- Existing `MIN(id)` / `MAX(id)` fast path derives primary-key extrema from data-key bounds.
- The same optimization is valid for qualified `MIN(table.id)` / `MAX(table.id)` and alias-qualified `MIN(t.id)` / `MAX(t.id)`.
- Compound identifiers must be checked against the active table name or alias so unrelated qualifiers are not accepted by the fast path.

Decision:
- Reuse one primary-key aggregate argument helper for `COUNT`, `MIN`, and `MAX`.
- Allow unqualified primary keys plus qualifiers matching the current table name or alias.
- Keep filtered, grouped, joined, and non-primary-key aggregates on the existing row-evaluation path.
