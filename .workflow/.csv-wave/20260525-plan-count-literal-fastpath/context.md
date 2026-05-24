# COUNT literal fast path plan

Goal: continue database-core performance iteration by extending the existing no-filter aggregate fast path from `COUNT(*)` to row-count-equivalent `COUNT` forms.

Scope:
- Include: `src/execution/query.rs`, focused SQL integration tests.
- Exclude: `dashboard/`.

Findings:
- The no-filter aggregate fast path already routes `COUNT(*)` through `Transaction::count_prefix`.
- `COUNT(1)` and other non-NULL literal count expressions still fell through to the generic bare aggregate path.
- `COUNT(NULL)` must not be optimized to row count because SQL semantics require zero for every row.
- `COUNT(DISTINCT ...)` must stay on the generic path.

Plan:
- TASK-011: add a narrow eligibility helper for `COUNT` arguments that can safely map to row count.
- TASK-012: reuse `count_prefix` for eligible no-filter literal `COUNT` calls and add a `COUNT(NULL)` guard test.
