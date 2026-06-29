# BENCHPROD-452 Execution Context

Distributed grouped `HAVING` post-merge — the last grouped post-merge clause, completing the family
(450/451 ORDER BY/LIMIT for SUM/MIN/MAX; 459 for COUNT/AVG). A group spans shard owners, so `HAVING`
(like `LIMIT`) MUST be evaluated post-merge on the GLOBAL groups, never per-owner: a group below the
threshold on each owner may be above it globally. Distributed-only (no single-node bench delta);
converts queries the 449 net currently errors on into correct distributed results.

## Design
- `GroupedPostMerge` gains `having: Option<GroupedHaving>`. `GroupedHaving` = AND of
  `GroupedHavingConjunct { col_index, op: GroupedHavingOp(Gt/GtEq/Lt/LtEq/Eq/NotEq), literal:
  serde_json::Value }`. (The 3 plan structs dropped the `Eq` derive — `serde_json::Value` is not `Eq`;
  `PartialEq` is all that's used, via `assert_eq!`.)
- `apply_grouped_order_limit` now applies the spec in SQL order: **filter by `having` → stable-sort by
  `order_keys` → `offset`/`limit`**. `grouped_having_predicate_holds` ANDs the conjuncts; a `NULL` on
  either side of a comparison drops the row (SQL "unknown" ≠ TRUE).
- Resolution (`resolve_grouped_having` → `collect_grouped_having_conjuncts`): walk the `HAVING` `Expr` —
  unwrap `Nested`, recurse `AND`, each leaf is one comparison. Anything else (`OR`, `NOT`, `IN`,
  `BETWEEN`, `IS NULL`, function-of-row, etc.) → `None` → the 449 net errors loudly. Over-reject freely.
- The 3 grouped extractors removed `select.having.is_some()` from their early reject and added it to the
  post-merge gate; `resolve_grouped_order_limit` now also takes the `HAVING` expr and returns the
  resolved `GroupedHaving`. Per-owner SQL: count/aggregate use `strip_grouped_post_merge_clauses` (which
  already nulls the inner `HAVING`); AVG uses `rewritten_sql` (rebuilt from FROM/WHERE/GROUP BY — never
  carried `HAVING`). All 4 entry points get `HAVING` for free since they already call the shared
  `apply_grouped_order_limit`.

## Adversarial review caught a real bug (high confidence, 2 independent finders)
**HAVING reused the ORDER BY positional resolver.** `resolve_grouped_having_comparison` resolved each
side via `resolve_grouped_order_column`, whose positional branch treats a bare integer as a 1-based
output-column index. `HAVING` has NO positional semantics, so `HAVING 1 > 5` was misread as
`region > 5` → silently returned ALL groups (should be empty); `HAVING 1 = 1` → `region == 1` → dropped
all groups; `HAVING 2 <= SUM(amount)` → mis-resolved + 449 over-rejection (while `100 < SUM(amount)`
worked — inconsistent). Silent-wrong → violates invariant 1.

**Fix:** `resolve_grouped_having_comparison` now extracts the LITERAL side first (via
`grouped_having_literal`) and resolves the OTHER side via the new `resolve_grouped_having_column`, which
rejects bare literals (`Expr::Value`, `UnaryOp(Minus)`) before delegating — so the positional branch can
never fire for `HAVING`. Constant-only predicates (`1>5`, `1=1`) now → `None` → 449 loud error (safe
over-rejection); `2 <= SUM(amount)` correctly flips to `SUM >= 2`. Regression tests added for all of
these. (A focused single-agent adversarial re-verification of the fix followed.)

## Verified
- lib 358 (planner: `group_aggregate_fanout_resolves_having` — resolution, literal-on-either-side flip,
  small-literal-not-positional regression, OR/non-column/non-literal/constant-only → None;
  `apply_grouped_post_merge_filters_having_before_order_limit` — HAVING-then-order-then-limit + NULL
  drop; HTTP integration `http_query_fanouts_group_aggregate_having_global_filter` — global filter where
  a group is below threshold per-owner but above globally, plus HAVING+ORDER+LIMIT combo),
  pg_integration 38 (the 2-node simple+extended top-k test extended with a HAVING-on-global-sum
  assertion), sql_group_aggregate 50. fmt clean; no new clippy warnings.

## Family complete
All three grouped variants (`COUNT(*)`, `SUM/MIN/MAX`, `AVG`) now support `HAVING` + `ORDER BY` /
`LIMIT` / `OFFSET` post-merge across all 4 entry points. Remaining P5-3: mixed/multi-owner writes,
distributed index ownership, broader cross-node query planning.
