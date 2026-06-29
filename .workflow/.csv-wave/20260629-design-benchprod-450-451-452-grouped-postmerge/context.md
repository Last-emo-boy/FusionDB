# BENCHPROD-450/451/452 Design — distributed grouped ORDER BY/LIMIT/HAVING (post-merge)

Running-start design for a FRESH session. Builds on the grouped-aggregate fan-out family (446 COUNT(*),
447 SUM/MIN/MAX, 448 AVG) and the safety net (449). These are distributed-only (no single-node bench
delta); they convert queries that 449 currently errors on into correct distributed results.

## The problem & the invariant
Today the three grouped extractors hard-reject `ORDER BY` / `LIMIT` / `OFFSET` / `HAVING`:
- `group_count_select_fanout_plan` (src/execution/mod.rs ~1507)
- `group_aggregate_select_fanout_target` (~1596)
- `group_avg_select_fanout_target` (~1700)
Each rejects at `query.with.is_some() || query.order_by.is_some() || query.limit_clause.is_some()` and
`select.having.is_some()`. A multi-owner grouped query with those clauses therefore hits the 449 safety
net and errors loudly.

**Why per-owner stripping is mandatory:** an owner's local `LIMIT`/`HAVING` is computed on its partial
groups, but a group spans owners — local top-N ≠ global top-N, and a group below a HAVING threshold
locally may be above it globally. So owners MUST return ALL groups (clauses stripped); ORDER/LIMIT/
HAVING are applied once, post-merge.

**Safety invariant (carry forward from 449):** any ORDER BY/HAVING shape the resolver cannot map to an
output column / supported predicate ⇒ the extractor returns `None` ⇒ the 449 net errors loudly. Never
silently wrong. Over-reject freely.

**Atomicity:** the extractors are shared across all 4 entry points (HTTP /query + /execute, pgwire
simple + extended). Relaxing an extractor without updating ALL of that variant's consumers returns
silently-wrong (unsorted/unfiltered) results. So land ONE grouped variant's FULL 4-entry-point support
(+ tests + adversarial review) per atomic commit. Suggested order: group_aggregate ORDER BY/LIMIT →
group_count → group_avg → then HAVING across all three.

## 450 — shared infra
1. **Per-owner clause strip** (execution/mod.rs): given the single grouped `Query`, produce the SQL
   with `order_by=None`, `limit_clause=None`, and the inner `Select.having=None`, via clone + `.to_string()`.
   For group_avg, fold the strip into its existing `rewritten_sql` (it already rewrites AVG→SUM,COUNT).
   For group_count/group_aggregate (currently sent verbatim) the plan gains a `per_owner_sql: Option<String>`
   used only when post-merge clauses are present; else keep sending the original query.
2. **SQL-semantics JSON comparator** for `serde_json::Value` rows: numbers (int/float) numerically,
   strings lexically, NULL ordering (default NULLS LAST for ASC, FIRST for DESC unless specified), per
   `OrderKey { col_index, asc, nulls_first }`. Mirror in http_server + pg_server (same pattern as the
   duplicated `accumulate_fanout_group_*` / `merge_fanout_extremum`), OR a shared `pub(crate)` free fn.
3. **Post-merge spec** carried on each grouped plan: `GroupedPostMerge { order_keys: Vec<OrderKey>,
   limit: Option<usize>, offset: usize, having: Option<HavingPredicate> }` (all optional pieces).

## 451 — ORDER BY / LIMIT post-merge top-N
- Relax the 3 extractors to accept `order_by`/`limit_clause`; resolve each ORDER BY expr to an OUTPUT
  column index: positional (`ORDER BY 2`), a group-column name/alias, or the aggregate's output
  name/expr (e.g. `ORDER BY SUM(amount)` → the agg column). Unresolvable ⇒ `None`. Capture limit/offset.
- Per-owner exec uses the stripped SQL (all groups). Merge unchanged.
- After building the merged `rows: Vec<Vec<serde_json::Value>>`, apply post-merge: stable sort by
  `order_keys` (group cardinality is small, a full `sort_by` is fine; can use select_nth top-N later),
  then `offset`/`limit`. A shared `apply_grouped_order_limit(rows, spec)` per server.
- Wire into all 4 handlers of the variant (use per_owner_sql + call apply_grouped_order_limit).

## 452 — HAVING post-merge
- Relax extractors to accept `select.having`. Resolve the HAVING predicate against output columns:
  scope to `<agg-or-group-col> <cmp> <literal>` and AND-combinations (reuse the shape of
  `is_cacheable_predicate_expr` / the 449 pin walkers). Unresolvable ⇒ `None`.
- Per-owner strip HAVING (owners return all groups). Post-merge: filter merged rows by evaluating the
  predicate on each row's values (a small JSON predicate evaluator: read the referenced output column,
  compare to the literal). Apply HAVING BEFORE ORDER/LIMIT.

## Tests (per variant)
- 2-node integration (mirror `http_query_fanouts_group_aggregate_across_shard_owners`): a group spanning
  both owners; `ORDER BY SUM(x) DESC LIMIT k` returns the correct GLOBAL top-k (not per-owner); `HAVING
  SUM(x) > t` filters on the GLOBAL sum.
- Planner unit: resolution (positional / group-col / agg-expr; ASC/DESC; offset) + ineligibility
  (unresolvable ORDER key, non-simple HAVING) ⇒ None.
- Adversarial review workflow (find+verify): NULL ordering, ties/stability, OFFSET past end, post-merge
  applied at all 4 entry points, the 449-fallback for unresolvable shapes, HAVING on a group present on
  only one owner.

## Status
Design only; no code. Next session implements per the atomic-per-variant plan. See [[benchprod-campaign]].
