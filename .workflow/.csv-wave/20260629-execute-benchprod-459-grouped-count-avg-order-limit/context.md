# BENCHPROD-459 Execution Context

Extends the 450/451 distributed grouped `ORDER BY` / `LIMIT` / `OFFSET` post-merge top-N from the
`SUM/MIN/MAX(col)` variant to the remaining two grouped fan-out variants — `COUNT(*)` and `AVG(col)` —
completing ORDER BY/LIMIT support across all three grouped aggregates. Reuses the 450 shared infra
(`GroupedPostMerge`, `apply_grouped_order_limit`, `compare_grouped_order_values`,
`strip_grouped_post_merge_clauses`). Distributed-only (no single-node bench delta); converts queries
449 currently errors on into correct distributed results.

## Refactor (450 infra)
`resolve_grouped_post_merge` → `resolve_grouped_order_limit(query, projection, output_columns) ->
Option<(Vec<GroupedOrderKey>, Option<usize>, usize)>`. It no longer builds `per_owner_sql`; each
variant constructs its own `GroupedPostMerge` because the per-owner SQL differs:
- group_count / group_aggregate: `per_owner_sql = strip_grouped_post_merge_clauses(query)` (the
  ORIGINAL query with ORDER BY/LIMIT/OFFSET/FETCH/inner-HAVING stripped).
- group_avg: `per_owner_sql = rewritten_sql.clone()` — AVG owners ALWAYS run the rewritten `SUM,COUNT`
  query, which is reconstructed from FROM/WHERE/GROUP BY parts and is therefore already clause-free.

## Variant wiring
- `SqlShardGroupCountFanoutPlan` and `SqlShardGroupAvgFanoutPlan` each gain `post_merge:
  Option<GroupedPostMerge>`.
- `group_count_select_fanout_plan`: guard now rejects only `with`/`fetch` (was `with`/`order_by`/
  `limit_clause`); resolves post_merge at the end.
- `group_avg_select_fanout_target`: same guard relaxation; resolves post_merge after building
  `rewritten_sql`. Order keys resolve against `output_columns` = the ORIGINAL projection layout, which
  is exactly the layout of the rebuilt AVG rows (`group_avg_rows` / `forward_group_avg_rows`), so the
  `col_index` values index the final rows correctly. (The `group_indices` +1 shift for the rewritten
  layout does NOT apply to order_keys — they index the final/original layout.)
- Eight handlers wired (4 per variant): HTTP `/query` + `/execute`, pgwire simple + extended.
  - count handlers: switch local + every remote owner exec to `per_owner_sql` (the `/execute` path
    swaps the prepared helpers on `post_merge.is_some()`), then `apply_grouped_order_limit`.
  - avg handlers: already ran `rewritten_sql` per owner — only added the post-merge `apply` step.

## Safety / atomicity (unchanged invariants)
- Unresolvable ORDER/LIMIT (or `FETCH FIRST n ROWS`, ORDER BY ALL, WITH FILL, INTERPOLATE, LIMIT BY,
  MySQL `LIMIT a,b`, non-literal limit/offset, out-of-range positional, unknown column) ⇒ extractor
  `None` ⇒ 449 loud error. `FETCH` rejected for count + avg too (carrying the 451 regression guard).
- After 459 all three grouped variants support ORDER BY/LIMIT atomically across all 4 entry points;
  grouped `HAVING` remains the only open grouped post-merge clause (452).

## Adversarial review
Find→independently-verify workflow, 4 dimensions (avg-correctness, count-correctness,
refactor-regression, safety-and-dispatch), 4 agents, ~346k tokens: **0 findings**. The reuse of the
already-reviewed 451 infra + the symmetric wiring left no new gaps; the avg-specific subtleties
(per_owner_sql = rewritten_sql; order_keys index the final not the rewritten layout) verified correct.

## Verified
- lib 355 (planner: `group_count_select_fanout_plan_matches_group_count_shapes` and
  `group_avg_select_fanout_plan_matches_avg_shapes` extended with ORDER BY/LIMIT/OFFSET resolution +
  FETCH-rejection; integration `http_query_fanouts_group_count_order_by_limit_global_top_k` and
  `http_query_fanouts_group_avg_order_by_limit_global_top_k`), pg_integration 38, sql_group_aggregate 50.
  fmt clean; no new clippy warnings.
- Strong discriminators: count test — local a×1 (one row a buggy per-owner LIMIT 1 would drop) so
  global top-1 a=4 vs buggy 3; avg test — local a=10, remote a=40,40 → global AVG a=30 (only the
  partial-sum/count post-merge division gives the correct order).

## Deferred
- 452 grouped `HAVING` post-merge (the strip already nulls inner HAVING; needs a HAVING predicate
  resolver + JSON predicate evaluator applied before ORDER/LIMIT).
