# BENCHPROD-450/451 Execution Context

Distributed grouped `ORDER BY` / `LIMIT` / `OFFSET` post-merge top-N for the grouped `SUM/MIN/MAX(col)`
fan-out variant. Builds on the grouped-aggregate fan-out family (446 COUNT(*), 447 SUM/MIN/MAX, 448 AVG)
and the 449 safety net. This is distributed-only — no single-node benchmark delta; it converts queries
that 449 currently errors on into correct distributed results, for one variant, atomically across all
four entry points.

450 (shared infra) and 451 (the group_aggregate consumer) landed together in one feat commit: the infra
has no other consumer yet, so committing it alone would be dead code. The remaining grouped variants
(`COUNT(*)`, `AVG`) ORDER BY/LIMIT and grouped `HAVING` (452) are deferred follow-ups that will reuse
this same infra.

## Why per-owner stripping (the core invariant)
A group spans owners, so an owner's local `LIMIT`/`OFFSET` is computed on its PARTIAL groups — local
top-N ≠ global top-N. Owners therefore MUST return ALL groups (clauses stripped); `ORDER BY`/`LIMIT`/
`OFFSET` are applied exactly once, post-merge, on the globally combined rows. Owner ROUTING still uses
the original SQL (ORDER BY/LIMIT do not change which owners a query scatters to).

## 450 — shared infra (src/execution/mod.rs)
- `GroupedOrderKey { col_index, asc, nulls_first }` — one resolved ORDER BY key into the OUTPUT row
  layout.
- `GroupedPostMerge { per_owner_sql, order_keys, limit, offset }` — carried on the plan as
  `post_merge: Option<_>` (`None` = plain query, owners run the original SQL).
- `apply_grouped_order_limit(&mut rows, spec)` — shared pub(crate) free fn: global stable `sort_by`
  over `order_keys`, then `offset` drain + `limit` truncate. Called by all four handlers so HTTP and
  pgwire apply identical semantics.
- `compare_grouped_order_values` / `compare_grouped_non_null` — SQL ordering on `serde_json::Value`:
  NULL placement absolute (`nulls_first`; default NULLS LAST for ASC, NULLS FIRST for DESC; DESC
  reverses values but NOT null placement); i64 → u64 → f64 numeric ladder; lexical strings; bool;
  total/panic-free type-rank fallback for mixed columns.
- `strip_grouped_post_merge_clauses(query)` — clone, null `order_by` + `limit_clause` + `fetch` +
  inner `having`, `.to_string()`.

## 451 — group_aggregate ORDER BY/LIMIT (src/execution/mod.rs + both servers)
- Relaxed `group_aggregate_select_fanout_target`: now accepts `order_by`/`limit_clause`; rejects
  `with` and `fetch`. After computing output_columns it calls `resolve_grouped_post_merge`, which:
  resolves each ORDER BY expr to an output column index (positional `ORDER BY 2`, bare identifier
  matching an output name/alias, or a projection expression e.g. `SUM(amount)`), captures
  `LIMIT`/`OFFSET` numeric literals; ANY unresolvable/unsupported part (ORDER BY ALL, WITH FILL,
  INTERPOLATE, LIMIT BY, MySQL `LIMIT a,b`, non-literal limit/offset, out-of-range positional,
  unknown column) ⇒ `None` ⇒ the 449 net errors loudly. Over-rejection is deliberate.
- Threaded `per_owner_sql` through all 4 entry points (local exec + every remote owner exec):
  - HTTP `/query` `try_fanout_group_aggregate_query_to_shard_owners`
  - HTTP `/execute` `try_fanout_group_aggregate_execute_to_shard_owners` (switches the prepared
    helpers `execute_sql_locally_for_fanout` / `query_remote_prepared_sql_shard_owner` on
    `post_merge.is_some()`)
  - pgwire simple `fanout_group_aggregate_select_to_shard_owners`
  - pgwire extended `fanout_extended_group_aggregate_select_to_shard_owners`
  Each builds the merged rows then calls `apply_grouped_order_limit` when `post_merge` is `Some`.

## Atomicity / safety
- Only `group_aggregate` is relaxed; `group_count` and `group_avg` extractors STILL reject
  ORDER BY/LIMIT, so `COUNT(*)`/`AVG` + GROUP BY + ORDER BY stay loud 449 errors (verified in the
  ineligible unit-test block). Relaxing the shared extractor without wiring all 4 consumers would be
  silently wrong, so the variant landed all-or-nothing.

## Adversarial review (find→independently-verify workflow, 8 agents, ~591k tokens)
Caught ONE real, shipped-blocking regression (high confidence): **FETCH FIRST n ROWS**. sqlparser 0.60
stores `FETCH { FIRST|NEXT } <n> ROWS` in a SEPARATE `Query.fetch` field (not `limit_clause`). A query
like `... GROUP BY region ORDER BY SUM(amount) DESC FETCH FIRST 5 ROWS ONLY` was ACCEPTED (order_by
present), but `strip` left `fetch` intact and `post_merge.limit` was `None` → the global row limit was
silently dropped (returns all groups instead of top-5). Pre-451 this shape errored loudly via 449 →
silent-wrong regression of invariant 1. Fixed by rejecting `query.fetch.is_some()` in the extractor
guard (over-rejection → loud 449 error) + nulling `fetch` in `strip` (defense-in-depth for 452) +
two rejection test cases. The other 2 findings were refuted on independent verification.

## Verified
- lib 353 (planner: `group_aggregate_fanout_resolves_order_by_limit` resolution + ineligibility incl.
  FETCH; `apply_grouped_order_limit_sorts_slices_and_orders_nulls` NULLS/offset-past-end; integration
  `http_query_fanouts_group_aggregate_order_by_limit_global_top_k`), pg_integration 38
  (`test_pg_grouped_aggregate_order_by_limit_global_top_k_across_owners`: 2-node simple + extended,
  global top-2 ordered + OFFSET), sql_group_aggregate 50. fmt clean; no new clippy warnings.
- Strong discriminator in both integration tests: local group 'a' partial = 5, remote = 100 → global
  top group is 'a' (105); a buggy per-owner LIMIT would drop 'a' locally → exact-value assertions
  (105 / 21) would fail.

## Deferred (next, reusing this infra)
- COUNT(*) + AVG grouped ORDER BY/LIMIT (relax group_count / group_avg extractors).
- 452 grouped HAVING post-merge (strip already nulls inner HAVING; add resolver + predicate eval).
