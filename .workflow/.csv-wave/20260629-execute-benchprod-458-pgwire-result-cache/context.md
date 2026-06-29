# BENCHPROD-458 Execution Context — pgwire query-result-cache parity

Surfaced by the BENCH_PROTO=pg A/B (the pgwire benchmark path added just before this): repeated grouped
aggregates were ~8.7 ms over pgwire but ~0.4 ms over HTTP, because the executor's grouped-aggregate
result cache (in `execute_sql`) was only on the HTTP `/query` path; pgwire's autocommit local execution
(`execute_first_statement` extended path, and the simple-query handler) bypassed it.

## Change
- `Executor::is_query_result_cacheable_statement(stmt)` (pub(crate) wrapper of the two existing
  `is_cacheable_*_group_aggregate_statement` predicates) + `Executor::execute_cached_select(stmt)`
  (mirrors the cache path in `execute_sql`: key = `query_result_cache_key(stmt.to_string())`, epoch
  check, return cached SELECT or `execute(stmt)` + insert).
- pgwire `execute_first_statement`: in the autocommit branch, `if params.is_empty() &&
  is_query_result_cacheable_statement(stmt) -> execute_cached_select(stmt)`. Guards: only outside an
  explicit transaction (the other branch runs in the session txn), and only with no bind params (the
  cache keys on statement text, not bound values).
- pgwire simple-query handler: autocommit arm routes cacheable statements to execute_cached_select.
- Correctness rests on `execute()` already calling `invalidate_query_result_cache()` (epoch bump) on
  any write, so cached reads never go stale.

## Predicate hardening (bundled — fixes a pre-existing bug the review surfaced)
The adversarial review confirmed a medium pre-existing bug in the *shared* cacheable predicate:
`is_cacheable_join_group_aggregate_statement` matched the JOIN `ON(_)` without inspecting it, so a
volatile function in ON (`... ON u.id = e.user_id AND e.ts > NOW()`) was cached and frozen until an
unrelated write — already broken on HTTP, and 458 would widen it to pgwire. Fixed by extracting the ON
expression and requiring `is_cacheable_join_on_expr` (AND-combined comparisons over columns/literals
only; rejects any function). Closes the volatility hole for BOTH paths; common `a.id = b.id` joins stay
cached.

## Verification
- pg_integration `test_pg_grouped_aggregate_cache_consistent_and_invalidated_by_writes`: cached result
  equals fresh compute, and a write makes the next read reflect it (no stale).
- lib unit `join_group_aggregate_cacheability_rejects_volatile_on_predicate`: plain/compound ON
  cacheable, `NOW()` in ON not.
- Full gate: lib + pg_integration + sql_group_aggregate, fmt/diff clean.
- Benchmark A/B (large, pgwire): GROUP BY category 8.7 -> 0.1 ms (17.5k ops/s), the whole Analytics
  family (Category avg, Event counts, GROUP BY SUM WHERE) -> ~0.1 ms — now matching HTTP. GROUP BY +
  HAVING correctly NOT cached (has HAVING). COUNT(*) unchanged (not a GROUP BY).
- Adversarial review (3 dims: staleness, txn/params, scope-parity; find+verify): 458's own new logic
  clean (0); the one confirmed finding was the pre-existing ON-predicate hole, fixed above.

Cross-protocol consistency reached: pgwire and HTTP now share grouped-aggregate caching semantics.
