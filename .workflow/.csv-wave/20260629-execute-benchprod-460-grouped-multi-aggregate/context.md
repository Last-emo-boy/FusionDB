# BENCHPROD-460 Execution Context

Distributed grouped MULTI-AGGREGATE fan-out: `SELECT g1[, ...], AGG1, AGG2[, ...] FROM t [WHERE ...]
GROUP BY g1[, ...]` where each AGG is `COUNT(*)` / `COUNT(col)` / `SUM(col)` / `MIN(col)` / `MAX(col)`
(two or more aggregates, or shapes the single-aggregate planners don't cover, e.g. a lone `COUNT(col)`).
A very common analytics shape — the benchmark's "Fulfillment backlog" (`status, COUNT(*), SUM(total)
GROUP BY status`) is exactly this, and multi-node it previously hit the 449 net. `AVG` and `DISTINCT`
aggregates are NOT supported here (they need a rewrite / value-set merge) → extractor `None` → 449 loud
error. Distributed-only (no single-node bench delta).

## Design (reuses the grouped fan-out family + post-merge infra)
- `COUNT(*)`, `COUNT(col)`, `SUM(col)` all merge by ADDING partials (kind `Sum`); `MIN`/`MAX` by
  extremum. So the plan stores `aggregates: Vec<GroupMultiAggregate { output_index, kind }>` (COUNT →
  `Sum`). Owners run the ORIGINAL query (all directly mergeable — no rewrite, unlike AVG).
- `src/execution/mod.rs`: `GroupMultiAggregate` + `SqlShardGroupMultiAggregateFanoutPlan`;
  `group_multi_aggregate_select_fanout_target` (returns `(table, numeric_required_columns, plan)`);
  `shard_group_multi_aggregate_select_fanout_{plan,owners}_for_{sql,statements}` — the owners check
  validates each `SUM`/`MIN`/`MAX` arg column is integer/float. Dispatched AFTER the single-aggregate
  planners, so single `COUNT(*)`/`SUM`/`MIN`/`MAX`/`AVG` keep their existing tested paths; this catches
  the multi/mixed shapes (and a lone `COUNT(col)`).
- `src/server/http_server.rs`: `accumulate_fanout_group_multi_aggregates` + `group_multi_aggregate_rows`
  — per-group `Vec<GroupAggAcc>`, one accumulator per aggregate, each reduced independently at its
  `output_index`. `try_fanout_group_multi_aggregate_query` + `_execute` handlers; dispatched after
  group_avg.
- `src/server/pg_server.rs`: `ForwardGroupAcc::new`; `accumulate_forward_group_multi_aggregates` +
  `forward_group_multi_aggregate_rows`; `fanout_group_multi_aggregate_select` + `_extended` handlers;
  dispatched after group_avg (simple + extended).
- Post-merge `HAVING` / `ORDER BY` / `LIMIT` / `OFFSET` work unchanged (shared `apply_grouped_order_limit`).

## Adversarial review caught a real regression (high confidence, 2 finders converged)
**Non-numeric `SUM`/`MIN`/`MAX` column → silent local-only (loud→silent regression).** The extractor
matches on SHAPE only (no type check — it has no schema access); the numeric validation lives in the
owners function (returns empty owners for a non-numeric column). The 449 net's step-2 short-circuit
trusted `group_multi_aggregate_select_fanout_target(...).is_some()` as "supported" and suppressed its
loud error; the handler then declined (empty owners) → the query fell through to local-only and silently
returned just the local owner's groups. Pre-460 a 2-aggregate projection matched no planner, so 449
fired loudly — so 460 turned a loud error into a silent wrong answer (violates "never silently wrong").

**Fix:** removed `group_multi_aggregate_select_fanout_target(...).is_some()` from the 449 net's step-2
short-circuit. A non-numeric multi-agg query is no longer counted as "supported", so it reaches the
generic scatter check (step 4); a genuinely-scattering one now errors LOUDLY instead of running
local-only. Traced safe: a valid (numeric) multi-agg query is handled by its own handler BEFORE the 449
net is reached (handler returns `Some`); the try_unsupported wrapper guards `shard_forwarded` /
forwarding-disabled (so forwarded sub-queries never reach the net); a pinned single-owner query gives an
empty scatter set → no error. Regression test added (`MAX(grp)` over a TEXT column, 2 owners →
BAD_REQUEST). A focused single-agent adversarial re-verification of the fix followed.

**Known pre-existing limitation (NOT introduced here, left untouched):** the single-aggregate planners
(`group_aggregate`/`group_avg`) share the same structural-`is_some()` step-2 short-circuit, so a single
`SUM`/`MIN`/`MAX`/`AVG` over a non-numeric column is silently local-only multi-owner; and the numeric
check omits `DECIMAL`/`NUMERIC` (so `SUM` over `NUMERIC` is rejected/silent across all variants). Both
are pre-existing and warrant a separate follow-up (uniformly route structurally-matched-but-ineligible
shapes to the loud net, and add `is_decimal_type_name` to the numeric gate).

## Verified
- lib 360 (planner: `group_multi_aggregate_select_fanout_plan_matches_shapes` — resolution incl.
  COUNT(*)/COUNT(col)/SUM/MIN/MAX, mixed, multi-column, interleaved + ineligibility AVG/DISTINCT/no-GROUP
  BY/non-group-non-agg; integration `http_query_fanouts_group_multi_aggregate_across_shard_owners` —
  COUNT(*)+SUM merged independently per global group + HAVING/ORDER combo; the 449 test now uses
  COUNT(DISTINCT) as its unsupported case and asserts the non-numeric multi-agg loud error),
  pg_integration 38 (the 2-node test extended with a multi-aggregate simple-query assertion),
  sql_group_aggregate 50. fmt clean; clippy: only the pre-existing let-else style lint shared with the
  existing single-agg execute handlers.
- Strong discriminator: group 'a' spans owners with COUNT 1 (local) + 2 (remote) = 3 and SUM 10 + 35 =
  45 — each aggregate merges independently and correctly.

## Remaining P5-3
mixed/multi-owner writes, distributed index ownership, broader cross-node query planning (JOINs);
follow-up: uniform numeric-gate / decimal handling for the single-aggregate silent-local-only gap.
