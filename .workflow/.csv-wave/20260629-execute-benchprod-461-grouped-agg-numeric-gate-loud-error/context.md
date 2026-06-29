# BENCHPROD-461 Execution Context

Uniformly close the silent-wrong gap the 460 adversarial review surfaced: a grouped single-aggregate
query whose `SUM`/`MIN`/`MAX`/`AVG` argument column is NON-numeric matched the planner's SHAPE, so the
449 safety net short-circuited it as "supported" (`group_aggregate_select_fanout_target(...).is_some()`
/ `group_avg_...is_some()`), but the dispatcher then declined (empty owners from the numeric check), so
a genuinely-scattering query fell through to LOCAL-ONLY execution and silently returned only the local
owner's groups. (460 already fixed this for the multi-aggregate planner; 461 extends the same fix to the
single-aggregate planners.)

## Fix
In `shard_unsupported_group_by_fanout_error_for_statements` (src/execution/mod.rs), the step-2
"supported" short-circuit now lists ONLY `group_count_select_fanout_plan` — the one planner with no
column-type requirement, so its structural match always means it can fan out. The type-gated planners
(`group_aggregate`, `group_avg`, and `group_multi_aggregate`) are NOT short-circuited; a
structurally-matching-but-type-ineligible query falls through to the generic scatter check, which fails
LOUDLY for a multi-owner query instead of returning silently-incomplete results.

Safety (same reasoning verified for the 460 fix): an ELIGIBLE (numeric) scattering aggregate is answered
by its own dispatcher BEFORE the 449 net is reached (the dispatcher returns owners → `Some`), so this
never produces a false error for a supported query. The net is only reached when every dispatcher
returned `None` (empty owners = pinned single-owner → scatter check also empty → local; or type-
ineligible + scattering → scatter check non-empty → loud error). The HTTP/pgwire `try_unsupported`
wrappers guard `shard_forwarded` / forwarding-disabled, so forwarded sub-queries never reach the net.

## Verified
- lib 360 / pg_integration 38 / sql_group_aggregate 50, all green; fmt clean.
- Regression assertions added to `http_query_rejects_unsupported_multiowner_group_by`: a non-numeric
  SINGLE aggregate (`MAX(grp)`, `SUM(grp)`, `AVG(grp)` over a TEXT column, 2 owners) now returns
  BAD_REQUEST (was silently local-only); the multi-agg `MAX(grp)` case from 460 still errors; supported
  numeric single- and multi-aggregate shapes still succeed (no false positive).

## Note / next
This makes `SUM`/`MIN`/`MAX`/`AVG` over a `DECIMAL`/`NUMERIC` column LOUD-error across owners (the
numeric gate currently accepts only integer/float — `DECIMAL` is stored as a JSON string and the
fan-out accumulators only merge JSON numbers). That is SAFE (loud, not silent-wrong) but rejects a
legitimate query. **BENCHPROD-462** adds `DECIMAL`/`NUMERIC` support (decimal-aware accumulator:
DECIMAL is f64-backed via `Value::decimal_from_f64`, so the merge sums as f64 and finalizes via the
same normalization; MIN/MAX compare numerically and return the decimal string) so those fan out
correctly instead of erroring.
