# BENCHPROD-448 Execution Context
Added grouped `AVG(col)` shard-owner fan-out, completing the grouped-aggregate family (446 COUNT(*),
447 SUM/MIN/MAX). AVG is not directly mergeable, so it reuses the scalar-AVG `rewritten_sql` trick:
the extractor `group_avg_select_fanout_target` rewrites the AVG projection slot in place to
`SUM(arg), COUNT(arg)` (COUNT of the argument, matching AVG's non-null denominator) while preserving
FROM/WHERE/GROUP BY verbatim. Because the AVG slot expands to two columns, group-column indices after
it shift +1 in the rewritten result; sum_index=avg_index, count_index=avg_index+1,
avg_output_index=avg_index. Each owner runs the rewritten query; the merge re-groups on the composite
canonical-JSON key, adds partial sums (FanoutSum int/float ladder) and non-null counts (checked_add),
then divides (sum/count, NULL when count<=0), rebuilding rows in the ORIGINAL projection layout. Covered
all 4 entry points (HTTP query/execute via accumulate_fanout_group_avg+group_avg_rows; pgwire
simple/extended via accumulate_forward_group_avg+forward_group_avg_rows), dispatched right after the
447 grouped-aggregate handler. Numeric type guard on the AVG column mirrors 447.

Verified: lib 346 (planner unit group_avg_select_fanout_plan_matches_avg_shapes covering AVG-first/last/
mid + multi-col + ineligibility; 2-node cross-owner integration http_query_fanouts_group_avg_across_shard_owners
with group 'a' AVG=(10 local + 50 remote)/2=30.0), sql_group_aggregate 50, fmt/diff-check clean. A 5-dimension
adversarial review workflow (merge-math, index-mapping, sql-rewrite, dispatch-eligibility, cross-layer-parity;
find+independently-verify, 295k tokens) returned 0 confirmed findings. No single-node bench delta
(distributed-only path).
