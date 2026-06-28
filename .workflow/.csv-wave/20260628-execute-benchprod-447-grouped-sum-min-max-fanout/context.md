# BENCHPROD-447 Execution Context
Extended BENCHPROD-446's grouped `COUNT(*)` shard-owner fan-out to grouped `SUM/MIN/MAX(col)`.
Planner: new `SqlShardGroupAggregateKind{Sum,Min,Max}` + `SqlShardGroupAggregateFanoutPlan{group_indices,
agg_index, kind, output_columns}`; extractor mirrors group_count but matches `AGG(col)` (one aggregate,
N group cols == projection minus the aggregate, bijection check) and resolves kind from the function
name — `COUNT(*)` and `AVG` deliberately rejected (AVG deferred to 448). Owner-resolution adds a numeric
type guard. Merge: per-group accumulator — SUM reuses the FanoutSum int/float ladder, MIN/MAX reuse the
extremum-merge + canonical-JSON compare helpers from scalar fan-out; group key is the canonical JSON
array of the group tuple (NULL preserved), rows scattered back into projection order. Covered all 4
entry points (HTTP query/execute via GroupAggAcc; pgwire simple/extended via ForwardGroupAcc). Designed
by the batch-design workflow. Verified: lib 344 (incl. planner unit SUM/MIN/MAX+multi-col+ineligibility
and 2-node cross-owner integration SUM=40/MAX=30), sql_group_aggregate 50, fmt/diff-check clean. No
single-node bench delta (distributed-only path).
