# BENCHPROD-445 Execution Context

## Outcome
P5-3 distributed iteration: distributed `SELECT col, COUNT(*) FROM t [WHERE ...] GROUP BY col`
shard-owner fan-out — the first GROUPED aggregate fan-out (after BENCHPROD-436's scalar family).
Chosen after a workflow confirmed the single-node ~238ms full-scan floor is storage-read bound
(not decode-bound — BENCHPROD-440's parallel decode didn't move it), so the clean floor wins are
negligible (block clone is per-block, ~2ms) and the magnitude win is medium-risk/measurement-gated;
this distributed increment is higher value-per-risk and fixes a real correctness bug.

## Bug fixed
Before this, a multi-node `GROUP BY col, COUNT(*)` matched NO fan-out eligibility (the scalar
COUNT(*) path requires an empty GROUP BY), so it silently executed local-only and returned an
INCOMPLETE grouped result across shards. Now it fans out and merges correctly.

## Implementation
- `SqlShardGroupCountFanoutPlan { group_index, count_index, output_columns }` + `group_count_select_fanout_plan`
  (`src/execution/mod.rs`): eligibility = single table, single-column `GROUP BY` (identifier), projection
  of exactly {COUNT(*), the group identifier} in either order, no DISTINCT/HAVING/ORDER BY/LIMIT/joins,
  fan-out-local WHERE. No SQL rewrite — each owner runs the original query.
- Merge: accumulate per-owner `(group_value, count)` rows into a `BTreeMap` keyed by the canonical JSON
  of the group value (so NULL is its own group, distinct from the distinct-aggregate helper which skips
  NULL), summing counts (checked); emit one row per group in projection column order, sorted by group
  key for determinism (GROUP BY without ORDER BY is unordered).
- Covered all 4 entry points: HTTP `/query` + `/execute` (`try_fanout_group_count_*`, `accumulate_fanout_group_counts`,
  `group_count_rows`), pgwire simple + extended (`fanout_[extended_]group_count_select_to_shard_owners`,
  `accumulate_forward_group_counts`, `forward_group_count_rows`). Eligibility is mutually exclusive with
  the scalar paths (projection length 2 + non-empty GROUP BY), so dispatch ordering is safe.

## Verification
- `cargo fmt --check`, `git diff --check`, `cargo check --bins` passed.
- `cargo test --lib` passed, incl. new `group_count_select_fanout_plan_matches_group_count_shapes`
  (planner) and `http_query_fanouts_group_count_across_shard_owners` (2-node integration: group 'a'
  spans both owners -> merged count 2, plus single-owner groups 'b','c').
- `cargo test --test sql_group_aggregate` (single-node GROUP BY regression) and
  `cargo test --test pg_integration shard_owner` passed.

## Remaining (P5-3)
- Grouped SUM/MIN/MAX/AVG fan-out (next: 446+, mirror 432-434 over groups), multi-column GROUP BY,
  HAVING, grouped ORDER BY/LIMIT, distributed joins/subqueries/set-ops.
- Single-node: the ~238ms full-scan floor (storage-read bound) — parallel range-merge is the
  magnitude lever (medium-risk, measurement-gated).
