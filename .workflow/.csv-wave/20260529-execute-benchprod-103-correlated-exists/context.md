# BENCHPROD-103: correlated EXISTS / NOT EXISTS support

## Purpose

Continue LDBC native readiness after BENCHPROD-102 by addressing the Query 14 blocker `Column k_person2id not found` in `not exists(select * from sg y where y.link = k_person2id)`.

## Changes

- Updated `src/execution/expr/subquery.rs` so `EXISTS` / `NOT EXISTS` predicates are no longer globally pre-materialized into constants.
- Added a deferred subquery filter path for predicates containing `EXISTS`.
- Added row-aware binding for simple correlated `EXISTS` / `NOT EXISTS` subqueries:
  - Builds local subquery scope from subquery `FROM` relations.
  - Leaves local subquery columns intact.
  - Replaces outer-column references with the current outer row value.
  - Executes the bound subquery per outer row.
- Updated `src/execution/query/mod.rs` to split eager predicates from deferred `EXISTS` predicates.
- Applied deferred filters after scan/join row construction and before grouping, ordering, and limit.
- Disabled scan limit/projection optimizations when a deferred subquery filter is present so outer columns and candidate rows are not dropped early.
- Added regression tests in `tests/sql_set_subquery.rs`:
  - `test_correlated_not_exists_against_cte`
  - `test_correlated_not_exists_with_join_alias_shape`
  - `test_correlated_not_exists_filters_before_limit`

## Evidence

- `cargo fmt --check`: passed.
- `cargo test --test sql_set_subquery`: passed, `18/18`.
- `cargo test --test sql_join test_comma_join_projection_keeps_right_columns_without_join_pushdown`: passed.
- `cargo build --release --bin fusiondb`: passed.
- `cargo test --release --test pg_integration`: passed, `25/25`.
- LDBC non-isolation smoke:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod103_correlated_exists_frontier_20rows_10ops_20260529\ldbc_snb_native_smoke_summary.json`
  - `status=gap`, `steps=7/8`.
  - The previous `Column k_person2id not found` blocker is gone from server logs.
  - Workload reached Q14 and timed out after 180 seconds with `Operations [1]`.

## Result

BENCHPROD-103 does not make LDBC pass. It implements a constrained correlated `EXISTS` / `NOT EXISTS` path sufficient to move past the previous outer-column resolution blocker. The new frontier is Q14 runtime/progression: the native workload now reaches Q14 and times out rather than failing immediately on `k_person2id`.

## Current Blockers

- Q14 likely needs targeted profiling because naive per-row correlated `EXISTS` inside recursive CTE expansion can be very expensive.
- Query 14 still contains SQL features that are not fully proven in FusionDB: array concatenation, multidimensional array indexing, `generate_subscripts`, `row_number() OVER ()` interaction, and weighted path aggregation.
- The LDBC report is non-isolation evidence, but it remains `gap`; it is not a full native pass.
- Native memtier remains blocked by missing real `memtier_benchmark` probe.

## Next Task Candidate

BENCHPROD-104 should isolate the Q14 timeout with a bounded targeted SQL harness and server-side timing, then decide whether the next production-readiness step is recursive frontier pruning, an `EXISTS` semi-join cache, or array SQL support.
