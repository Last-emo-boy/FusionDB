# BENCHPROD-102: bounded recursive CTE support

## Purpose

Continue LDBC native readiness after BENCHPROD-101 by implementing a bounded recursive CTE executor subset instead of leaving every LDBC recursive query at `WITH RECURSIVE is not supported`.

## Changes

- Added bounded recursive CTE materialization in `src/execution/query/mod.rs`.
- Supports recursive CTEs shaped as `anchor UNION [ALL] recursive_term`.
- Uses delta-table iteration during recursion and replaces the CTE temp table with the accumulated result before evaluating the outer query.
- Reused CTE temp-table materialization helpers for ordinary CTEs.
- Inferred materialized CTE column types from row values so recursive predicates like `n < 3` compare numerically.
- Allowed CTE alias column lists to rename a prefix of the query output, matching LDBC Query 14's `WITH sg(link, depth) AS (SELECT * FROM search_graph)` shape.
- Stopped exposing the previous synthetic `_rowid` column in CTE result schemas.
- Disabled scan projection pushdown for joins in the select path to avoid dropping projected columns needed by comma joins while LDBC recursive terms are being evaluated.

## Evidence

- `cargo test --test sql_set_subquery`: passed, `15/15`.
- `cargo test --test sql_join test_comma_join_projection_keeps_right_columns_without_join_pushdown`: passed.
- `cargo build --release --bin fusiondb`: passed.
- `cargo test --release --test pg_integration`: passed, `25/25`.
- LDBC non-isolation smoke:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod102_recursive_cte_frontier_20rows_10ops_20260529\ldbc_snb_native_smoke_summary.json`
  - `WITH RECURSIVE is not supported` was replaced by `CTE sg column count mismatch`.
- LDBC non-isolation smoke after prefix alias fix:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod102_cte_alias_frontier_20rows_10ops_20260529\ldbc_snb_native_smoke_summary.json`
  - Blocker moved to `Column k_person2id not found`.
- LDBC non-isolation smoke after join projection guard:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod102_join_projection_frontier_20rows_10ops_20260529\ldbc_snb_native_smoke_summary.json`
  - Blocker remains `Column k_person2id not found`, indicating the next frontier is correlated `NOT EXISTS` / outer column resolution in subqueries rather than the original recursive CTE unsupported gate.

## Result

BENCHPROD-102 does not make LDBC pass. It makes recursive CTE a supported bounded execution shape for simple SQL and moves the LDBC Query 14 blocker beyond the original parser/executor gap.

## Current Blockers

- Query 14 still fails in a correlated `NOT EXISTS` predicate inside the recursive term: `not exists(select * from sg y where y.link = k_person2id)`.
- Existing subquery materialization is not correlated-row aware, so outer-column references such as `k_person2id` are not available when the subquery is evaluated.
- Query 14 still has later expected blockers: arrays, array concatenation, `generate_subscripts`, row-number window output shape, and weighted path aggregation.
- Native memtier remains blocked by missing real `memtier_benchmark` probe.

## Next Task Candidate

BENCHPROD-103 should implement correlated `EXISTS` / `NOT EXISTS` evaluation for simple SELECT subqueries against the current outer row, then rerun the same non-isolation LDBC smoke.
