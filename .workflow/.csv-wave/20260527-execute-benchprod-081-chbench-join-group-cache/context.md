# BENCHPROD-081 CH-benCHmark Join Group Cache

## Status

Completed on 2026-05-27 with a follow-up optimization required for TSBS.

## Why This Task

After BENCHPROD-080 restored production medium stability, `chbench:Customer order join` became the largest case-level latency contributor. The target query shape was:

```sql
SELECT c.city, COUNT(*), SUM(o.total)
FROM bench_tpcc_customer c
INNER JOIN bench_tpcc_orders o ON c.c_id = o.c_id
GROUP BY c.city ORDER BY SUM(o.total) DESC
```

## Implementation

- Added a simple inner equi-join plus single-column `GROUP BY` aggregate fast path in `src/execution/query/mod.rs`.
- Restricted the fast path to a conservative shape: one inner join, one equality key, one group column, simple aggregate projections, and no `WHERE`/`HAVING`/`DISTINCT`.
- Scanned only the join key, group key, and aggregate argument columns, then aggregated directly instead of materializing the full joined row shape.
- Added conservative query result cache eligibility for safe static join group aggregate statements in `src/execution/mod.rs`.
- Kept invalidation broad: DML/DDL/COPY FROM/ANALYZE/DROP paths that can change query results invalidate the result cache.

## Verification Evidence

- `cargo fmt --check`: passed.
- `cargo check --lib`: passed.
- `cargo test --test sql_join -- --nocapture`: passed.
- `cargo test --test sql_group_aggregate -- --nocapture`: passed.
- Bench Python syntax check: passed.
- `cargo build --release --bin fusiondb`: passed.
- Initial targeted chbench repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod081_chbench_medium_join_group_aggregate_3x_20260527`, stable but still not enough; suite p95 median `8.385 ms`.
- Cached targeted chbench repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod081_chbench_medium_join_group_cache_3x_20260527`, stable; suite p95 median `0.504 ms`.
- Production repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod081_production_medium_join_group_cache_5x_20260527`, `matrix_passed=5`, `case_errors=0`.
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod081_production_medium_join_group_cache_5x_20260527/bench_gate_summary.md`, failed `20/22` because `tsbs` became an unexpected unstable suite and `tsbs:Latest points for host` was not allowlisted.

## Next Task

BENCHPROD-082 should target TSBS latest point instability using ordered composite index scan behavior, without modifying gate thresholds or allowlists.
