# BENCHPROD-028 Cost-Based Optimizer and Join Ordering

## Goal

Feed `ANALYZE` table statistics into optimizer decisions and make `EXPLAIN` expose cardinality and cost signals for index and join planning.

## Implementation

- `src/execution/ddl/explain.rs`
  - Added table cardinality and cost estimates to `EXPLAIN` output when `ANALYZE` stats are available.
  - Estimates equality, range, `IS NULL`, `IS NOT NULL`, `IN`, and `LIKE` predicate selectivity from column distinct/null counts.
  - Added implicit comma-join order explanation for 3+ table joins, including per-relation estimated rows and a join cost summary.
- `src/execution/scan/mod.rs`
  - Added stats-guided index probe caps so indexed predicates with estimated candidate counts above the fixed default cap can still choose index access when estimated index cost is below full scan cost.
  - Kept the existing real candidate-count guard as the final safety check.
- `src/execution/scan/index_plan.rs`
  - Reused `should_use_index_plan` with the active candidate cap, including stats-expanded caps.
- `src/execution/scan/join.rs`
  - Updated comma join reorder to prefer `ANALYZE` `row_count` over live `count_prefix`, with existing `count_prefix` fallback.
- `tests/sql_ddl.rs`
  - Extended `ANALYZE + EXPLAIN` coverage to assert `Estimate: rows=..., cost=...`.
  - Added 3-table implicit join `EXPLAIN` coverage showing stats-derived `Join Order` and `Join Estimate`.

## Verification

- `cargo test --test sql_ddl analyze`
  - Passed: 3/3.
- `cargo test --test sql_ddl explain`
  - Passed: 6/6.
- `cargo test --test sql_join comma_join_reorder`
  - Passed: 1/1.
- `cargo test --test sql_index_cache index`
  - Passed: 19/19.
- `cargo test --test sql_ddl`
  - Passed: 28/28.
- `cargo test --test sql_join`
  - Passed: 28/28.
- `cargo test --test sql_index_cache`
  - Passed: 36/36.
- `cargo build --release --bin fusiondb`
  - Passed.

## Result

This closes `BENCHPROD-028`: `ANALYZE` stats now feed index access costing, comma join order planning, and `EXPLAIN` cardinality/cost reporting. Existing LDBC and CH-style join regression tests passed after the planner change.
