# BENCHPROD-079 Indexed Filtered Bare Aggregate Optimization

## Status

Completed on 2026-05-27.

## Why This Task

After BENCHPROD-078, the production medium stability report still identified `tpcc:Stock level query`, `tpcc:Payment transaction`, and `tsbs:Ingest one point` as the main write/short-query p95 jitter contributors. The stock-level SQL shape is:

```sql
SELECT COUNT(*) FROM bench_tpcc_stock WHERE w_id = ? AND quantity < 20
```

`bench_tpcc_stock` has a BTree index on `w_id`, while `quantity < 20` is a residual predicate.

## Implementation

- Added `ColumnAggregateScanVisitor` in `src/execution/query/column_scan.rs` so bare aggregate scans stream via `scan_prefix_for_each` instead of materializing all `(key, value)` pairs.
- Added an indexed filtered bare aggregate path for predicates with a primary-key or single-column BTree equality term.
- The indexed path scans matching row IDs from the equality index, fetches candidate rows by primary key, and still evaluates the full predicate with existing partial column decode logic.
- Updated `src/execution/query/mod.rs` to pass schema context into simple column aggregate scans.
- Added tests covering streaming filtered bare aggregates and indexed candidate filtered counts with intentionally corrupted unrelated columns:
  - `test_filtered_bare_aggregates_stream_required_columns_only`
  - `test_filtered_count_uses_index_candidates_and_required_columns_only`

## Verification Evidence

- `cargo fmt --check`: passed.
- `cargo check --lib`: passed.
- `cargo test --test sql_group_aggregate -- --nocapture`: passed, `44 passed`.
- `cargo build --release --bin fusiondb`: passed.
- Bench Python syntax check: `python -m py_compile bench_stability.py bench_gate.py bench_repeat.py fusiondb_matrix.py fusiondb_bench.py`: passed.
- Targeted repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod079_tpcc_tsbs_medium_indexed_bareagg_3x_20260527`, `matrix_passed=3`, `case_errors=0`, `unstable_cases=0`.
- Production repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod079_production_medium_indexed_bareagg_5x_20260527`, `matrix_passed=5`, `matrix_failed=0`, `case_errors=0`.
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod079_production_medium_indexed_bareagg_5x_20260527/bench_gate_summary.md`, passed `22/22`.

## Observed Impact

- BENCHPROD-078 production baseline: `tpcc` suite p95 range `0.568 ms`, `tsbs` suite p95 range `0.163 ms`, unstable cases included `tpcc:Stock level query`, `tpcc:Payment transaction`, and `tsbs:Ingest one point`.
- BENCHPROD-079 targeted `tpcc,tsbs` repeat: `unstable_cases=0`; `tpcc` suite p95 range `0.077 ms`; `tsbs` suite p95 range `0.009 ms`.
- BENCHPROD-079 production repeat: all production suites stable; remaining unstable cases are case-level allowlisted `ldbc:One-hop friends` and `tpcc:Stock level query`.

## Next Task

BENCHPROD-080 should focus on removing the remaining production case instability:

- `ldbc:One-hop friends`
- `tpcc:Stock level query`

Likely directions: index-probe row-id ordering/dedup overhead, join fast path for one-hop LDBC, and a more selective stock-level access path such as composite index support for equality plus residual/range count.
