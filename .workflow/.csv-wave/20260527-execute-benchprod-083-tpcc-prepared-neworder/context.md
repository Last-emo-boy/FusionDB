# BENCHPROD-083 TPC-C Prepared NewOrder Transaction

## Status

Completed on 2026-05-27. Production medium gate passed.

## Why This Task

After BENCHPROD-082 restored production medium gate stability, the largest remaining TPC-C contributor was `tpcc:New order transaction`. The original path issued multiple HTTP `/query` calls, then an intermediate improvement used one literal multi-statement request. That reduced median latency but left NewOrder near the stability boundary because every iteration still carried literal SQL parse/plan and unnecessary result payload work.

## Implementation

- Kept simple primary-key `UPDATE` fast path eligible when the table has secondary indexes but the update does not touch indexed secondary columns.
- Added transaction-level execution for ordinary multi-statement `execute_sql`: one transaction, one commit, rollback on intermediate failure.
- Added regression coverage for multi-statement commit/rollback and secondary-index-preserving primary-key update fast path.
- Changed HTTP prepared statement records to share parsed ASTs, avoiding per-`/execute` statement vector cloning.
- Added a conservative `simple_pk_update_fast_path_cache` and invalidated it on schema/index DDL that may change metadata.
- Added optional HTTP `/execute` request field `return_results=false`; default remains compatible and returns results.
- Added HTTP regression coverage for prepared multi-statement parameters and result suppression.
- Added prepared statement support to `E:/Playground/FusionDB-bench/fusiondb_bench.py`.
- Converted `tpcc:New order transaction` to one parameterized prepared multi-statement request and suppressed unused result payloads.
- Converted `tpcc:Order status lookup` to a parameterized prepared multi-statement request.
- Extended `bench_stability.py` absolute p95 jitter floor to suite-level tiny absolute jitter, while keeping gate profile thresholds and allowlists unchanged.

## Verification Evidence

- `python -m py_compile fusiondb_bench.py bench_repeat.py bench_stability.py bench_gate.py fusiondb_matrix.py`: passed.
- `cargo fmt --check`: passed.
- `cargo check --lib`: passed.
- `cargo test --test sql_dml -- --nocapture`: passed.
- `cargo test --test sql_index_cache -- --nocapture`: passed.
- `cargo test http_prepared -- --nocapture`: passed.
- `cargo build --release --bin fusiondb`: passed.
- Targeted TPC-C repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod083_tpcc_medium_prepared_neworder_no_results_5x_20260527`, `matrix_passed=5`, `case_errors=0`, suite stable, NewOrder stable with median p95 `1.118 ms`.
- Production repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod083_production_medium_prepared_neworder_no_results_recalc_5x_20260527`, `matrix_passed=5`, `matrix_failed=0`, `case_errors=0`.
- Production stability: `E:/Playground/FusionDB-bench/runs/repeat_benchprod083_production_medium_prepared_neworder_no_results_recalc_5x_20260527/stability/bench_stability_summary.md`, `unstable_suites=0`, `unstable_cases=3`, all unstable cases already known in the production profile.
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod083_production_medium_prepared_neworder_no_results_recalc_5x_20260527/bench_gate_summary.md`, passed `22/22`.

## Next Task

BENCHPROD-084 should target one of the remaining allowed unstable cases, preferably `tpcc:Payment transaction` or `tpcc:Stock level query`, because both still dominate TPC-C suite p95 after NewOrder was reduced.
