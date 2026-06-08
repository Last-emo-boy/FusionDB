# BENCHPROD-084 TPC-C Prepared Payment Transaction

## Status

Completed on 2026-05-27. Production medium gate passed with the strengthened sample-count check.

## Why This Task

After BENCHPROD-083, the production gate passed but TPC-C still had allowed unstable cases. `tpcc:Payment transaction` was still implemented as three independent HTTP `/query` requests:

- update customer balance,
- update district YTD,
- update warehouse YTD.

That path paid three HTTP round trips, three parse/execute cycles, and unnecessary response payload work for a single logical transaction.

## Implementation

- Added `TPCC_PAYMENT_PREPARED_SQL` in `E:/Playground/FusionDB-bench/fusiondb_bench.py`.
- Converted `tpcc_payment` to `runner.execute_prepared("tpcc_payment", ..., return_results=False)`.
- Kept the operation semantics at three logical row updates.
- Added `min_case_samples` support to `bench_gate.py`.
- Updated `gate_profiles/production_medium.json` with `min_case_samples=60`.
- Updated `E:/Playground/FusionDB-bench/README.md` to recommend `--iters 60 --warmup 5` for production gate repeats.

## Verification Evidence

- `python -m py_compile fusiondb_bench.py bench_repeat.py bench_stability.py bench_gate.py fusiondb_matrix.py`: passed.
- `cargo build --release --bin fusiondb`: passed.
- Targeted TPC-C repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod084_tpcc_medium_prepared_payment_5x_20260527`, `matrix_passed=5`, `case_errors=0`, `unstable_suites=0`, `unstable_cases=0`, suite median p95 `1.611 ms`.
- Low-sample gate negative check: `E:/Playground/FusionDB-bench/runs/gate_benchprod084_sample_check_expected_fail_20260527/bench_gate_summary.md` failed on `repeat.case_samples` with `min=20`, proving the new production gate rejects undersampled repeat evidence.
- High-sample production repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod084_production_medium_prepared_payment_60iters_5x_20260527`, `matrix_passed=5`, `matrix_failed=0`, `case_errors=0`.
- High-sample production stability: `E:/Playground/FusionDB-bench/runs/repeat_benchprod084_production_medium_prepared_payment_60iters_5x_20260527/stability/bench_stability_summary.md`, `unstable_suites=0`, `unstable_cases=0`.
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod084_production_medium_prepared_payment_60iters_5x_20260527/bench_gate_summary.md`, passed `23/23`, including `repeat.case_samples` with `min=60; checked=90`.

## Outcome

Production medium gate now has stronger evidence quality and no unstable cases in the high-sample BENCHPROD-084 run. Suite medians from the passing gate:

- `tpcc`: p95 `1.822 ms`, ops/sec `937.0`.
- `memtier`: p95 `0.900 ms`, ops/sec `1103.5`.
- `tsbs`: p95 `1.315 ms`, ops/sec `1000.4`.
- `ldbc`: p95 `1.194 ms`, ops/sec `1095.6`.
- `chbench`: p95 `0.684 ms`, ops/sec `1117.2`.

## Next Task

BENCHPROD-085 should move from HTTP workload polish toward production ecosystem readiness. The highest-value next target is a PgWire/JDBC-oriented smoke path for production suites, because official BenchBase/TPC-C, LDBC, and CH-benCHmark depend on protocol metadata and driver behavior rather than only HTTP `/query`.
