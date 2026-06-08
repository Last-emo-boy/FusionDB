# BENCHPROD-122: CH-benCHmark Q1-Q4 query-class gate

## Purpose

Continue the production benchmark iteration by expanding CH-benCHmark command-mode one-hot query-class coverage from the previous Q1/Q2/Q3 frontier to Q1-Q4, then make that coverage part of the production gate profile.

## Changes

- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json`.
  - `external_smoke.chbenchmark_query_matrix.required_queries` now requires `Q1`, `Q2`, `Q3`, and `Q4`.
  - `external_smoke.chbenchmark_query_matrix.min_passed_count` is now `4`.
  - Source metadata records BENCHPROD-122.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json` with the same Q1-Q4 requirement.
- Updated `E:\Playground\FusionDB-bench\README.md`.
  - Added CH-benCHmark query-class matrix commands.
  - Recorded the BENCHPROD-122 evidence path and clarified the scope boundary.
  - Updated readiness status for CH-benCHmark to mention Q1-Q4 one-hot coverage.

## Evidence

- Q4 one-hot probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod122_q4_probe2_20260529\chbenchmark_query_class_matrix_summary.json`
  - Status: `passed`, 1/1.
  - `Q4` completed count: `2`.
- Q1-Q4 combined matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod122_q1_q2_q3_q4_20260529\chbenchmark_query_class_matrix_summary.json`
  - Status: `passed`, 4/4.
  - Covered queries: `Q1`, `Q2`, `Q3`, `Q4`.
  - Each query completed count: `2`.
- Explicit strict gate using Q1-Q4 matrix:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod122_strict_explicit_q1_q2_q3_q4_20260529\bench_gate_summary.json`
  - Status: `passed`, 54/54 checks.

## Verification

- `cargo build --release --bin fusiondb`: passed.
- `python chbenchmark_query_class_matrix.py --queries Q4 ... --fail-on-gap`: passed.
- `python chbenchmark_query_class_matrix.py --queries Q1,Q2,Q3,Q4 ... --fail-on-gap`: passed.
- `python bench_gate.py --gate-profile gate_profiles\production_medium_strict_native.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --chbenchmark-query-matrix-report runs\chbenchmark_query_class_matrix_benchprod122_q1_q2_q3_q4_20260529\chbenchmark_query_class_matrix_summary.json --no-external-smoke --no-recovery-smoke --run-name gate_benchprod122_strict_explicit_q1_q2_q3_q4_20260529`: passed.

## Boundary

This is BenchBase command-mode one-hot query-class evidence. It is not an official CH-benCHmark score and does not prove sustained mixed HTAP behavior. The next CH-benCHmark iteration should expand to Q5 or run a longer mixed HTAP probe only after checking the current gate remains stable.
