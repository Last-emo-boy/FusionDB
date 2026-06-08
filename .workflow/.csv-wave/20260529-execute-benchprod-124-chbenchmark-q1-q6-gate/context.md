# BENCHPROD-124: CH-benCHmark Q1-Q6 query-class gate

## Purpose

Continue the production benchmark iteration by expanding CH-benCHmark command-mode one-hot query-class coverage from Q1-Q5 to Q1-Q6, then make that coverage part of the production gate profile.

## Changes

- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json`.
  - `external_smoke.chbenchmark_query_matrix.required_queries` now requires `Q1`, `Q2`, `Q3`, `Q4`, `Q5`, and `Q6`.
  - `external_smoke.chbenchmark_query_matrix.min_passed_count` is now `6`.
  - Source metadata records BENCHPROD-124.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json` with the same Q1-Q6 requirement.
- Updated `E:\Playground\FusionDB-bench\README.md`.
  - Refreshed CH-benCHmark query-class matrix commands and evidence path.
  - Updated readiness status for CH-benCHmark to mention Q1-Q6 one-hot coverage.

## Evidence

- Q6 one-hot probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod124_q6_probe_20260529\chbenchmark_query_class_matrix_summary.json`
  - Status: `passed`, 1/1.
  - `Q6` completed count: `2`.
- Q1-Q6 combined matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod124_q1_q2_q3_q4_q5_q6_20260529\chbenchmark_query_class_matrix_summary.json`
  - Status: `passed`, 6/6.
  - Covered queries: `Q1`, `Q2`, `Q3`, `Q4`, `Q5`, `Q6`.
  - Each query completed count: `2`.
- Explicit strict gate using Q1-Q6 matrix:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod124_strict_explicit_q1_q2_q3_q4_q5_q6_20260529\bench_gate_summary.json`
  - Status: `passed`, 54/54 checks.

## Verification

- `python chbenchmark_query_class_matrix.py --queries Q6 ... --fail-on-gap`: passed.
- `python chbenchmark_query_class_matrix.py --queries Q1,Q2,Q3,Q4,Q5,Q6 ... --fail-on-gap`: passed.
- `python bench_gate.py --gate-profile gate_profiles\production_medium_strict_native.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --chbenchmark-query-matrix-report runs\chbenchmark_query_class_matrix_benchprod124_q1_q2_q3_q4_q5_q6_20260529\chbenchmark_query_class_matrix_summary.json --no-external-smoke --no-recovery-smoke --run-name gate_benchprod124_strict_explicit_q1_q2_q3_q4_q5_q6_20260529`: passed.

## Boundary

This is BenchBase command-mode one-hot query-class evidence. It is not an official CH-benCHmark score and does not prove sustained mixed HTAP behavior. The next CH-benCHmark iteration should expand to Q7 or run a longer mixed HTAP probe only after checking the current gate remains stable.
