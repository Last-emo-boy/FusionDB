# BENCHPROD-128: CH-benCHmark Q1-Q10 query-class gate

## Purpose

Continue the production benchmark iteration by expanding CH-benCHmark command-mode one-hot query-class coverage from Q1-Q9 to Q1-Q10, then make that coverage part of the production gate profile.

## Changes

- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json`.
  - `external_smoke.chbenchmark_query_matrix.required_queries` now requires `Q1` through `Q10`.
  - `external_smoke.chbenchmark_query_matrix.min_passed_count` is now `10`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json` with the same Q1-Q10 requirement.
- Updated `E:\Playground\FusionDB-bench\README.md`.
  - Refreshed CH-benCHmark query-class matrix commands and evidence path.
  - Updated readiness status for CH-benCHmark to mention Q1-Q10 one-hot coverage.

## Evidence

- Q10 one-hot probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod128_q10_probe_20260529\chbenchmark_query_class_matrix_summary.json`
  - Status: `passed`.
  - `Q10` completed count: `2`.
- Q1-Q10 combined matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod128_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_20260529\chbenchmark_query_class_matrix_summary.json`
  - Status: `passed`.
  - Passed count: `10/10`.
  - Covered queries: `Q1`, `Q2`, `Q3`, `Q4`, `Q5`, `Q6`, `Q7`, `Q8`, `Q9`, `Q10`.
  - Each query completed count: `2`.

## Commands

- `python chbenchmark_query_class_matrix.py --queries Q10 ... --fail-on-gap`: passed.
- `python chbenchmark_query_class_matrix.py --queries Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10 ... --fail-on-gap`: passed.

## Scope

This is BenchBase command-mode one-hot query-class evidence. It is not an official CH-benCHmark score and does not prove sustained mixed HTAP behavior. The next CH-benCHmark iteration should expand to Q11 or run a longer mixed HTAP probe only after checking the current gate remains stable.
