# BENCHPROD-120: CH-benCHmark query-class matrix

## Purpose

BENCHPROD-119 made CH-benCHmark completed query classes visible, but the latest native full-run smoke only covered `Q3`. BENCHPROD-120 adds a dedicated BenchBase one-hot query-class matrix so CH queries can be exercised intentionally and promoted into production gate evidence.

## Changes

- Added `E:\Playground\FusionDB-bench\chbenchmark_query_class_matrix.py`.
- The runner:
  - starts FusionDB unless `--reuse-server` is used;
  - runs BenchBase `tpcc,chbenchmark` create/load once;
  - writes one BenchBase XML config per query case;
  - disables TPC-C during execute cases;
  - uses one-hot `weights bench="chbenchmark"` and serial execution;
  - captures stdout/stderr, BenchBase output files, and per-case coverage.
- Updated `E:\Playground\FusionDB-bench\bench_gate.py`.
  - Added latest discovery for `chbenchmark_query_class_matrix*/chbenchmark_query_class_matrix_summary.json`.
  - Added `--chbenchmark-query-matrix-report`.
  - Added `--chbenchmark-query-matrix-coverage`.
  - Added checks under `external_smoke.chbenchmark_query_matrix.*`.
- Updated profiles:
  - `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json`
  - `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json`

## Evidence

- Q3 probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod120_q3_probe3_20260529\chbenchmark_query_class_matrix_summary.json`
  - Status: `passed`, 1/1.
  - Completed `Q3=2`; no TPC-C transactions completed.
- Q1/Q3 positive matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod120_q1_q3_pass_20260529\chbenchmark_query_class_matrix_summary.json`
  - Status: `passed`, 2/2.
  - Covered queries: `Q1`, `Q3`.
- Q2 gap probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod120_q1_q3_probe_20260529\chbenchmark_query_class_matrix_summary.json`
  - Status: `gap`, Q1 passed, Q2 timed out after 420 seconds, Q3 was not run due to `--stop-on-gap`.

## Verification

- `python -m py_compile bench_gate.py chbenchmark_query_class_matrix.py chbenchmark_native_smoke.py`: passed.
- `python -m json.tool gate_profiles\production_medium.json`: passed.
- `python -m json.tool gate_profiles\production_medium_strict_native.json`: passed.
- CH query matrix only gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod120_chbenchmark_query_matrix_only_20260529\bench_gate_summary.json`
  - Status: `passed`, 31/31 checks.
- Combined explicit independent gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod120_independent_ldbc_tsbs_ch_matrix_20260529\bench_gate_summary.json`
  - Status: `passed`, 54/54 checks.
- Combined explicit strict independent gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod120_strict_independent_ldbc_tsbs_ch_matrix_20260529\bench_gate_summary.json`
  - Status: `passed`, 54/54 checks.
- Combined discovery gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod120_independent_discovery_ch_matrix_20260529\bench_gate_summary.json`
  - Status: `passed`, 54/54 checks.
- Combined strict discovery gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod120_strict_independent_discovery_ch_matrix_20260529\bench_gate_summary.json`
  - Status: `passed`, 54/54 checks.

## Gate Scope

The new gate checks are:

- `external_smoke.chbenchmark_query_matrix.exists`
- `external_smoke.chbenchmark_query_matrix.status`
- `external_smoke.chbenchmark_query_matrix.passed_count`
- `external_smoke.chbenchmark_query_matrix.required_queries`
- `external_smoke.chbenchmark_query_matrix.required_status`
- `external_smoke.chbenchmark_query_matrix.completions`
- `external_smoke.chbenchmark_query_matrix.isolation`
- `external_smoke.chbenchmark_query_matrix.scope`

Current profile defaults require `Q1` and `Q3`, with `min_passed_count=2`.

## Boundary

This is a BenchBase command-mode one-hot query-class matrix. It is not an official CH-benCHmark score and does not prove sustained mixed HTAP behavior. Broad CH Q1-Q22 coverage remains incomplete.

## Current Frontier

The next high-value task is BENCHPROD-121: profile and optimize CH-benCHmark `Q2`, which timed out after 420 seconds in the one-hot matrix at `scale_factor=1`.
