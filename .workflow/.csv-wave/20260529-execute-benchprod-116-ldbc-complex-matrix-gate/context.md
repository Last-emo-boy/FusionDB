# BENCHPROD-116: LDBC complex-query matrix gate

## Purpose

BENCHPROD-115 added a reproducible LDBC SNB Interactive complex-query command-mode isolation matrix. BENCHPROD-116 wires that matrix into the benchmark gate so Q1-Q14 coverage is a production profile requirement instead of a manual artifact.

This remains conservative evidence. The matrix is not a full official LDBC benchmark pass, and updates remain disabled.

## Changes

- Updated `E:\Playground\FusionDB-bench\bench_gate.py`.
- Added latest-report discovery for `runs\ldbc_snb_complex_matrix*\ldbc_snb_complex_matrix_summary.json`.
- Added profile defaults under `external_smoke.ldbc_complex_matrix`.
- Added CLI controls:
  - `--ldbc-complex-matrix-report`
  - `--ldbc-complex-matrix-coverage`
  - `--no-ldbc-complex-matrix-coverage`
  - `--ldbc-complex-matrix-allowed-status`
  - `--ldbc-complex-matrix-required-queries`
  - `--ldbc-complex-matrix-min-passed-count`
- Added gate checks for report existence, matrix status, passed case count, required Q1-Q14 presence, per-query status, command-mode metrics status, metric count, and conservative scope.
- Added `ldbc_complex_matrix_report` to JSON and Markdown gate outputs.
- Updated:
  - `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json`
  - `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json`

## Evidence

- Python compile:
  - `python -m py_compile bench_gate.py ldbc_snb_complex_matrix.py`: passed.
- Explicit matrix gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod116_ldbc_complex_matrix_only_20260529\bench_gate_summary.json`
  - Status: `passed`, 31/31 checks.
- Matrix discovery gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod116_ldbc_complex_matrix_discovery_20260529\bench_gate_summary.json`
  - Status: `passed`, 31/31 checks.
  - The gate discovered `E:\Playground\FusionDB-bench\runs\ldbc_snb_complex_matrix_benchprod115_all_q1_q14_20260529\ldbc_snb_complex_matrix_summary.json`.
- Strict profile matrix gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod116_strict_ldbc_complex_matrix_20260529\bench_gate_summary.json`
  - Status: `passed`, 31/31 checks.

## Gate Scope

The new checks are:

- `external_smoke.ldbc_complex_matrix.exists`
- `external_smoke.ldbc_complex_matrix.status`
- `external_smoke.ldbc_complex_matrix.passed_count`
- `external_smoke.ldbc_complex_matrix.required_queries`
- `external_smoke.ldbc_complex_matrix.required_status`
- `external_smoke.ldbc_complex_matrix.command_status`
- `external_smoke.ldbc_complex_matrix.metrics`
- `external_smoke.ldbc_complex_matrix.scope`

The profile requires all `LdbcQuery1` through `LdbcQuery14` and `min_passed_count=14`.

## Boundary

This closes the gate coverage gap where broad LDBC smoke could pass without sampling every complex query. It does not close full official LDBC coverage:

- official mixed scheduling is not covered;
- update operations remain disabled;
- the existing LDBC native gate still treats disabled-query isolation runs as diagnosis-only;
- native memtier remains blocked by missing real `memtier_benchmark`.

## Current Frontier

Next useful BENCHPROD work is either a similar non-optional coverage matrix for TSBS or CH-benCHmark, or advancing full official LDBC mixed workload/update coverage.
