# BENCHPROD-115: LDBC complex-query matrix

## Purpose

Move LDBC evidence from broad 80-op smoke that may miss complex queries to a reproducible complex-query coverage matrix. BENCHPROD-114 proved a fast 80-op smoke, but still noted that the broad stream did not sample `LdbcQuery6`.

This task records command-mode isolation matrix evidence only. It does not claim a full official LDBC benchmark pass.

## Changes

- Added `E:\Playground\FusionDB-bench\ldbc_snb_complex_matrix.py`.
- The new wrapper reuses `ldbc_snb_native_smoke.py` instead of duplicating server/preload/index/command logic.
- Each matrix case:
  - runs native command mode with the PostgreSQL LDBC implementation;
  - preloads the PostgreSQL implementation test data;
  - creates Q14 and Q6 indexes by default;
  - enables exactly one complex query;
  - disables all other complex queries and all short reads;
  - writes per-case `ldbc_snb_native_smoke_summary.json`;
  - extracts per-query metrics from `ldbc_results\fusiondb-results.json`.
- The matrix writes:
  - `ldbc_snb_complex_matrix_summary.json`;
  - `ldbc_snb_complex_matrix_summary.md`;
  - `cases\<query>\matrix_case_command.json` for command provenance.

## Evidence

- Python compile:
  - `python -m py_compile ldbc_snb_complex_matrix.py ldbc_snb_native_smoke.py external_bootstrap.py`: passed.
- Representative matrix subset:
  - Summary: `E:\Playground\FusionDB-bench\runs\ldbc_snb_complex_matrix_benchprod115_q4_q6_q10_q14_20260529\ldbc_snb_complex_matrix_summary.json`.
  - Status: `passed`, 4/4.
  - Covered Q4, Q6, Q10, Q14.
- Full complex-query matrix:
  - Summary: `E:\Playground\FusionDB-bench\runs\ldbc_snb_complex_matrix_benchprod115_all_q1_q14_20260529\ldbc_snb_complex_matrix_summary.json`.
  - Markdown: `E:\Playground\FusionDB-bench\runs\ldbc_snb_complex_matrix_benchprod115_all_q1_q14_20260529\ldbc_snb_complex_matrix_summary.md`.
  - Status: `passed`.
  - Passed: 14/14.

## Full Matrix Metrics

Each row is 1 operation in a command-mode isolation case:

- `LdbcQuery1`: 101 ms.
- `LdbcQuery2`: 54 ms.
- `LdbcQuery3`: 160 ms.
- `LdbcQuery4`: 140 ms.
- `LdbcQuery5`: 195 ms.
- `LdbcQuery6`: 841 ms.
- `LdbcQuery7`: 62 ms.
- `LdbcQuery8`: 43 ms.
- `LdbcQuery9`: 71 ms.
- `LdbcQuery10`: 73 ms.
- `LdbcQuery11`: 44 ms.
- `LdbcQuery12`: 161 ms.
- `LdbcQuery13`: 95 ms.
- `LdbcQuery14`: 1,034 ms.

## Result

FusionDB-bench now has a repeatable LDBC SNB Interactive complex-query coverage matrix. It closes the evidence gap where broad smoke runs could pass while not sampling some complex queries, especially Q6.

This is still not a full official LDBC pass. The matrix intentionally disables all non-target reads per case, updates remain disabled through the underlying native smoke harness, and official mixed scheduling semantics are not covered by this artifact.

## Current Frontier

- Wire this matrix into gate profiles so LDBC complex coverage is not optional.
- Consider a similar coverage matrix for TSBS query shapes or CH-benCHmark query classes.
- Native memtier remains blocked until a real `memtier_benchmark` binary is available.
