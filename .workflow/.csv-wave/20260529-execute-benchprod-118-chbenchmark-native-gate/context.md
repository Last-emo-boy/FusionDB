# BENCHPROD-118: CH-benCHmark native gate

## Purpose

CH-benCHmark had native full-run evidence, but the production gate mainly verified it through `external_smoke_summary.json`. BENCHPROD-118 promotes that evidence to an independently discoverable gate input, matching the newer LDBC matrix and TSBS official runner gate patterns.

## Changes

- Updated `E:\Playground\FusionDB-bench\bench_gate.py`.
- Added discovery for `runs\chbenchmark_native*\chbenchmark_native_smoke_summary.json`.
- Added `--chbenchmark-native-report`.
- Added `evaluate_chbenchmark_native`.
- Extracted CH-benCHmark native checks into `add_chbenchmark_native_checks`.
- Added `chbenchmark_native_report` to JSON and Markdown gate outputs.
- Kept checks under `external_smoke.chbenchmark.*` to preserve profile semantics while removing dependence on an external smoke report link.
- Updated profile source text in:
  - `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json`
  - `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json`

## Evidence

- Source CH-benCHmark native report:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_native_benchbase_full_after_q6_q8_fix_20260528\chbenchmark_native_smoke_summary.json`
  - Status: `passed`.
  - Run mode: `full`.
  - Required workload steps: `chbenchmark_create`, `chbenchmark_load`, `chbenchmark_execute`.
- Python compile:
  - `python -m py_compile bench_gate.py chbenchmark_native_smoke.py tsbs_official_runner_smoke.py ldbc_snb_complex_matrix.py`: passed.
- JSON validation:
  - `python -m json.tool gate_profiles\production_medium.json`: passed.
  - `python -m json.tool gate_profiles\production_medium_strict_native.json`: passed.
- Explicit CH-benCHmark report gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod118_chbenchmark_native_only_20260529\bench_gate_summary.json`
  - Status: `passed`, 28/28 checks.
- CH-benCHmark discovery gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod118_chbenchmark_native_discovery_20260529\bench_gate_summary.json`
  - Status: `passed`, 28/28 checks.
- Strict profile CH-benCHmark discovery gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod118_strict_chbenchmark_native_discovery_20260529\bench_gate_summary.json`
  - Status: `passed`, 28/28 checks.
- Combined independent gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod118_independent_ldbc_tsbs_ch_20260529\bench_gate_summary.json`
  - Status: `passed`, 43/43 checks.
- Combined strict independent gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod118_strict_independent_ldbc_tsbs_ch_20260529\bench_gate_summary.json`
  - Status: `passed`, 43/43 checks.

## Gate Scope

The independent checks are:

- `external_smoke.chbenchmark.native_evidence`
- `external_smoke.chbenchmark.native_status`
- `external_smoke.chbenchmark.native_run_mode`
- `external_smoke.chbenchmark.native_steps`
- `external_smoke.chbenchmark.native_workload_steps`
- `external_smoke.chbenchmark.native_blockers`

The gate now requires a passed CH-benCHmark native report to be `run_mode=full` and to include passed `chbenchmark_create`, `chbenchmark_load`, and `chbenchmark_execute` steps.

## Boundary

This is native CH-benCHmark full-run smoke, not long-duration HTAP production certification. The current latest proof is a short `duration=10` run and does not prove all CH-benCHmark query classes under sustained mixed load.

## Current Frontier

Good next BENCHPROD candidates:

- move native memtier from blocked to runnable by installing/building real `memtier_benchmark`;
- add a CH-benCHmark query-class matrix;
- deepen TSBS and CH-benCHmark to longer official-runner profiles.
