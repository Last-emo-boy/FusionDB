# BENCHPROD-117: TSBS official runner gate

## Purpose

BENCHPROD-084 produced TSBS official runner evidence with six required query types, three queries per type, 100 generated data points, and load/query result validation. Before this task, `bench_gate.py` could verify that evidence only through `external_smoke_summary.json`.

BENCHPROD-117 promotes TSBS official runner evidence to an independently discoverable gate input, matching the stronger pattern introduced for the LDBC complex-query matrix in BENCHPROD-116.

## Changes

- Updated `E:\Playground\FusionDB-bench\bench_gate.py`.
- Added discovery for `runs\tsbs_official_runner*\tsbs_official_runner_smoke_summary.json`.
- Added `--tsbs-official-runner-report`.
- Added `evaluate_tsbs_official_runner`.
- Extracted TSBS official runner checks into `add_tsbs_official_runner_checks`.
- Added `tsbs_official_runner_report` to JSON and Markdown gate outputs.
- Kept TSBS checks under the existing `external_smoke.tsbs.official_runner.*` namespace so production profile semantics remain stable.
- Updated profile source text in:
  - `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json`
  - `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json`

## Evidence

- Source TSBS official runner report:
  - `E:\Playground\FusionDB-bench\runs\tsbs_official_runner_benchprod084_q3_points100_day_window_20260528\tsbs_official_runner_smoke_summary.json`
  - Status: `passed`.
  - Query types: `single-groupby-1-1-1`, `lastpoint`, `cpu-max-all-1`, `high-cpu-1`, `double-groupby-1`, `groupby-orderby-limit`.
  - Queries per type: 3.
  - Max data points: 100.
  - Results validation: load and required query result JSON validated.
- Python compile:
  - `python -m py_compile bench_gate.py tsbs_official_runner_smoke.py ldbc_snb_complex_matrix.py`: passed.
- JSON validation:
  - `python -m json.tool gate_profiles\production_medium.json`: passed.
  - `python -m json.tool gate_profiles\production_medium_strict_native.json`: passed.
- Explicit report gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod117_tsbs_official_runner_only_20260529\bench_gate_summary.json`
  - Status: `passed`, 30/30 checks.
- Discovery gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod117_tsbs_official_runner_discovery_20260529\bench_gate_summary.json`
  - Status: `passed`, 30/30 checks.
- Strict profile discovery gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod117_strict_tsbs_official_runner_discovery_20260529\bench_gate_summary.json`
  - Status: `passed`, 30/30 checks.

## Gate Scope

The independent checks are:

- `external_smoke.tsbs.official_runner.exists`
- `external_smoke.tsbs.official_runner.status`
- `external_smoke.tsbs.official_runner.query_types`
- `external_smoke.tsbs.official_runner.queries_per_type`
- `external_smoke.tsbs.official_runner.data_points`
- `external_smoke.tsbs.official_runner.results_validated`
- `external_smoke.tsbs.official_runner.query_results_validated`

The production profile still requires:

- all six configured TSBS query types;
- at least three queries per type;
- at least 100 generated data points;
- load result validation;
- query result validation for every required query type.

## Boundary

This is official TSBS runner compatibility smoke, not an official TSBS benchmark score. It strengthens gate evidence by making TSBS official runner coverage independent of `external_smoke_summary.json`, but broader scale, longer duration, and oracle-level query-result correctness are still future production-readiness tasks.

## Current Frontier

Good next BENCHPROD candidates:

- add a CH-benCHmark workload-shape gate independent of external smoke;
- move native memtier from blocked to runnable by installing/building a real `memtier_benchmark`;
- deepen TSBS from q3/100 smoke to a longer official runner profile.
