# BENCHPROD-096 Context

## Result

Built a reproducible LDBC SNB Interactive v1 artifact bootstrap path for the benchmark system in `E:\Playground\FusionDB-bench`.

Changes in `E:\Playground\FusionDB-bench`:

- `install_external_tools.ps1 -Target ldbc` now installs/discovers portable Apache Maven under `E:\Playground\tools\apache-maven`, clones the official LDBC driver and implementation repositories under `E:\Playground\ldbc-snb`, and builds:
  - `E:\Playground\ldbc-snb\driver\target\driver-standalone.jar`
  - `E:\Playground\ldbc-snb\impls\postgres\target\postgres-1.2.0-SNAPSHOT.jar`
- `external_bootstrap.py` now has fast preferred LDBC artifact discovery, so `--search-root E:\Playground` no longer needs an expensive broad scan before finding standard LDBC artifacts.
- `ldbc_snb_native_smoke.py` now prefers `driver-standalone.jar` over non-executable implementation jars.
- `external_smoke.py` records LDBC native evidence `run_mode`, and reports help/readiness mode as a blocker.
- `bench_gate.py` now requires `external_smoke.ldbc.native_run_mode == command`, preventing a help-mode `passed` report from satisfying strict native workload coverage.
- `README.md` documents the LDBC bootstrap path and the boundary between artifact readiness and native workload pass.

## Verification

- PowerShell parser check for `install_external_tools.ps1`: passed.
- `python -m py_compile external_bootstrap.py external_smoke.py ldbc_snb_native_smoke.py bench_gate.py`: passed.
- `.\install_external_tools.ps1 -Target ldbc`: passed and resolves `driver-standalone.jar`.
- `python external_bootstrap.py --target ldbc --search-root E:\Playground --max-depth 5 --run-name external_bootstrap_benchprod096_ldbc_artifact_bootstrap_20260529_fast`: passed.
- `python ldbc_snb_native_smoke.py --ldbc-artifact E:\Playground\ldbc-snb\driver\target\driver-standalone.jar --run-mode help --run-name ldbc_snb_native_benchprod096_driver_help_20260529`: passed as readiness only.
- `python external_smoke.py --target benchbase-tpcc,memtier,tsbs,ldbc,chbenchmark ... --run-name external_smoke_benchprod096_all_targets_ldbc_artifact_bootstrap_with_tsbs_path_20260529`: passed report generation, with LDBC `tool_available` and memtier still `tool_missing`.
- `python bench_gate.py --gate-profile gate_profiles\production_medium_strict_native.json ... --run-name gate_benchprod096_ldbc_artifact_bootstrap_strict_20260529`: failed as expected, `58/61` checks passed.

Reports:

- `E:\Playground\FusionDB-bench\runs\external_bootstrap_benchprod096_ldbc_artifact_bootstrap_20260529_fast\external_bootstrap_summary.json`
- `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod096_driver_help_20260529\ldbc_snb_native_smoke_summary.json`
- `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod096_all_targets_ldbc_artifact_bootstrap_with_tsbs_path_20260529\external_smoke_summary.json`
- `E:\Playground\FusionDB-bench\runs\gate_benchprod096_ldbc_artifact_bootstrap_strict_20260529\bench_gate_summary.json`
- `E:\Playground\ldbc-snb\build_logs\driver_maven_package.log`
- `E:\Playground\ldbc-snb\build_logs\impls_postgres_maven_package.log`

## Current Gate State

Strict native gate remains blocked, correctly:

- `external_smoke.memtier.status`: `tool_missing`, expected `tool_available`.
- `external_smoke.memtier.native_status`: `tool_missing`, expected `passed`.
- `external_smoke.ldbc.native_run_mode`: `help`, expected `command`.

LDBC artifact availability is no longer the blocker:

- `external_bootstrap.ldbc.status`: `artifact_configurable`.
- `external_smoke.ldbc.status`: `tool_available`.
- `ldbc_snb_native_smoke.status`: `passed` in `run_mode=help`, explicitly readiness only.

## Remaining Work

Next high-value tasks:

1. `BENCHPROD-097`: turn LDBC from help readiness into a first `run_mode=command` execution using the official driver classpath, PostgreSQL implementation jar, generated FusionDB properties, and a tiny/simple workload or explicit SNB command template.
2. Install real `memtier_benchmark` in WSL or Windows and rerun `memtier_native_smoke.py --start-fusiondb --attempt-protocol-probe --fail-on-gap`.
3. Keep strict native gate failing until memtier real probe and LDBC command-mode workload evidence are both present.
