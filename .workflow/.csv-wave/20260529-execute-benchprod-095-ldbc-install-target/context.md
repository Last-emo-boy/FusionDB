# BENCHPROD-095 Context

## Result

Added a bounded LDBC SNB install/bootstrap target to the external benchmark tooling. This keeps LDBC in the same operational path as TSBS, BenchBase, and memtier while preserving the current strict native blocker.

Changes in `E:\Playground\FusionDB-bench`:

- `install_external_tools.ps1` now accepts `-Target ldbc`.
- The LDBC target checks `LDBC_SNB_HOME`, `LDBC_DRIVER_HOME`, and expected local layouts such as `E:\Playground\ldbc-snb`.
- Candidate discovery is bounded to expected LDBC locations rather than scanning all of `E:\Playground`.
- The script prints exact `LDBC_SNB_HOME`, `external_bootstrap.py`, and `ldbc_snb_native_smoke.py` follow-up commands, while explicitly noting that help mode is not a native workload pass.

## Verification

- `powershell -NoProfile -ExecutionPolicy Bypass -File .\install_external_tools.ps1 -Target ldbc`
- PowerShell parser check via `[System.Management.Automation.Language.Parser]::ParseFile(...)`
- `python -m py_compile external_bootstrap.py external_smoke.py ldbc_snb_native_smoke.py bench_gate.py`
- `python external_bootstrap.py --target ldbc --search-root E:\Playground\FusionDB-bench --run-name external_bootstrap_benchprod095_ldbc_install_target_20260529`
- `python ldbc_snb_native_smoke.py --run-name ldbc_snb_native_benchprod095_install_target_20260529 --search-root E:\Playground\FusionDB-bench --artifact-scan-depth 3`
- `python external_smoke.py --target ldbc --ldbc-native-evidence runs\ldbc_snb_native_benchprod095_install_target_20260529\ldbc_snb_native_smoke_summary.json --run-name external_smoke_benchprod095_ldbc_install_target_20260529`

Reports:

- `E:\Playground\FusionDB-bench\runs\external_bootstrap_benchprod095_ldbc_install_target_20260529\external_bootstrap_summary.md`
- `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod095_install_target_20260529\ldbc_snb_native_smoke_summary.md`
- `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod095_ldbc_install_target_20260529\external_smoke_summary.md`

## Current Gate State

Native LDBC remains blocked, correctly:

- `external_bootstrap.ldbc.status`: `artifact_missing`
- `ldbc_snb_native_smoke.status`: `artifact_missing`
- `external_smoke.ldbc.status`: `artifact_missing`

The refreshed evidence confirms Java/JDBC prerequisites are present and FusionDB LDBC config generation still works, but no native LDBC claim is made without an actual SNB driver/datagen artifact and command template.

## Remaining Work

Next high-value tasks:

1. `BENCHPROD-096`: place/build LDBC SNB under `E:\Playground\ldbc-snb` or set `LDBC_SNB_HOME`.
2. Run `install_external_tools.ps1 -Target ldbc` and `ldbc_snb_native_smoke.py --ldbc-artifact ... --run-mode help`.
3. Provide the distribution-specific `--ldbc-command` template and capture the first real schema/import/workload compatibility gap.
