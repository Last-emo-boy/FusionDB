# BENCHPROD-094 Context

## Result

Added a FusionDB RESP preflight path to the native memtier evidence chain. This proves the local Redis-compatible endpoint can execute the command family memtier needs before the real `memtier_benchmark` binary is available.

Changes in `E:\Playground\FusionDB-bench`:

- `memtier_native_smoke.py` adds `--resp-preflight`.
- `external_smoke.py` includes RESP preflight evidence in native memtier readiness output.
- `install_external_tools.ps1` no longer fails during WSL missing-command/package probes and now prints the RESP preflight command.
- `README.md` documents the new preflight and keeps the distinction between endpoint evidence and native memtier pass.

## Verification

- `python -m py_compile memtier_native_smoke.py external_smoke.py fusiondb_matrix.py bench_gate.py`
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\install_external_tools.ps1 -Target memtier`
- `python memtier_native_smoke.py --run-name memtier_native_benchprod094_resp_preflight_20260528 --detect-wsl --start-fusiondb --resp-preflight --attempt-protocol-probe --target-port 6379 --fail-on-gap`
- `python external_smoke.py --run-name external_smoke_benchprod094_memtier_resp_preflight_20260528 --target benchbase-tpcc,memtier,tsbs,ldbc,chbenchmark --memtier-native-evidence runs\memtier_native_benchprod094_resp_preflight_20260528\memtier_native_smoke_summary.json --chbenchmark-native-evidence runs\chbenchmark_native_benchbase_full_after_q6_q8_fix_20260528\chbenchmark_native_smoke_summary.json`
- `python bench_gate.py --gate-profile gate_profiles\production_medium.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --external-smoke-report runs\external_smoke_benchprod094_memtier_resp_preflight_20260528\external_smoke_summary.json --recovery-smoke-report runs\recovery_smoke_benchprod086_pgtest_port_fix_20260528\recovery_smoke_summary.json --run-name gate_benchprod094_memtier_resp_preflight_relaxed_20260528`
- `python bench_gate.py --gate-profile gate_profiles\production_medium_strict_native.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --external-smoke-report runs\external_smoke_benchprod094_memtier_resp_preflight_20260528\external_smoke_summary.json --recovery-smoke-report runs\recovery_smoke_benchprod086_pgtest_port_fix_20260528\recovery_smoke_summary.json --run-name gate_benchprod094_memtier_resp_preflight_strict_20260528`

Reports:

- `E:\Playground\FusionDB-bench\runs\memtier_native_benchprod094_resp_preflight_20260528\memtier_native_smoke_summary.md`
- `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod094_memtier_resp_preflight_20260528\external_smoke_summary.md`
- `E:\Playground\FusionDB-bench\runs\gate_benchprod094_memtier_resp_preflight_relaxed_20260528\bench_gate_summary.md`
- `E:\Playground\FusionDB-bench\runs\gate_benchprod094_memtier_resp_preflight_strict_20260528\bench_gate_summary.md`

## Current Gate State

The ordinary `production_medium` gate passed: 61/61 checks.

The strict native gate still fails by design: 57/61 checks passed, 4 failures remain:

- `external_smoke.memtier.status`: `tool_missing`
- `external_smoke.memtier.native_status`: `tool_missing`
- `external_smoke.ldbc.status`: `artifact_missing`
- `external_smoke.ldbc.native_status`: `artifact_missing`

The native memtier report now additionally proves:

- FusionDB started with Redis-compatible endpoint on `127.0.0.1:6379`.
- RESP commands passed: PING, ECHO, SELECT, SET, GET, MSET, MGET, EXISTS, INCR, INFO, DEL, QUIT.

This is not a native memtier pass because `memtier_benchmark` is still unavailable.

## Remaining Work

Next high-value tasks:

1. Build or install real `memtier_benchmark` in WSL or Windows, then run `memtier_native_smoke.py --detect-wsl --start-fusiondb --attempt-protocol-probe --target-port 6379 --fail-on-gap`.
2. If the real memtier probe fails, fix the captured RESP/protocol gap in FusionDB.
3. `BENCHPROD-095`: provide LDBC SNB driver/datagen artifact and run native LDBC smoke.
