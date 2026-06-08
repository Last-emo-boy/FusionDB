# BENCHPROD-093 Context

## Result

Connected the latest CH-benCHmark BenchBase native full smoke evidence into the production benchmark evidence chain.

Changes in `E:\Playground\FusionDB-bench`:

- `external_smoke.py` now treats `BENCHBASE_HOME` / `BENCHBASE_JAR` as valid CH-benCHmark artifact sources, matching `chbenchmark_native_smoke.py`.
- `fusiondb_bench.py` capability output now reports BenchBase/TPC-C, TSBS official runner, and CH-benCHmark native smoke status more accurately.
- `README.md` now documents the CH-benCHmark BenchBase full smoke and the updated strict native blocker set.

## Verification

- `python -m py_compile external_smoke.py fusiondb_bench.py chbenchmark_native_smoke.py bench_gate.py`
- `python external_smoke.py --run-name external_smoke_benchprod093_ch_native_refresh_20260528 --target benchbase-tpcc,memtier,tsbs,ldbc,chbenchmark --chbenchmark-native-evidence runs\chbenchmark_native_benchbase_full_after_q6_q8_fix_20260528\chbenchmark_native_smoke_summary.json`
- `python bench_gate.py --gate-profile gate_profiles\production_medium.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --external-smoke-report runs\external_smoke_benchprod093_ch_native_refresh_20260528\external_smoke_summary.json --recovery-smoke-report runs\recovery_smoke_benchprod086_pgtest_port_fix_20260528\recovery_smoke_summary.json --run-name gate_benchprod093_ch_native_refresh_relaxed_20260528`
- `python bench_gate.py --gate-profile gate_profiles\production_medium_strict_native.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --external-smoke-report runs\external_smoke_benchprod093_ch_native_refresh_20260528\external_smoke_summary.json --recovery-smoke-report runs\recovery_smoke_benchprod086_pgtest_port_fix_20260528\recovery_smoke_summary.json --run-name gate_benchprod093_ch_native_refresh_20260528`
- `python fusiondb_bench.py --list`

Reports:

- `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod093_ch_native_refresh_20260528\external_smoke_summary.md`
- `E:\Playground\FusionDB-bench\runs\gate_benchprod093_ch_native_refresh_relaxed_20260528\bench_gate_summary.md`
- `E:\Playground\FusionDB-bench\runs\gate_benchprod093_ch_native_refresh_20260528\bench_gate_summary.md`

## Current Gate State

The ordinary `production_medium` gate passed: 61/61 checks.

The strict native gate still fails by design: 57/61 checks passed, 4 failures remain:

- `external_smoke.memtier.status`: `tool_missing`
- `external_smoke.memtier.native_status`: `tool_missing`
- `external_smoke.ldbc.status`: `artifact_missing`
- `external_smoke.ldbc.native_status`: `artifact_missing`

CH-benCHmark is no longer a strict native blocker when the refreshed evidence is used.

## Remaining Work

Next high-value tasks:

1. `BENCHPROD-094`: provide/build real `memtier_benchmark`, run `memtier_native_smoke.py --start-fusiondb --attempt-protocol-probe --fail-on-gap`, and fix the first concrete RESP/protocol gap.
2. `BENCHPROD-095`: provide/build LDBC SNB driver/datagen artifact, run `ldbc_snb_native_smoke.py`, and capture the first schema/import/command gap.
3. Extend CH-benCHmark from short native full smoke to longer mixed HTAP and all-query coverage once strict external blockers are reduced.
