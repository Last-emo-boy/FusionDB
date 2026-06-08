# BENCHPROD-144: Native memtier completion

## Purpose

Close the native memtier gap by running a real `memtier_benchmark` protocol probe against FusionDB's Redis-compatible RESP endpoint. This replaces the previous `tool_missing` blocker evidence with a passed native probe while keeping official-score claims conservative.

## Evidence

- Native memtier probe:
  - `E:\Playground\FusionDB-bench\runs\memtier_native_benchprod144_wsl_probe_bind_all_20260608\memtier_native_smoke_summary.json`
  - status `passed`
  - steps `5/5`
  - command source `WSL`
  - binary `/home/w33d/src/memtier_benchmark/memtier_benchmark`
  - protocol probe completed `12` ops through Redis protocol.
- External smoke:
  - `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod144_native_memtier_pass_20260608\external_smoke_summary.json`
  - links the passed native memtier evidence.
- Production medium gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod144_medium_native_memtier_pass_20260608\bench_gate_summary.json`
  - status `passed`
  - checks `84/84`
  - failures `0`

## Notes

The first real WSL probe against port `6380` reached the tool but timed out with `0` ops because FusionDB was bound to `127.0.0.1` while WSL targeted the Windows host gateway. The passing run used `--bind-host 0.0.0.0`, kept host health checks on `127.0.0.1`, and passed `--memtier-server auto-wsl-host`.

The native memtier pass proves a small Redis protocol probe through the FusionDB RESP endpoint. It does not certify complete Redis or Memcached semantics, pipelining coverage, or long-duration memtier behavior.

## Verification

```powershell
python memtier_native_smoke.py --detect-wsl --start-fusiondb --resp-preflight --attempt-protocol-probe --memtier-server auto-wsl-host --bind-host 0.0.0.0 --fusiondb-bin E:\Playground\FusionDB\target\release\fusiondb.exe --run-name memtier_native_benchprod144_wsl_probe_bind_all_20260608 --http-port 8095 --pg-port 8096 --target-port 6381 --requests 12 --clients 1 --threads 1 --tool-timeout 60 --startup-timeout 30 --fail-on-gap --fail-on-missing
python external_smoke.py --target benchbase-tpcc,memtier,tsbs,ldbc,chbenchmark --memtier-native-evidence runs\memtier_native_benchprod144_wsl_probe_bind_all_20260608\memtier_native_smoke_summary.json --run-name external_smoke_benchprod144_native_memtier_pass_20260608
python bench_gate.py --gate-profile gate_profiles\production_medium.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --external-smoke-report runs\external_smoke_benchprod144_native_memtier_pass_20260608\external_smoke_summary.json --chbenchmark-native-report runs\chbenchmark_native_benchprod143_mixed_htap_300s_t2_after_40001_20260530\chbenchmark_native_smoke_summary.json --chbenchmark-query-matrix-report runs\chbenchmark_query_class_matrix_benchprod140_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_q19_q20_q21_q22_20260529\chbenchmark_query_class_matrix_summary.json --recovery-smoke-report runs\recovery_smoke_benchprod142_current_20260530\recovery_smoke_summary.json --run-name gate_benchprod144_medium_native_memtier_pass_20260608
```

## Next

- Continue BENCHPROD-145: LDBC official-shape gap reduction.
- Keep full native memtier production hardening open for pipeline, concurrency, longer duration, and broader Redis/Memcached semantics.
