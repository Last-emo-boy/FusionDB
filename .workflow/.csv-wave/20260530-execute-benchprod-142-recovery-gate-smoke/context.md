# BENCHPROD-142: Recovery gate smoke refresh

## Purpose

Continue production hardening by refreshing recovery evidence on the current release binary after BENCHPROD-141. The goal is to prove that the current FusionDB build still passes checkpoint recovery, forced-kill recovery, and WAL replay recovery, and that `bench_gate.py` can consume that report as a gate input.

## Scope

This wave uses the existing `E:\Playground\FusionDB-bench\recovery_smoke.py` harness. No FusionDB code change was required.

The smoke covers:

- table/index creation through the HTTP query API
- checkpoint through `POST /checkpoint`
- graceful restart after checkpoint
- indexed and FTS query correctness after checkpoint recovery
- WAL-only insert/update/delete after restart
- forced process kill without graceful shutdown
- restart and WAL replay validation

## Evidence

- Recovery smoke:
  - `E:\Playground\FusionDB-bench\runs\recovery_smoke_benchprod142_current_20260530\recovery_smoke_summary.json`
  - status `passed`
  - steps `15/15`
  - coverage: checkpoint recovery, WAL replay recovery, forced kill
- Production medium gate with explicit recovery report:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod142_medium_recovery_current_20260530\bench_gate_summary.json`
  - status `passed`
  - checks `60/60`
  - failures `0`

## Verification

```powershell
python recovery_smoke.py --fusiondb-bin E:\Playground\FusionDB\target\release\fusiondb.exe --run-name recovery_smoke_benchprod142_current_20260530 --fail-on-gap
python bench_gate.py --gate-profile gate_profiles\production_medium.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --chbenchmark-query-matrix-report runs\chbenchmark_query_class_matrix_benchprod140_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_q19_q20_q21_q22_20260529\chbenchmark_query_class_matrix_summary.json --recovery-smoke-report runs\recovery_smoke_benchprod142_current_20260530\recovery_smoke_summary.json --no-external-smoke --run-name gate_benchprod142_medium_recovery_current_20260530
```

## Next

- BENCHPROD-143 should extend CH-benCHmark mixed HTAP from `60s` to longer/larger evidence.
- Keep recovery smoke in the production gate input path; do not treat it as full power-loss or filesystem fault-injection certification.
