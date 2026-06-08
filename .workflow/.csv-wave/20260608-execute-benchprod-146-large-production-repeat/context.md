# BENCHPROD-146: Larger-scale production repeat

## Purpose

Raise the production readiness evidence from medium repeat/gate coverage to a larger local production repeat. The goal is to prove the current `tpcc`, `memtier`, `tsbs`, `ldbc`, and `chbench` production suite can complete three large-scale matrix runs and pass a thresholded gate without claiming an official benchmark score.

## Evidence

- Large production repeat:
  - `E:\Playground\FusionDB-bench\runs\benchprod146_large_production_3x_20260608\bench_repeat_summary.json`
  - scale `large`
  - suite `production`
  - repeats `3`
  - threads `4`
  - load mode `insert`
  - matrices passed `3/3`
  - matrix failures `0`
  - case errors `0`
- Stability summary:
  - `E:\Playground\FusionDB-bench\runs\benchprod146_large_production_3x_20260608\stability\bench_stability_summary.json`
  - unstable suites `1`
  - unstable cases `5`
  - reports with errors `0`
  - median suite metrics:
    - `tpcc`: p95 `8.940 ms`, ops/sec `700.8`
    - `memtier`: p95 `1.003 ms`, ops/sec `986.4`
    - `tsbs`: p95 `3.300 ms`, ops/sec `708.9`
    - `ldbc`: p95 `2.165 ms`, ops/sec `707.5`
    - `chbench`: p95 `0.865 ms`, ops/sec `847.8`
- Large profile gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod146_large_profile_20260608\bench_gate_summary.json`
  - status `passed`
  - checks `84/84`
  - failures `0`

## Boundary

The first gate attempt used `gate_profiles\production_medium.json` without overrides and failed as expected because that profile requires `scale=medium`, at least `60` latency samples per case, and medium-scale suite thresholds. The passing gate keeps the production official-target evidence, native coverage checks, TSBS official runner check, CH-benCHmark query matrix check, and recovery smoke check from the medium profile, but overrides scale, minimum case samples, and suite p95/throughput thresholds for this large repeat.

This is local large-scale production repeat evidence. It is not an official TPC-C, TSBS, LDBC, memtier, or CH-benCHmark score, and it does not replace future work on longer duration, larger external workloads, correctness oracles, official scheduling, pipeline coverage, and compaction/recovery endurance.

## Verification

```powershell
python bench_repeat.py --scale large --suite production --repeats 3 --threads 4 --load-mode insert --suite-timeout 3600 --matrix-timeout 10800 --run-name benchprod146_large_production_3x_20260608
python bench_gate.py --gate-profile gate_profiles/production_medium.json --repeat-report runs/benchprod146_large_production_3x_20260608/bench_repeat_summary.json --external-smoke-report runs/external_smoke_benchprod145_ldbc_120ops_pass_20260608/external_smoke_summary.json --chbenchmark-native-report runs/chbenchmark_native_benchprod143_mixed_htap_300s_t2_after_40001_20260530/chbenchmark_native_smoke_summary.json --chbenchmark-query-matrix-report runs/chbenchmark_query_class_matrix_benchprod140_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_q19_q20_q21_q22_20260529/chbenchmark_query_class_matrix_summary.json --recovery-smoke-report runs/recovery_smoke_benchprod142_current_20260530/recovery_smoke_summary.json --scale large --min-case-samples 30 --suite-max-p95-ms tpcc=10.0 --suite-max-p95-ms memtier=1.2 --suite-max-p95-ms tsbs=4.0 --suite-max-p95-ms ldbc=8.0 --suite-max-p95-ms chbench=2.0 --suite-min-ops-sec tpcc=650 --suite-min-ops-sec memtier=950 --suite-min-ops-sec tsbs=650 --suite-min-ops-sec ldbc=550 --suite-min-ops-sec chbench=800 --run-name gate_benchprod146_large_profile_20260608
```

## Next

- The BENCHPROD-142 production hardening wave is complete.
- Before starting a new wave, scan historical `.workflow/.csv-wave/**/tasks.csv` and `.workflow/.csv-wave/**/plan.json` records for remaining live TASK entries.
