# BENCHPROD-145: LDBC official-shape gap reduction

## Purpose

Refresh LDBC native evidence after the earlier recursive CTE, array, `generate_subscripts`, Q6 index, and queryDir compatibility work. The goal is to reduce the Query 12-14 official-shape blocker without claiming a full official LDBC benchmark pass.

## Evidence

- Native LDBC command-mode smoke:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod145_q12_q14_reduction_120ops_20260608\ldbc_snb_native_smoke_summary.json`
  - status `passed`
  - steps `13/13`
  - `operation_count=120`
  - `isolation_mode=false`
  - full PostgreSQL implementation test-data preload (`preload_max_rows_per_file=0`)
  - Q14 and Q6 support indexes enabled.
- Metrics:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod145_q12_q14_reduction_120ops_20260608\ldbc_results\fusiondb-results.json`
  - total operations `120`
  - throughput `24.237527772167237` ops/s.
  - sampled `LdbcQuery12` once at `99 ms`.
  - sampled `LdbcQuery13` twice at mean `88 ms`.
  - sampled `LdbcQuery14` once at `227 ms`.
- External smoke:
  - `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod145_ldbc_120ops_pass_20260608\external_smoke_summary.json`
  - links the passed LDBC native evidence.
- Production medium gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod145_medium_ldbc_120ops_pass_20260608\bench_gate_summary.json`
  - status `passed`
  - checks `84/84`
  - failures `0`

## Boundary

This is stronger than the earlier Q1-Q14 isolation matrix because it is not isolation mode and it sampled Q12, Q13, and Q14 inside one native command-mode run. It is still not a full official LDBC pass: update operations remain disabled in the generated properties, official mixed scheduling and scale are not certified, and top-level external smoke still records environment-level artifact variables as not configured unless explicit evidence paths are supplied.

## Verification

```powershell
cargo build --release --bin fusiondb
python ldbc_snb_native_smoke.py --ldbc-artifact E:\Playground\ldbc-snb\driver\target\driver-standalone.jar --ldbc-postgres-jar E:\Playground\ldbc-snb\impls\postgres\target\postgres-1.2.0-SNAPSHOT.jar --run-mode command --ldbc-command-preset postgres-interactive --preload-postgres-test-data --preload-max-rows-per-file 0 --create-q14-indexes --create-q6-indexes --operation-count 120 --duration 30 --warmup 0 --tool-timeout 420 --preload-timeout 420 --run-name ldbc_snb_native_benchprod145_q12_q14_reduction_120ops_20260608 --fail-on-gap
python external_smoke.py --target benchbase-tpcc,memtier,tsbs,ldbc,chbenchmark --memtier-native-evidence runs\memtier_native_benchprod144_wsl_probe_bind_all_20260608\memtier_native_smoke_summary.json --ldbc-native-evidence runs\ldbc_snb_native_benchprod145_q12_q14_reduction_120ops_20260608\ldbc_snb_native_smoke_summary.json --run-name external_smoke_benchprod145_ldbc_120ops_pass_20260608
python bench_gate.py --gate-profile gate_profiles\production_medium.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --external-smoke-report runs\external_smoke_benchprod145_ldbc_120ops_pass_20260608\external_smoke_summary.json --chbenchmark-native-report runs\chbenchmark_native_benchprod143_mixed_htap_300s_t2_after_40001_20260530\chbenchmark_native_smoke_summary.json --chbenchmark-query-matrix-report runs\chbenchmark_query_class_matrix_benchprod140_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_q19_q20_q21_q22_20260529\chbenchmark_query_class_matrix_summary.json --recovery-smoke-report runs\recovery_smoke_benchprod142_current_20260530\recovery_smoke_summary.json --run-name gate_benchprod145_medium_ldbc_120ops_pass_20260608
```

## Next

- Continue BENCHPROD-146: larger-scale production repeat.
- Keep future LDBC work focused on updates, official scheduling, larger scale, and environment-level artifact configuration.
