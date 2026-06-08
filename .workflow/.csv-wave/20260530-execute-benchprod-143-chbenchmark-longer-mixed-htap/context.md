# BENCHPROD-143: CH-benCHmark longer mixed HTAP

## Purpose

Extend BENCHPROD-141 from a `60s`, `1` terminal CH-benCHmark mixed HTAP run toward longer duration and higher concurrency evidence. This is local sustained HTAP hardening evidence through BenchBase `tpcc,chbenchmark`, PostgreSQL JDBC, and FusionDB PgWire. It is not an official CH-benCHmark score.

## Evidence

- `300s`, `1` terminal mixed HTAP:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_native_benchprod143_mixed_htap_300s_t1_20260530\chbenchmark_native_smoke_summary.json`
  - status `passed`
  - completed transactions `120`
  - TPC-C classes `5/5`
  - CH-benCHmark query classes `18/22`
- `300s`, `2` terminals before SQLSTATE fix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_native_benchprod143_mixed_htap_300s_t2_20260530\chbenchmark_native_smoke_summary.json`
  - status `gap`
  - completed transactions `252`
  - TPC-C classes `5/5`
  - CH-benCHmark query classes `21/22`
  - blocker: TPC-C `Payment` and `Delivery` write conflicts were returned as SQLSTATE `XX000`, so BenchBase counted them as unexpected SQL errors instead of retryable serialization failures.
- `120s`, `2` terminals after SQLSTATE fix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_native_benchprod143_mixed_htap_120s_t2_after_40001_20260530\chbenchmark_native_smoke_summary.json`
  - status `passed`
  - completed transactions `101`
  - TPC-C classes `5/5`
  - CH-benCHmark query classes `17/22`
  - BenchBase reports write conflicts under `Rejected Transactions (Server Retry)` and `Unexpected SQL Errors` is empty.
- `300s`, `2` terminals after SQLSTATE fix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_native_benchprod143_mixed_htap_300s_t2_after_40001_20260530\chbenchmark_native_smoke_summary.json`
  - status `passed`
  - completed transactions `243`
  - TPC-C classes `5/5`
  - CH-benCHmark query classes `20/22`
  - BenchBase reports write conflicts under `Rejected Transactions (Server Retry)` and `Unexpected SQL Errors` is empty.
- Gate with tightened CH native evidence and recovery:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod143_medium_chbenchmark_300s_t2_40001_recovery_20260530\bench_gate_summary.json`
  - status `passed`
  - checks `86/86`

## Code Changes

- `src/server/pg_server.rs`
  - Added FusionDB-to-PostgreSQL SQLSTATE mapping for PgWire errors.
  - Maps `FusionError::Storage("Write conflict: ...")` to SQLSTATE `40001` (`serialization_failure`).
  - Keeps other execution/storage errors on `XX000`.
- `tests/pg_integration.rs`
  - Added a FusionStorage-backed PgWire test that creates two concurrent transactions on the same row and verifies the conflicting `COMMIT` returns SQLSTATE `40001`.

## Verification

```powershell
cargo test test_pg_protocol_write_conflict_uses_serialization_failure_sqlstate --test pg_integration
cargo fmt --check
cargo check
cargo build --release --bin fusiondb

$env:JAVA_HOME='E:\Playground\tools\jdk-23'
$env:Path='E:\Playground\tools\jdk-23\bin;' + $env:Path
python chbenchmark_native_smoke.py --chbenchmark-artifact E:\Playground\benchbase --run-mode full --duration 300 --terminals 1 --run-name chbenchmark_native_benchprod143_mixed_htap_300s_t1_20260530 --fusiondb-bin E:\Playground\FusionDB\target\release\fusiondb.exe --jdbc-driver E:\Playground\tools\postgresql-jdbc\postgresql-42.7.11.jar --tool-timeout 1800 --fail-on-gap
python chbenchmark_native_smoke.py --chbenchmark-artifact E:\Playground\benchbase --run-mode full --duration 300 --terminals 2 --run-name chbenchmark_native_benchprod143_mixed_htap_300s_t2_after_40001_20260530 --fusiondb-bin E:\Playground\FusionDB\target\release\fusiondb.exe --jdbc-driver E:\Playground\tools\postgresql-jdbc\postgresql-42.7.11.jar --tool-timeout 1800 --fail-on-gap
python bench_gate.py --gate-profile gate_profiles\production_medium.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --chbenchmark-native-report runs\chbenchmark_native_benchprod143_mixed_htap_300s_t2_after_40001_20260530\chbenchmark_native_smoke_summary.json --chbenchmark-query-matrix-report runs\chbenchmark_query_class_matrix_benchprod140_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_q19_q20_q21_q22_20260529\chbenchmark_query_class_matrix_summary.json --recovery-smoke-report runs\recovery_smoke_benchprod142_current_20260530\recovery_smoke_summary.json --run-name gate_benchprod143_medium_chbenchmark_300s_t2_40001_recovery_20260530
```

## Next

- Continue BENCHPROD-144 native memtier completion.
- Use the full `300s`, `2` terminal mixed HTAP report as the current CH-benCHmark native evidence until a longer or larger run replaces it.
