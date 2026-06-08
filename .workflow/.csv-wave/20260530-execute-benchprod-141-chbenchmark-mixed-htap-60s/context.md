# BENCHPROD-141: CH-benCHmark 60s mixed HTAP

## Purpose

Continue the production benchmark iteration after Q1-Q22 one-hot coverage by running a longer BenchBase `tpcc,chbenchmark` mixed HTAP workload for 60 seconds through the native PgWire/JDBC path.

This wave is scoped as local sustained HTAP evidence. It is not an official CH-benCHmark score and does not close the larger production blockers around crash/recovery, larger scale, or official mixed scheduling.

## Initial Gaps

- The first run used JDK 17 and failed before workload execution:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_native_benchprod141_mixed_htap_60s_20260529\chbenchmark_native_smoke_summary.json`
  - BenchBase was compiled for class file version `67.0`; JDK 17 only recognizes up to `61.0`.
- Re-running with JDK 23 reached real workload execution but failed with one unknown `OrderStatus` transaction:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_native_benchprod141_mixed_htap_60s_jdk23_20260529\chbenchmark_native_smoke_summary.json`
  - status `gap`, completed `24` transactions, covering 4 TPC-C classes and 9 CH query classes before failure.
  - BenchBase reported `No order records for CUSTOMER [C_W_ID=1, C_D_ID=2, C_ID=1480]`.

## Root Cause

The blocker was not missing TPC-C data. The recovered run data contained the matching `oorder` row, and an unrestricted query could find it.

The failure came from a `WHERE + LIMIT` and ordered composite-index scan path that could stop too early. BenchBase `OrderStatus` uses a query shaped like:

```sql
SELECT o_id, o_carrier_id, o_entry_d
FROM oorder
WHERE o_w_id = ?
  AND o_d_id = ?
  AND o_c_id = ?
ORDER BY o_id DESC
LIMIT 1
```

The execution path needed to preserve enough ordered index candidates to skip stale or non-matching rows while still stopping row fetch after enough valid ordered rows had been collected.

## Changes

- `src/execution/query/mod.rs`
  - restricted unordered `LIMIT` pushdown so `WHERE + LIMIT` does not truncate before predicate evaluation.
- `src/execution/composite_index.rs`
  - retained ordered composite-index candidate behavior for matching `ORDER BY` direction while avoiding unsafe early truncation when predicates are not fully index-covered.
- `src/execution/scan/mod.rs`
  - applied ordered row-fetch limits after valid rows are collected, so stale/non-matching candidates can be skipped without reading beyond the ordered result window.
- `tests/sql_dml.rs`
  - added `test_tpcc_order_status_limit_finds_late_composite_index_match`.
  - batched the `test_fusion_storage_tpcc_order_fk_chain_after_many_customers` customer load so the regression keeps the same 6000-row TPC-C FK-chain coverage without timing out under the test-only `memtable_flush_mb = 0` setting.

## Evidence

- Passing 60s mixed HTAP after the fix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_native_benchprod141_mixed_htap_60s_after_limit_fix_20260530\chbenchmark_native_smoke_summary.json`
  - status `passed`
  - completed `38` transactions
  - covered TPC-C classes: `NewOrder`, `Payment`, `OrderStatus`, `Delivery`
  - covered CH query classes: `Q5`, `Q6`, `Q8`, `Q9`, `Q11`, `Q12`, `Q14`, `Q16`, `Q17`, `Q18`
- Regression tests:
  - `cargo test test_tpcc_order_status_limit_finds_late_composite_index_match --test sql_dml`
  - `cargo test test_tpcc_order_status_uses_filter_columns_outside_projection --test sql_dml`
  - `cargo test test_select_with_limit --test sql_select`
  - `cargo test test_correlated_not_exists_filters_before_limit --test sql_set_subquery`
  - `cargo test --test sql_dml`
  - `cargo test --test sql_index_cache`
  - `cargo check`
  - `cargo build --release --bin fusiondb`

## Verification

```powershell
$env:JAVA_HOME='E:\Playground\tools\jdk-23'
$env:Path='E:\Playground\tools\jdk-23\bin;' + $env:Path
python chbenchmark_native_smoke.py --chbenchmark-artifact E:\Playground\benchbase --run-mode full --duration 60 --terminals 1 --run-name chbenchmark_native_benchprod141_mixed_htap_60s_after_limit_fix_20260530 --fusiondb-bin E:\Playground\FusionDB\target\release\fusiondb.exe --jdbc-driver E:\Playground\tools\postgresql-jdbc\postgresql-42.7.11.jar --tool-timeout 1200 --fail-on-gap
```

## Next

- Decide whether the 60s mixed HTAP report should become a production gate input or stay as certification evidence.
- Extend from 60s local HTAP to longer duration, larger scale, more terminals, and recovery/forced-kill/WAL replay validation.
