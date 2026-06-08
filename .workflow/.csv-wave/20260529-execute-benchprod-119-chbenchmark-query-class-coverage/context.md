# BENCHPROD-119: CH-benCHmark query-class coverage visibility

## Purpose

BENCHPROD-118 promoted CH-benCHmark native full-run smoke into independent gate coverage, but the latest full-run evidence only proved lifecycle execution. BENCHPROD-119 makes the executed query and transaction classes visible in smoke summaries and gate reports.

## Changes

- Updated `E:\Playground\FusionDB-bench\chbenchmark_native_smoke.py`.
- Added BenchBase `Completed Transactions` histogram parsing with ANSI cleanup.
- Added `coverage` output for CH-benCHmark native smoke summaries:
  - `completed_transaction_counts`
  - `completed_total`
  - `covered_chbenchmark_queries`
  - `covered_tpcc_transactions`
  - observed/completed class counts
  - missing CH-benCHmark queries and TPC-C transactions
- Updated `E:\Playground\FusionDB-bench\bench_gate.py`.
- Added CH coverage derivation from either new `coverage` fields or existing `chbenchmark_execute.stdout.log` paths, so old evidence remains usable.
- Added gate checks:
  - `external_smoke.chbenchmark.completed_transactions`
  - `external_smoke.chbenchmark.query_classes`
  - `external_smoke.chbenchmark.tpcc_transaction_classes`
- Updated profile source text and conservative thresholds in:
  - `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json`
  - `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json`

## Evidence

- Source CH-benCHmark native report:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_native_benchbase_full_after_q6_q8_fix_20260528\chbenchmark_native_smoke_summary.json`
- Source BenchBase stdout:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_native_benchbase_full_after_q6_q8_fix_20260528\chbenchmark_execute.stdout.log`
- Parsed coverage:
  - Completed transactions: `4`
  - CH-benCHmark query classes completed: `1/22`
  - Covered CH-benCHmark queries: `Q3`
  - TPC-C transaction classes completed: `1/5`
  - Covered TPC-C transactions: `NewOrder`

## Verification

- `python -m py_compile chbenchmark_native_smoke.py bench_gate.py`: passed.
- `python -m json.tool gate_profiles\production_medium.json`: passed.
- `python -m json.tool gate_profiles\production_medium_strict_native.json`: passed.
- `chbenchmark_native_smoke.build_chbenchmark_coverage(...)`: produced `completed_total=4`, `Q3`, and `NewOrder`.
- CH-only gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod119_chbenchmark_query_coverage_only_20260529\bench_gate_summary.json`
  - Status: `passed`, 31/31 checks.
- Strict CH-only gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod119_strict_chbenchmark_query_coverage_only_20260529\bench_gate_summary.json`
  - Status: `passed`, 31/31 checks.
- Combined explicit independent gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod119_independent_ldbc_tsbs_ch_query_coverage_20260529\bench_gate_summary.json`
  - Status: `passed`, 46/46 checks.
- Combined explicit strict independent gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod119_strict_independent_ldbc_tsbs_ch_query_coverage_20260529\bench_gate_summary.json`
  - Status: `passed`, 46/46 checks.
- Combined discovery gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod119_independent_discovery_query_coverage_20260529\bench_gate_summary.json`
  - Status: `passed`, 46/46 checks.
- Combined strict discovery gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod119_strict_independent_discovery_query_coverage_20260529\bench_gate_summary.json`
  - Status: `passed`, 46/46 checks.

## Boundary

This is query-class coverage extraction and conservative gate visibility. It does not prove broad CH Q1-Q22 execution. The current latest full-run smoke completed only `Q3` for CH-benCHmark and `NewOrder` for TPC-C. A future forced-query or query-class matrix run is still required before claiming broad CH-benCHmark query coverage.

## Current Frontier

Good next BENCHPROD candidates:

- build a CH-benCHmark query-class matrix or forced-query runner for Q1-Q22;
- move native memtier from blocked to runnable by installing or building a real `memtier_benchmark`;
- deepen CH-benCHmark from short full-run smoke into longer mixed HTAP profiles.
