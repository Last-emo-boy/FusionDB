# BENCHPROD-133: CH-benCHmark Q15 query-class gate

## Purpose

Continue the production benchmark iteration by expanding CH-benCHmark command-mode one-hot query-class coverage from Q1-Q14 to Q1-Q15.

## Query Shape

BenchBase Q15 creates and drops a temporary view around the selected query:

- `CREATE VIEW revenue0 (supplier_no, total_revenue) AS ...`
- join `order_line` to `stock`
- `GROUP BY supplier_no`
- query `supplier, revenue0`
- scalar subquery `SELECT max(total_revenue) FROM revenue0`
- `DROP VIEW revenue0`

The current FusionDB build already supports this BenchBase Q15 command path; no FusionDB code change was required in this wave.

## Changes

- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json` with required CH-benCHmark queries `Q1-Q15` and `min_passed_count = 15`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json` with the same Q1-Q15 requirement.
- Updated `E:\Playground\FusionDB-bench\README.md` with BENCHPROD-133 evidence and commands.

## Evidence

- Q15 one-hot probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod133_q15_probe_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `1/1`, Q15 completed `2`.
- Q1-Q15 combined matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod133_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `15/15`, each query completed `2`.
- Explicit strict native gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod133_strict_explicit_q1_q15_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.
- Explicit production medium gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod133_medium_explicit_q1_q15_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.

## Next

- Continue to CH-benCHmark Q16 one-hot probe.
- Do not raise the CH-benCHmark query-class gate to Q1-Q16 until Q16 one-hot and Q1-Q16 combined matrix pass.
- This remains command-mode one-hot coverage, not official CH-benCHmark score or long-duration mixed HTAP certification.
