# BENCHPROD-139: CH-benCHmark Q21 query-class gate

## Purpose

Continue the production benchmark iteration by expanding CH-benCHmark command-mode one-hot query-class coverage from Q1-Q20 to Q1-Q21.

## Query Shape

BenchBase Q21 exercises:

- implicit multi-table join across `supplier`, aliased `order_line l1`, `oorder`, `stock`, and `nation`
- supplier key expression `mod((s_w_id * s_i_id),10000) = su_suppkey`
- timestamp comparison `l1.ol_delivery_d > o_entry_d`
- correlated `NOT EXISTS` against aliased `order_line l2`
- grouped aggregate `count(*) AS numwait`
- `ORDER BY numwait DESC, su_name`

The current FusionDB build already supports this BenchBase Q21 command path; no FusionDB code change was required in this wave.

## Changes

- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json` with required CH-benCHmark queries `Q1-Q21` and `min_passed_count = 21`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json` with the same Q1-Q21 requirement.
- Updated `E:\Playground\FusionDB-bench\README.md` with BENCHPROD-139 evidence and commands.

## Evidence

- Q21 one-hot probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod139_q21_probe_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `1/1`, Q21 completed `2`.
- Q1-Q21 combined matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod139_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_q19_q20_q21_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `21/21`, each query completed `2`.
- Explicit strict native gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod139_strict_explicit_q1_q21_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.
- Explicit production medium gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod139_medium_explicit_q1_q21_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.

## Verification

- `python -m json.tool gate_profiles\production_medium.json`
- `python -m json.tool gate_profiles\production_medium_strict_native.json`

## Next

- Continue to CH-benCHmark Q22 one-hot probe.
- Do not raise the CH-benCHmark query-class gate to Q1-Q22 until Q22 one-hot and Q1-Q22 combined matrix pass.
- This remains command-mode one-hot coverage, not official CH-benCHmark score or long-duration mixed HTAP certification.
