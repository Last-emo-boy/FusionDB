# BENCHPROD-136: CH-benCHmark Q18 query-class gate

## Purpose

Continue the production benchmark iteration by expanding CH-benCHmark command-mode one-hot query-class coverage from Q1-Q17 to Q1-Q18.

## Query Shape

BenchBase Q18 exercises:

- three-table join across `customer`, `oorder`, and `order_line`
- composite join predicates on warehouse, district, order, and customer keys
- grouped aggregate `sum(ol_amount) AS amount_sum`
- `HAVING sum(ol_amount) > 200`
- `ORDER BY amount_sum DESC, o_entry_d`

The current FusionDB build already supports this BenchBase Q18 command path; no FusionDB code change was required in this wave.

## Changes

- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json` with required CH-benCHmark queries `Q1-Q18` and `min_passed_count = 18`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json` with the same Q1-Q18 requirement.
- Updated `E:\Playground\FusionDB-bench\README.md` with BENCHPROD-136 evidence and commands.

## Evidence

- Q18 one-hot probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod136_q18_probe_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `1/1`, Q18 completed `2`.
- Q1-Q18 combined matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod136_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `18/18`, each query completed `2`.
- Explicit strict native gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod136_strict_explicit_q1_q18_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.
- Explicit production medium gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod136_medium_explicit_q1_q18_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.

## Next

- Continue to CH-benCHmark Q19 one-hot probe.
- Do not raise the CH-benCHmark query-class gate to Q1-Q19 until Q19 one-hot and Q1-Q19 combined matrix pass.
- This remains command-mode one-hot coverage, not official CH-benCHmark score or long-duration mixed HTAP certification.
