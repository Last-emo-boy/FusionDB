# BENCHPROD-135: CH-benCHmark Q17 query-class gate

## Purpose

Continue the production benchmark iteration by expanding CH-benCHmark command-mode one-hot query-class coverage from Q1-Q16 to Q1-Q17.

## Query Shape

BenchBase Q17 exercises:

- derived table with `AVG(ol_quantity)` and `GROUP BY i_id`
- `LIKE '%b'` filter inside the derived aggregate
- join from outer `order_line` to the derived table
- non-equi residual predicate `ol_quantity < t.a`
- scalar arithmetic in the final projection: `SUM(ol_amount) / 2.0`

The current FusionDB build already supports this BenchBase Q17 command path; no FusionDB code change was required in this wave.

## Changes

- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json` with required CH-benCHmark queries `Q1-Q17` and `min_passed_count = 17`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json` with the same Q1-Q17 requirement.
- Updated `E:\Playground\FusionDB-bench\README.md` with BENCHPROD-135 evidence and commands.

## Evidence

- Q17 one-hot probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod135_q17_probe_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `1/1`, Q17 completed `2`.
- Q1-Q17 combined matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod135_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `17/17`, each query completed `2`.
- Explicit strict native gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod135_strict_explicit_q1_q17_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.
- Explicit production medium gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod135_medium_explicit_q1_q17_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.

## Next

- Continue to CH-benCHmark Q18 one-hot probe.
- Do not raise the CH-benCHmark query-class gate to Q1-Q18 until Q18 one-hot and Q1-Q18 combined matrix pass.
- This remains command-mode one-hot coverage, not official CH-benCHmark score or long-duration mixed HTAP certification.
