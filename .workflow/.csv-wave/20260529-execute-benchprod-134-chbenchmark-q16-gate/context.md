# BENCHPROD-134: CH-benCHmark Q16 query-class gate

## Purpose

Continue the production benchmark iteration by expanding CH-benCHmark command-mode one-hot query-class coverage from Q1-Q15 to Q1-Q16.

## Query Shape

BenchBase Q16 exercises:

- `substring(i_data from 1 for 3)`
- `count(DISTINCT mod((s_w_id * s_i_id), 10000))`
- `NOT LIKE`
- `NOT IN (SELECT su_suppkey FROM supplier WHERE su_comment LIKE '%bad%')`
- `GROUP BY` on a select-list alias
- `ORDER BY supplier_cnt DESC`

The current FusionDB build already supports this BenchBase Q16 command path; no FusionDB code change was required in this wave.

## Changes

- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json` with required CH-benCHmark queries `Q1-Q16` and `min_passed_count = 16`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json` with the same Q1-Q16 requirement.
- Updated `E:\Playground\FusionDB-bench\README.md` with BENCHPROD-134 evidence and commands.

## Evidence

- Q16 one-hot probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod134_q16_probe_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `1/1`, Q16 completed `2`.
- Q1-Q16 combined matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod134_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `16/16`, each query completed `2`.
- Explicit strict native gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod134_strict_explicit_q1_q16_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.
- Explicit production medium gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod134_medium_explicit_q1_q16_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.

## Next

- Continue to CH-benCHmark Q17 one-hot probe.
- Do not raise the CH-benCHmark query-class gate to Q1-Q17 until Q17 one-hot and Q1-Q17 combined matrix pass.
- This remains command-mode one-hot coverage, not official CH-benCHmark score or long-duration mixed HTAP certification.
