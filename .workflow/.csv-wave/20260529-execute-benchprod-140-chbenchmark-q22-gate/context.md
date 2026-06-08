# BENCHPROD-140: CH-benCHmark Q22 query-class gate

## Purpose

Continue the production benchmark iteration by expanding CH-benCHmark command-mode one-hot query-class coverage from Q1-Q21 to Q1-Q22, completing the BenchBase CH-benCHmark query-class frontier.

## Query Shape

BenchBase Q22 exercises:

- `substring(c_state from 1 for 1)` projection and grouping
- `substring(c_phone from 1 for 1) IN (...)` filter
- scalar subquery `avg(c_balance)` with repeated substring predicate
- correlated `NOT EXISTS` against `oorder`
- `ORDER BY substring(c_state,1,1)`

The current FusionDB build already supports this BenchBase Q22 command path; no FusionDB code change was required in this wave.

## Changes

- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json` with required CH-benCHmark queries `Q1-Q22` and `min_passed_count = 22`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json` with the same Q1-Q22 requirement.
- Updated `E:\Playground\FusionDB-bench\README.md` with BENCHPROD-140 evidence and commands.

## Evidence

- Q22 one-hot probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod140_q22_probe_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `1/1`, Q22 completed `2`.
- Q1-Q22 combined matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod140_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_q19_q20_q21_q22_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `22/22`, each query completed `2`.
- Explicit strict native gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod140_strict_explicit_q1_q22_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.
- Explicit production medium gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod140_medium_explicit_q1_q22_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.

## Verification

- `python -m json.tool gate_profiles\production_medium.json`
- `python -m json.tool gate_profiles\production_medium_strict_native.json`
- `python chbenchmark_query_class_matrix.py --chbenchmark-artifact E:\Playground\benchbase --queries Q22 --run-name chbenchmark_query_class_matrix_benchprod140_q22_probe_20260529 --fusiondb-bin E:\Playground\FusionDB\target\release\fusiondb.exe --java-bin E:\Playground\tools\jdk-23\bin\java.exe --jdbc-driver E:\Playground\tools\postgresql-jdbc\postgresql-42.7.11.jar --case-duration 5 --fail-on-gap`
- `python chbenchmark_query_class_matrix.py --chbenchmark-artifact E:\Playground\benchbase --queries Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10,Q11,Q12,Q13,Q14,Q15,Q16,Q17,Q18,Q19,Q20,Q21,Q22 --run-name chbenchmark_query_class_matrix_benchprod140_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_q19_q20_q21_q22_20260529 --fusiondb-bin E:\Playground\FusionDB\target\release\fusiondb.exe --java-bin E:\Playground\tools\jdk-23\bin\java.exe --jdbc-driver E:\Playground\tools\postgresql-jdbc\postgresql-42.7.11.jar --case-duration 5 --fail-on-gap`
- `python bench_gate.py --gate-profile gate_profiles\production_medium_strict_native.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --chbenchmark-query-matrix-report runs\chbenchmark_query_class_matrix_benchprod140_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_q19_q20_q21_q22_20260529\chbenchmark_query_class_matrix_summary.json --no-external-smoke --no-recovery-smoke --run-name gate_benchprod140_strict_explicit_q1_q22_20260529`
- `python bench_gate.py --gate-profile gate_profiles\production_medium.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --chbenchmark-query-matrix-report runs\chbenchmark_query_class_matrix_benchprod140_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_q19_q20_q21_q22_20260529\chbenchmark_query_class_matrix_summary.json --no-external-smoke --no-recovery-smoke --run-name gate_benchprod140_medium_explicit_q1_q22_20260529`

## Next

- Move from CH-benCHmark command-mode one-hot coverage to official-score or longer mixed HTAP evidence.
- Keep full production blockers explicit: official CH-benCHmark score, sustained HTAP behavior, crash/recovery proof, larger scale, native memtier, and full official LDBC mixed update coverage remain open.
