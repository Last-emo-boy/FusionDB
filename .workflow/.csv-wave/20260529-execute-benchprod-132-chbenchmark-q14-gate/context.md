# BENCHPROD-132: CH-benCHmark Q14 query-class gate

## Purpose

Continue the production benchmark iteration by expanding CH-benCHmark command-mode one-hot query-class coverage from Q1-Q13 to Q1-Q14.

## Query Shape

BenchBase Q14 computes promotional revenue over `order_line` joined to `item`:

- `ol_i_id = i_id`
- `ol_delivery_d` range filter
- `SUM(CASE WHEN i_data LIKE 'PR%' THEN ol_amount ELSE 0 END)`
- division by `1 + SUM(ol_amount)`

No FusionDB code change was required in this wave; the current release build already supports this query shape.

## Changes

- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json` with required CH-benCHmark queries `Q1-Q14` and `min_passed_count = 14`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json` with the same Q1-Q14 requirement.
- Updated `E:\Playground\FusionDB-bench\README.md` with BENCHPROD-132 evidence and commands.

## Evidence

- Release build:
  - `cargo build --release`
- Q14 one-hot probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod132_q14_probe_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `1/1`, Q14 completed `2`.
- Q1-Q14 combined matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod132_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `14/14`, each query completed `2`.
- Explicit strict native gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod132_strict_explicit_q1_q14_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.
- Explicit production medium gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod132_medium_explicit_q1_q14_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.

## Next

- Continue to CH-benCHmark Q15 one-hot probe.
- Do not raise the CH-benCHmark query-class gate to Q1-Q15 until Q15 one-hot and Q1-Q15 combined matrix pass.
- This remains command-mode one-hot coverage, not official CH-benCHmark score or long-duration mixed HTAP certification.
