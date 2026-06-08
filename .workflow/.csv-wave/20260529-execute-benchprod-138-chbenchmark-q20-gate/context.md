# BENCHPROD-138: CH-benCHmark Q20 query-class gate

## Purpose

Continue the production benchmark iteration by expanding CH-benCHmark command-mode one-hot query-class coverage from Q1-Q19 to Q1-Q20.

## Query Shape

BenchBase Q20 exercises:

- outer `supplier` and `nation` filter with `su_suppkey IN (subquery)`
- subquery joins `stock`, `item`, and `order_line`
- subquery projection `mod(s_i_id * s_w_id, 10000)`
- multi-column `GROUP BY s_i_id, s_w_id, s_quantity`
- `HAVING 2*s_quantity > sum(ol_quantity)`

The initial Q20 probe reached FusionDB execution and failed with `Unsupported expression in GROUP BY projection`. The root cause was that final grouped projection could evaluate aggregates, direct group expressions, and a few scalar cases, but not a scalar function built only from grouped columns.

## Changes

- Updated `E:\Playground\FusionDB\src\execution\expr\mod.rs` so grouped final projection can evaluate scalar functions whose arguments recursively resolve through group keys and aggregate values.
- Added `test_group_by_projection_scalar_function_from_group_columns` in `E:\Playground\FusionDB\tests\sql_group_aggregate.rs` for the Q20-style `MOD(s_i_id * s_w_id, 10000)` grouped projection plus `HAVING`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json` with required CH-benCHmark queries `Q1-Q20` and `min_passed_count = 20`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json` with the same Q1-Q20 requirement.
- Updated `E:\Playground\FusionDB-bench\README.md` with BENCHPROD-138 evidence and commands.

## Evidence

- Initial Q20 one-hot probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod138_q20_probe_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `gap`, passed `0/1`, error `Unsupported expression in GROUP BY projection`.
- Q20 one-hot probe after grouped scalar projection fix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod138_q20_probe_after_group_projection_fix_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `1/1`, Q20 completed `2`.
- Q1-Q20 combined matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod138_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_q19_q20_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `20/20`, each query completed `2`.
- Explicit strict native gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod138_strict_explicit_q1_q20_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.
- Explicit production medium gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod138_medium_explicit_q1_q20_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.

## Verification

- `cargo check`
- `cargo test test_group_by_projection_scalar_function_from_group_columns --test sql_group_aggregate`
- `cargo test collect_conjunctive_predicates_lifts_common_or_join_key --lib`
- `cargo test --test sql_group_aggregate`
- `cargo build --release --bin fusiondb`
- `python -m json.tool gate_profiles\production_medium.json`
- `python -m json.tool gate_profiles\production_medium_strict_native.json`

## Next

- Continue to CH-benCHmark Q21 one-hot probe.
- Do not raise the CH-benCHmark query-class gate to Q1-Q21 until Q21 one-hot and Q1-Q21 combined matrix pass.
- This remains command-mode one-hot coverage, not official CH-benCHmark score or long-duration mixed HTAP certification.
