# BENCHPROD-131: CH-benCHmark Q13 query-class gate

## Purpose

Continue the production benchmark iteration by fixing the CH-benCHmark Q13 timeout and expanding command-mode one-hot query-class coverage from Q1-Q12 to Q1-Q13.

## Root Cause

BenchBase Q13 emits the left outer join condition as `ON (...)`. FusionDB's conjunctive predicate splitter only split direct `AND` binary expressions and did not first unwrap `Expr::Nested`. As a result, Q13's join keys:

- `c_w_id = o_w_id`
- `c_d_id = o_d_id`
- `c_id = o_c_id`

were hidden inside a single nested expression. Join key extraction could not build the hash join plan, leaving Q13 on the slow fallback path until the 420 second case timeout.

## Changes

- Updated `src/execution/scan/predicate.rs` so `collect_conjunctive_predicates` and its predicate count helper flatten `Expr::Nested` before splitting `AND`.
- Added a unit regression proving nested `ON (...)` conditions split into 4 predicates.
- Added a SQL regression test matching the Q13 shape: `LEFT OUTER JOIN ON (...)`, right-side residual predicate, inner `GROUP BY`, outer `GROUP BY`, and `ORDER BY`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json` with required CH-benCHmark queries `Q1-Q13` and `min_passed_count = 13`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json` with the same Q1-Q13 requirement.
- Updated `E:\Playground\FusionDB-bench\README.md` with BENCHPROD-131 evidence and commands.

## Evidence

- Targeted predicate splitter regression:
  - `cargo test collect_conjunctive_predicates_flattens_nested_on_clause --lib`
- Targeted Q13 SQL shape regression:
  - `cargo test test_left_join_nested_on_group_by_matches_chbenchmark_q13_shape --test sql_join`
- Wider local regression:
  - `cargo test --test sql_join`
  - `cargo test --test sql_group_aggregate`
- Release build:
  - `cargo build --release`
- Q13 one-hot probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod131_q13_probe_after_nested_on_fix_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `1/1`, Q13 completed `2`.
- Q1-Q13 combined matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod131_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `13/13`, each query completed `2`.
- Explicit strict native gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod131_strict_explicit_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.

## Next

- Continue to CH-benCHmark Q14 one-hot probe or longer mixed HTAP evidence.
