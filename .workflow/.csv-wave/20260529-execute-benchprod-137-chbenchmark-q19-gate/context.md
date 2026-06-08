# BENCHPROD-137: CH-benCHmark Q19 query-class gate

## Purpose

Continue the production benchmark iteration by expanding CH-benCHmark command-mode one-hot query-class coverage from Q1-Q18 to Q1-Q19.

## Query Shape

BenchBase Q19 exercises:

- `item` to `order_line` join through `i_id = ol_i_id`
- top-level `OR` branches, where each branch repeats the same join equality and applies different brand/container/quantity/size filters
- residual branch filters after the common join predicate is extracted

FusionDB previously treated the top-level `OR` as opaque for conjunctive join extraction. The Q19 wave taught predicate splitting to lift common conjuncts out of `OR` branches, allowing the shared join equality to participate in the normal join path while preserving the residual disjunction.

## Changes

- Updated `E:\Playground\FusionDB\src\execution\scan\predicate.rs` to flatten nested conjuncts and extract common conjunctive predicates from disjunctive branches.
- Added regression coverage in `E:\Playground\FusionDB\tests\sql_join.rs` for the CH-benCHmark Q19-style common join key under `OR`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium.json` with required CH-benCHmark queries `Q1-Q19` and `min_passed_count = 19`.
- Updated `E:\Playground\FusionDB-bench\gate_profiles\production_medium_strict_native.json` with the same Q1-Q19 requirement.
- Updated `E:\Playground\FusionDB-bench\README.md` with BENCHPROD-137 evidence and commands.

## Evidence

- Q19 one-hot probe:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod137_q19_probe_after_or_fix_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `1/1`, Q19 completed `2`.
- Q1-Q19 combined matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod137_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_q19_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, passed `19/19`, each query completed `2`.
- Explicit strict native gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod137_strict_explicit_q1_q19_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.
- Explicit production medium gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod137_medium_explicit_q1_q19_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`, failures `0`.

## Verification

- `cargo test collect_conjunctive_predicates_lifts_common_or_join_key --lib`
- `cargo test collect_conjunctive_predicates_flattens_nested_on_clause --lib`
- `cargo test test_or_branch_common_join_key_matches_chbenchmark_q19_shape --test sql_join`
- `cargo test test_implicit_join_common_or_equi_predicate_matches_chbenchmark_q19_shape --test sql_join`
- `cargo test --test sql_join`
- `cargo test --test sql_group_aggregate`
- `cargo build --release`

## Next

- Continue to CH-benCHmark Q20 one-hot probe.
- Do not raise the CH-benCHmark query-class gate to Q1-Q20 until Q20 one-hot and Q1-Q20 combined matrix pass.
- This remains command-mode one-hot coverage, not official CH-benCHmark score or long-duration mixed HTAP certification.
