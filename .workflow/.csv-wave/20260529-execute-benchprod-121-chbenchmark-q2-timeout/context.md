# BENCHPROD-121: CH-benCHmark Q2 one-hot timeout

## Purpose

BENCHPROD-120 introduced a BenchBase command-mode one-hot CH-benCHmark query-class matrix and exposed `Q2` as the next blocker: `Q2` timed out after 420 seconds at `scale_factor=1`. BENCHPROD-121 fixes that timeout without broadening the gate default query set.

## Root Cause

The `Q2` shape combines a multi-relation comma join, a derived table, and expression equality predicates such as `MOD((s_w_id*s_i_id),10000)=su_suppkey`. The previous join planner handled simpler column equality joins and a narrow LDBC-specific comma-join reorder path, but it did not plan this CH-benCHmark shape early enough to avoid explosive intermediate rows.

## Changes

- Updated `E:\Playground\FusionDB\src\execution\scan\join.rs`.
  - Generalized conservative comma-join reordering from the LDBC Q4 special case to a broader guarded path based on predicate membership and row-count estimates.
  - Included derived table aliases in relation name discovery so derived-table predicates can participate in join planning.
  - Added expression join probe planning for indexed/primary right-side keys.
  - Added expression hash join planning for cases where one side is a real column and the other side is an expression over the opposite schema.
  - Relaxed the unique-index probe threshold for distinct probe keys.
  - Refreshed join predicate state after extracting derived-table predicates so later join planning sees the updated expression.
- Updated `E:\Playground\FusionDB\tests\sql_join.rs`.
  - Added `test_derived_table_join_matches_chbenchmark_q2_shape` as a miniature regression for the Q2 pattern.

## Evidence

- Prior failure frontier:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod120_q1_q3_probe_20260529\chbenchmark_query_class_matrix_summary.json`
  - `Q2` failed with `command timed out after 420 seconds`.
- Q2 positive matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod121_q2_probe_20260529\chbenchmark_query_class_matrix_summary.json`
  - Status: `passed`, 1/1.
  - Covered query: `Q2`.
  - Completed count for the Q2 case: `2`.
- Q1/Q2/Q3 matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod121_q1_q2_q3_20260529\chbenchmark_query_class_matrix_summary.json`
  - Status: `passed`, 3/3.
  - Covered queries: `Q1`, `Q2`, `Q3`.
- Strict explicit gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod121_strict_explicit_q1_q2_q3_20260529\bench_gate_summary.json`
  - Status: `passed`, 54/54 checks.

## Verification

- `cargo test --test sql_join`: passed, 25/25.
- `cargo build --release`: passed.
- CH-benCHmark Q2 one-hot query-class matrix: passed.
- CH-benCHmark Q1/Q2/Q3 one-hot query-class matrix: passed.
- Explicit strict production gate using the Q1/Q2/Q3 matrix report: passed.

## Boundary

This is BenchBase command-mode one-hot query-class evidence. It is not an official CH-benCHmark score and does not prove sustained mixed HTAP behavior. The default gate profile still requires `Q1` and `Q3`; promoting `Q2` into defaults is a separate policy change.

## Current State

Per user instruction, this task is stopped at BENCHPROD-121 completion. No next query-class expansion or new BENCHPROD work was started.
