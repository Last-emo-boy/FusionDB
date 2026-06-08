# BENCHPROD-110: Q14 weightedpaths expression-key probe

## Purpose

Continue BENCHPROD-109 from the Q14 profiler finding that full test-data recursive/path/edge stages were fast, while `weightedpaths` timed out after 90 seconds. The target was the message-pair scoring shape:

`unique_edges, message p1, message p2 WHERE p1.m_creatorid = e[1] ...`

The success criterion for this task was measurable full test-data Q14 profiler improvement. This task does not claim a full official/native LDBC pass.

## Changes

- Extended `E:\Playground\FusionDB-bench\ldbc_q14_profile.py` with `--create-q14-indexes`.
- Added profiler-side index creation for:
  - `message (m_creatorid)`;
  - `message (m_c_replyof)`;
  - `message (m_messageid)`.
- Extended `src/execution/expr/mod.rs` column extraction so `Expr::CompoundFieldAccess` with subscript access, such as `e[1]`, contributes its root column dependency.
- Extended `src/execution/scan/join.rs` with expression-key indexed probe planning:
  - supports an indexed right-table key compared to a left-side expression;
  - keeps unsupported shapes on the existing join path;
  - preserves residual predicate evaluation after probe lookup.
- Restored join projection-hint flow in `src/execution/query/mod.rs` so join scans can still avoid decoding unused columns.
- Preserved keyed probe decode safety by allowing the known probe key to be removed from decoded projection and restored from the probe key.
- Added `test_join_expr_probe_uses_indexed_subscript_key_projection` in `tests/sql_join.rs`.

## Evidence

- Index-only full test-data profiler remained blocked before the join expression-probe change:
  - `E:\Playground\FusionDB-bench\runs\ldbc_q14_profile_benchprod110_full_testdata_indexes_20260529\ldbc_q14_profile_summary.json`
  - `search_graph`: 57 ms, 506 rows, max depth 2.
  - `paths`: 59 ms, 7 paths.
  - `edges`: 62 ms, 14 edges.
  - `unique_edges`: 62 ms, 14 unique edges.
  - `weightedpaths`: gap, command timed out after 90 seconds.
- Prefix40 profiler after expression probe passed:
  - `E:\Playground\FusionDB-bench\runs\ldbc_q14_profile_benchprod110_prefix40_indexes_20260529\ldbc_q14_profile_summary.json`
  - `weightedpaths`: passed in 31 ms.
- Full test-data profiler after expression probe passed:
  - `E:\Playground\FusionDB-bench\runs\ldbc_q14_profile_benchprod110_full_testdata_expr_probe_20260529\ldbc_q14_profile_summary.json`
  - `search_graph`: 59 ms, 506 rows, max depth 2.
  - `paths`: 63 ms, 7 paths.
  - `edges`: 60 ms, 14 edges.
  - `unique_edges`: 68 ms, 14 unique edges.
  - `weightedpaths`: passed in 1116 ms, 7 weighted paths.

## Verification

- `python -m py_compile ldbc_q14_profile.py`: passed.
- `cargo fmt --check`: passed.
- `cargo test --test sql_join -- --nocapture`: 22/22 passed.
- `cargo test --test sql_set_subquery -- --nocapture`: 33/33 passed.
- `cargo test --release --test pg_integration -- --nocapture`: 27/27 passed.
- `cargo build --release --bin fusiondb`: passed.

## Result

Q14 `weightedpaths` is no longer the active timeout in the targeted full test-data profiler for the configured person pair. The before/after comparison is strong because indexes alone still timed out, while expression-key indexed probe lowered `weightedpaths` to 1116 ms.

This is profiler evidence only. It is not a full native LDBC workload pass and should not be reported as one.

## Current Frontier

- Re-run the native LDBC smoke with Q14 enabled to see whether this removes the prior 4-op timeout.
- If native LDBC still fails, separate database execution regressions from LDBC driver/query-file/update coverage gaps.
- Native memtier remains blocked by missing real `memtier_benchmark` tooling.
