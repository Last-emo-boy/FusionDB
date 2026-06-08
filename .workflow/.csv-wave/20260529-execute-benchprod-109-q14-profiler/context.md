# BENCHPROD-109: Q14 profiler

## Purpose

Continue BENCHPROD-108 from the full test-data non-isolation LDBC timeout. The goal was to stop treating Q14 as a black-box timeout and add targeted stage evidence that can guide the next production-readiness optimization.

## Changes

- Added `E:\Playground\FusionDB-bench\ldbc_q14_profile.py`.
- The profiler reuses the existing FusionDB bench harness pieces:
  - FusionDB server startup and health checks;
  - PostgreSQL JDBC driver resolution;
  - LDBC PostgreSQL test-data preload from `ldbc_snb_native_smoke.py`.
- Added a Java/JDBC profiler compiled into each run directory.
- The profiler executes Q14 in stages with independent timeouts:
  - `search_graph`
  - `paths`
  - `edges`
  - `unique_edges`
  - `weightedpaths`
- Reports are written as JSON and Markdown under `E:\Playground\FusionDB-bench\runs\...`.

## Evidence

- `python -m py_compile ldbc_q14_profile.py`: passed.
- `cargo fmt --check`: passed.
- Prefix40 profiler smoke passed:
  - `E:\Playground\FusionDB-bench\runs\ldbc_q14_profile_benchprod109_prefix40_smoke3_20260529\ldbc_q14_profile_summary.json`
  - `search_graph`: 129 rows, max depth 4, 22 ms.
  - `paths`: 1 path, 20 ms.
  - `edges`: 4 edges, 21 ms.
  - `unique_edges`: 4 unique edges, 22 ms.
  - `weightedpaths`: 1 weighted path, 377 ms.
- Full test-data profiler produced stage evidence:
  - `E:\Playground\FusionDB-bench\runs\ldbc_q14_profile_benchprod109_full_testdata_20260529\ldbc_q14_profile_summary.json`
  - `search_graph`: passed, 506 rows, max depth 2, 56 ms.
  - `paths`: passed, 7 paths, 60 ms.
  - `edges`: passed, 14 edges, 60 ms.
  - `unique_edges`: passed, 14 unique edges, 63 ms.
  - `weightedpaths`: gap, Java stage command timed out after 90 seconds.

## Result

Q14 full test-data is no longer an undifferentiated recursive CTE timeout. The recursive expansion and edge extraction stages are small and fast on the target pair. The active bottleneck is the `weightedpaths` stage, specifically message-pair scoring over `unique_edges, message p1, message p2` plus grouped aggregation.

This is targeted diagnostic evidence only. It is not a full official/native LDBC benchmark pass.

## Current Frontier

- Full native LDBC remains blocked by Q14.
- The next engineering target is no longer generic recursive CTE expansion; it is weighted path scoring.
- Likely optimization paths:
  - improve join ordering for `unique_edges, message p1, message p2`;
  - add/use indexes for `message.m_creatorid`, `message.m_c_replyof`, and `message.m_messageid`;
  - materialize candidate creator-pair message edges before the scoring union;
  - add a focused Q14 weightedpaths regression harness before changing broad join behavior.
- Native memtier remains blocked by missing real `memtier_benchmark` tooling.

## Next Task Candidate

BENCHPROD-110 should optimize Q14 `weightedpaths` message-pair scoring. The success criterion should be a measurable improvement in the full test-data Q14 profiler, not a claim of full native LDBC pass.
