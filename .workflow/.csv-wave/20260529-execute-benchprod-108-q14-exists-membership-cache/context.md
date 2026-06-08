# BENCHPROD-108: Q14 EXISTS membership cache

## Purpose

Continue the production-readiness bench track from BENCHPROD-107. The previous disable-Q14 full test-data isolation run completed `ldbc_command`, but non-isolation full test-data LDBC still timed out with Q14 enabled. BENCHPROD-108 targets the Q14 recursive CTE path without claiming any bounded or gap evidence as a full native benchmark pass.

## Changes

- Added a narrow fast path in `src/execution/expr/subquery.rs` for deferred `EXISTS` / `NOT EXISTS` filters whose subquery is a single-table membership check:
  - optimized shape: `EXISTS (SELECT * FROM table alias WHERE alias.column = outer_expr)`;
  - the local column must be explicitly qualified by the subquery table or alias;
  - unsupported shapes continue to use the existing bound-subquery fallback.
- The fast path builds one `HashSet<Value>` per local table/column during `filter_rows_with_subqueries`, then evaluates each outer row through set membership instead of re-running the same subquery scan.
- Added `tests/sql_set_subquery.rs` coverage for aliased `NOT EXISTS` membership filtering.

## Evidence

- `cargo fmt --check`: passed.
- `cargo test --test sql_set_subquery -- --nocapture`: passed, `33/33`.
- `cargo test --release --test pg_integration -- --nocapture`: passed, `27/27`.
- `cargo build --release --bin fusiondb`: passed.
- Non-isolation full test-data LDBC probe:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod108_exists_membership_full_testdata_10ops_20260529\ldbc_snb_native_smoke_summary.json`

## Result

The SQL and PgWire regressions pass, and the new membership-cache path is covered by a direct regression.

The full test-data non-isolation LDBC probe still does not pass. The run reported `status=gap`; `ldbc_command` timed out after 180 seconds, with workload status stuck at 4 operations. This means the Q14 performance frontier remains active.

This is not a full official/native LDBC benchmark pass.

## Current Frontier

- Q14 remains the active non-isolation full test-data LDBC blocker.
- The simple correlated `EXISTS` membership cache is not enough to make Q14 complete under the current smoke timeout.
- Next work should instrument Q14 directly and measure:
  - recursive `search_graph` iteration counts and row counts;
  - time spent in recursive edge expansion;
  - time spent in downstream `paths`, `edges`, `unique_edges`, `weights`, and `weightedpaths` stages.
- Native memtier remains blocked by missing real `memtier_benchmark` tooling.

## Next Task Candidate

BENCHPROD-109 should add a targeted Q14 profiler or harness using the PostgreSQL implementation test-data. The goal is to separate recursive row-growth cost from downstream array/aggregation cost, then pick the next engine optimization based on measured evidence.
