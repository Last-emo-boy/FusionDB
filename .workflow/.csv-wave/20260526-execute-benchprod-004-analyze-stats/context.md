# BENCHPROD-004 ANALYZE Statistics Skeleton

Date: 2026-05-26
Goal: Add optimizer statistics foundations for production benchmark workloads.

## Completed

- Added `ANALYZE TABLE <table> COMPUTE STATISTICS` execution support.
- Stores table statistics in `stats:table:<table>` records outside `TableSchema` to preserve schema bincode compatibility.
- Captures:
  - row count
  - per-column null count
  - per-column exact distinct count for scalar values
  - per-column min/max for scalar values
- `EXPLAIN SELECT` now shows stored table stats when present.
- Authorization treats `ANALYZE` as table `SELECT` access for non-superuser checks.

## Benchmark Relevance

- TPC-C / CH-benCHmark need cardinality estimates before join order and index selection can become cost-aware.
- LDBC multi-hop joins need table and column selectivity signals.
- TSBS tag/time filters need row and distinct counts before time-series access planning can improve.

## Deferred

- Sampling for large tables.
- Histograms and most-common-values.
- Automatic stats invalidation/refresh thresholds.
- Cost-based join ordering and selectivity estimation using these stats.

## Verified

- `cargo fmt --check`
- `cargo check --lib`
- `cargo test --test sql_ddl`
- `cargo test --test sql_index_cache`
- `cargo test --test sql_dml`
- `cargo test --test sql_join`
- `cargo test --test sql_view_show_constraints`

## Next TASK Queue

- `BENCHPROD-002`: `COPY`/bulk import compatibility.
- `BENCHPROD-006`: PgWire extended query and metadata parity.
- `BENCHPROD-007`: Production scalar types.
- `BENCHPROD-012`: Ordered composite index planning for TSBS-style tag/time predicates.
