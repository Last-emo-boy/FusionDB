# BENCHPROD-008 Benchmark Harness Expansion

Date: 2026-05-26
Goal: Build the external benchmark lab in `E:/Playground/FusionDB-bench` around real production benchmark targets.

## Completed

- Expanded `FusionDB-bench` from 4 local suites to 9 suites:
  - `ycsb`
  - `tpcc`
  - `tpch`
  - `search`
  - `memtier`
  - `tsbs`
  - `ldbc`
  - `ann`
  - `chbench`
- Added a capability/readiness model for:
  - TPC-C / BenchBase
  - memtier
  - TSBS
  - LDBC SNB
  - ANN benchmarks
  - CH-benCHmark
  - sysbench OLTP
  - pgbench
- Added JSON and Markdown report sections that include target readiness, native blockers, and the BENCHPROD task queue.
- Updated `config.example.json` and README so the benchmark lab defaults to all available local suites.

## Current Reality

- This is a benchmark lab, not official benchmark certification.
- FusionDB currently has local like-workloads for most named targets, but native official runs are blocked by protocol/dialect/import/optimizer gaps.
- `sysbench` and `pgbench` remain planned native adapter work, not runnable local suites yet.

## Verified

- `python -m py_compile fusiondb_bench.py`
- `python fusiondb_bench.py --list`

## Next TASK Queue

- `BENCHPROD-004`: Finish `ANALYZE` stats and feed `EXPLAIN`/optimizer decisions.
- `BENCHPROD-002`: Add `COPY`/bulk import compatibility for BenchBase, TSBS, LDBC, pgbench initialization.
- `BENCHPROD-006`: PgWire extended-query and metadata parity for JDBC/pgbench clients.
- `BENCHPROD-007`: Add production scalar types: `DATE`, `TIMESTAMP`, `DECIMAL/NUMERIC`, `INTERVAL`.
- `BENCHPROD-014`: Add ANN recall harness with ground-truth computation and recall@k metrics.
