# BENCHPROD-002 COPY Import Compatibility

Date: 2026-05-26
Goal: Unblock production benchmark initialization paths by adding a PostgreSQL-like file import surface.

## Completed

- Added `COPY <table> [(columns)] FROM '<file>' WITH (FORMAT CSV, HEADER ..., NULL ...)`.
- Supports local file CSV import with:
  - CSV quoting
  - configurable delimiter
  - configurable quote/escape
  - header skip
  - null marker
  - explicit column lists
- Reuses the existing `INSERT ... VALUES` execution path so constraints, indexes, foreign keys, row cache invalidation, and composite indexes stay consistent.
- Added FusionDB-bench `--load-mode copy` so setup data can flow through generated CSV and `COPY FROM`.

## Benchmark Relevance

- BenchBase/TPC-C, TSBS, LDBC, pgbench, and CH-benCHmark all need bulk initialization before meaningful measured workload runs.
- This is still a compatibility path, not the final high-throughput bulk writer.

## Deferred

- `COPY FROM STDIN` over PgWire.
- Direct bulk writer that avoids constructing `INSERT` SQL strings.
- Parallel load and configurable commit batching.
- Official tool adapters for BenchBase, TSBS, LDBC, pgbench.

## Verification

- `cargo fmt --check`
- `cargo check --lib`
- `cargo test --test sql_dml`
- `cargo test --test sql_ddl`
- `cargo test --test sql_index_cache`
- `python -m py_compile fusiondb_bench.py`
- `python fusiondb_bench.py --list`
- `python fusiondb_bench.py --scale tiny --suite ycsb,tsbs,ldbc --load-mode copy --iters 1 --warmup 0 --mixed-ops 20 --threads 2`

## Next TASK Queue

- `BENCHPROD-006`: PgWire extended query and metadata parity for external benchmark clients.
- `BENCHPROD-007`: Production scalar types: `DATE`, `TIMESTAMP`, `DECIMAL/NUMERIC`, `INTERVAL`.
- `BENCHPROD-009`: Direct COPY bulk writer with batching and lower SQL construction overhead.
- `BENCHPROD-014`: ANN recall harness with ground truth metrics.
