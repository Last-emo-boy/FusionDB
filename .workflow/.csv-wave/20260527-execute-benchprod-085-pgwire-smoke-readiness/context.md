# BENCHPROD-085 PgWire Smoke Readiness

## Status

Completed on 2026-05-27. PgWire smoke passed.

## Why This Task

BENCHPROD-084 strengthened the HTTP production-like gate and removed unstable cases in the high-sample production repeat. The next production-readiness gap is ecosystem compatibility: official BenchBase/TPC-C, LDBC, CH-benCHmark, pgbench, and many application clients depend on PostgreSQL wire protocol behavior and metadata, not just HTTP `/query`.

## Precheck

External smoke was run at `E:/Playground/FusionDB-bench/runs/external_smoke_benchprod085_precheck_20260527/external_smoke_summary.md`.

- `pgbench`: `tool_missing`.
- `benchbase-tpcc`: Java found, but `BENCHBASE_HOME` / `BENCHBASE_JAR` missing.
- `ldbc`: Java found, but `LDBC_SNB_HOME` / `LDBC_DRIVER_HOME` missing.
- `chbenchmark`: Java found, but `CHBENCH_HOME` / `CHBENCHMARK_HOME` / `CH_BENCHMARK_HOME` missing.

Because local external artifacts were missing, this task implemented a self-contained protocol smoke instead of blocking on third-party distributions.

## Implementation

Added `E:/Playground/FusionDB-bench/pgwire_smoke.py`.

The smoke runner:

- starts release FusionDB with an isolated data directory unless `--reuse-server` is provided,
- performs PostgreSQL startup/auth over a raw socket,
- executes simple DDL/DML/query messages,
- sends extended `Parse`, `Describe`, `Bind`, `Execute`, and `Sync`,
- checks RowDescription type OIDs for `INT4`, `TEXT`, `DATE`, `TIMESTAMP`, `NUMERIC`, and `INTERVAL`,
- exercises `COPY FROM STDIN WITH (FORMAT CSV, HEADER true, NULL 'NULL')`,
- queries the copied row through the prepared extended statement.

Updated `E:/Playground/FusionDB-bench/README.md` with `pgwire_smoke.py` usage.

## Verification Evidence

- `python -m py_compile pgwire_smoke.py fusiondb_bench.py bench_repeat.py bench_stability.py bench_gate.py fusiondb_matrix.py external_smoke.py external_bootstrap.py`: passed.
- `python pgwire_smoke.py --run-name pgwire_smoke_benchprod085_final_20260527`: passed.
- Final smoke report: `E:/Playground/FusionDB-bench/runs/pgwire_smoke_benchprod085_final_20260527/pgwire_smoke_summary.md`, `status=passed`, `steps=8/8`.

Passing steps:

- `startup_auth`
- `simple_create_table`
- `simple_insert_typed_row`
- `simple_select_data_row`
- `extended_describe_metadata`
- `extended_bind_execute`
- `copy_from_stdin_csv`
- `query_copied_row`

## Caveats

This is not an official benchmark run and not a full JDBC compatibility test. It is a local protocol readiness smoke that turns several previously hand-waved blockers into executable evidence. The report also surfaced a difference worth tracking: simple query inference reports `id:INT8` for one select, while extended describe metadata reports table schema `id:INT4`.

## Next Task

BENCHPROD-086 should extend this from raw PgWire protocol smoke to driver/tool compatibility:

- add a JDBC smoke using the PostgreSQL JDBC driver if a jar is available or can be bootstrapped under `E:/Playground`,
- add catalog metadata probes used by BenchBase/JDBC (`pg_catalog` / `information_schema`),
- keep `external_smoke.py` as the artifact readiness gate for official benchmark distributions.
