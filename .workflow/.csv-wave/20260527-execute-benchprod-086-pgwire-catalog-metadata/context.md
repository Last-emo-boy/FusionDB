# BENCHPROD-086 PgWire Catalog Metadata

## Status

Completed on 2026-05-27. PgWire catalog smoke passed.

## Why This Task

BENCHPROD-085 proved the raw PgWire startup/auth, simple query, extended query, typed RowDescription, and `COPY FROM STDIN` paths. The next blocker for BenchBase, pgbench, JDBC clients, LDBC, and CH-benCHmark adapters is metadata compatibility: these tools query PostgreSQL catalog and information schema tables before or during workload execution.

## Implementation

Updated `E:/Playground/FusionDB/src/server/pg_server.rs`.

The PgWire handler now recognizes a small set of PostgreSQL metadata queries before falling through to the normal SQL executor:

- `current_schema()`
- `current_database()`
- `version()`
- `information_schema.tables`
- `information_schema.columns`
- `pg_catalog.pg_type`
- `pg_catalog.pg_namespace`
- `pg_catalog.pg_class`

Metadata rows for user tables are loaded from FusionDB schema records under `schema:*`. Projection and simple equality filters are handled for the smoke-covered query shapes.

Updated `E:/Playground/FusionDB-bench/pgwire_smoke.py` to add catalog probes after table creation, typed row insertion, extended metadata, and `COPY FROM STDIN` validation.

## Verification Evidence

- `cargo build --release --bin fusiondb`: passed.
- `python pgwire_smoke.py --run-name pgwire_smoke_benchprod086_catalog_20260527`: passed.
- Final smoke report: `E:/Playground/FusionDB-bench/runs/pgwire_smoke_benchprod086_catalog_20260527/pgwire_smoke_summary.md`, `status=passed`, `steps=13/13`.
- `cargo fmt --check`: passed.
- `cargo check --lib`: passed.
- `cargo test --test pg_integration -- --nocapture`: passed, `9 passed`.
- `python -m py_compile pgwire_smoke.py fusiondb_bench.py bench_repeat.py bench_stability.py bench_gate.py fusiondb_matrix.py external_smoke.py external_bootstrap.py`: passed.

Passing catalog smoke steps:

- `metadata_scalar_functions`
- `information_schema_tables`
- `information_schema_columns`
- `pg_catalog_pg_type`
- `pg_catalog_pg_class`

## Caveats

This is a targeted compatibility slice, not a complete PostgreSQL catalog. It does not yet cover common JDBC probes such as `pg_catalog.pg_attribute`, `pg_catalog.pg_database`, `current_setting('server_version')`, `SHOW server_version`, relation owner fields, index metadata, or privilege metadata.

## Next Task

BENCHPROD-087 should continue toward JDBC/tool compatibility by adding the next common metadata probes:

- `SHOW server_version` and/or `SELECT current_setting('server_version')`
- `pg_catalog.pg_database`
- `pg_catalog.pg_attribute`
- broader `SELECT * FROM pg_catalog.pg_namespace` and `pg_catalog.pg_class` coverage
- optional PostgreSQL JDBC smoke if a driver jar is present under `E:/Playground`
