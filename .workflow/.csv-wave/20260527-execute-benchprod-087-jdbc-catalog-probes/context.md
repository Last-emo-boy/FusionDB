# BENCHPROD-087 JDBC-Style PgWire Catalog Probes

## Status

Completed on 2026-05-27. PgWire JDBC-style catalog smoke passed.

## Why This Task

BENCHPROD-086 added the first virtual PostgreSQL catalog slice. To keep moving toward external benchmark harness compatibility, this task expands from basic catalog probes to query shapes that JDBC/BenchBase/pgbench-style clients commonly use during connection and metadata discovery.

## Implementation

Updated `E:/Playground/FusionDB/src/server/pg_server.rs`.

New metadata support:

- `SHOW server_version`
- `SHOW server_encoding`
- `SELECT current_setting('server_version')`
- `SELECT current_setting('server_encoding')`
- `SELECT current_setting('client_encoding')`
- `SELECT current_setting('search_path')`
- `SELECT current_setting('application_name')`
- `pg_catalog.pg_database`
- `pg_catalog.pg_attribute`
- wider `pg_catalog.pg_namespace` fields: `nspowner`, `nspacl`
- wider `pg_catalog.pg_class` fields: `relowner`, `relhasindex`, `relpersistence`

Updated `E:/Playground/FusionDB-bench/pgwire_smoke.py`.

New smoke steps:

- `pg_catalog_pg_namespace_wildcard`
- `pg_catalog_pg_database`
- `current_setting_server_version`
- `show_server_version`
- `pg_catalog_pg_attribute`

## Verification Evidence

- `cargo build --release --bin fusiondb`: passed.
- `python pgwire_smoke.py --run-name pgwire_smoke_benchprod087_jdbc_catalog_final_20260527`: passed.
- Final smoke report: `E:/Playground/FusionDB-bench/runs/pgwire_smoke_benchprod087_jdbc_catalog_final_20260527/pgwire_smoke_summary.md`, `status=passed`, `steps=18/18`.
- `cargo fmt --check`: passed.
- `cargo check --lib`: passed.
- `cargo test --test pg_integration -- --nocapture`: passed, `9 passed`.
- `python -m py_compile pgwire_smoke.py fusiondb_bench.py bench_repeat.py bench_stability.py bench_gate.py fusiondb_matrix.py external_smoke.py external_bootstrap.py`: passed.

## Caveats

The catalog is still a targeted virtual compatibility layer, not a full PostgreSQL system catalog. It does not yet cover index metadata (`pg_index`, `pg_constraint`), privilege metadata, prepared statement introspection, protocol cancel request, real JDBC driver execution, or official benchmark adapters.

## Next Task

BENCHPROD-088 should move from raw-socket catalog probes to a real driver/tool check:

- Prefer adding a PostgreSQL JDBC smoke if a driver jar exists under `E:/Playground` or can be supplied locally.
- Otherwise add a self-contained `psql`/`pgbench` readiness path when `pgbench` appears on PATH.
- Add external smoke hints that connect missing artifact status to the new PgWire catalog evidence.
