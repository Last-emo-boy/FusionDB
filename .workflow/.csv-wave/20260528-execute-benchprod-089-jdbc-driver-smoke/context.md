# BENCHPROD-089: PostgreSQL JDBC driver smoke

## Purpose

Advance FusionDB from raw PgWire smoke toward real PostgreSQL client ecosystem compatibility by running the official PostgreSQL JDBC driver against FusionDB PgWire.

## Completed Work

- Added/validated JDBC smoke coverage in `E:/Playground/FusionDB-bench/jdbc_smoke.py`.
- Fixed PgWire metadata gaps needed by PostgreSQL JDBC:
  - `SELECT current_catalog`
  - `current_catalog()` scalar metadata
  - `SHOW server_version` RowDescription during extended Describe
  - JDBC `DatabaseMetaData.getTables()` and `getColumns()` catalog fast-paths
- Fixed extended protocol result format consistency:
  - metadata and normal query RowDescription/DataRow now use the Bind result format codes consistently
  - PostgreSQL JDBC `application_name` is tracked so JDBC default text result format is honored
  - existing tokio-postgres tests retain binary compatibility
- Fixed extended protocol transaction control:
  - JDBC `commit()` and `rollback()` now execute against the active PgWire session transaction instead of being treated as ordinary success statements.
- Updated `external_smoke.py` to include JDBC smoke evidence beside PgWire smoke evidence.
- Updated `README.md` with JDBC smoke and `--jdbc-evidence` usage.

## Evidence

- JDBC smoke passed:
  - `E:/Playground/FusionDB-bench/runs/jdbc_smoke_benchprod089_txn_extended_final_20260528/jdbc_smoke_summary.md`
  - status `passed`, steps `6/6`
- External smoke evidence report:
  - `E:/Playground/FusionDB-bench/runs/external_smoke_benchprod089_jdbc_evidence_20260528/external_smoke_summary.md`
- Rust validation:
  - `cargo fmt --check`
  - `cargo check --lib`
  - `cargo test --test pg_integration -- --nocapture` passed, `9 passed`
  - `cargo build --release --bin fusiondb` passed
- Bench script validation:
  - `python -m py_compile jdbc_smoke.py pgwire_smoke.py fusiondb_matrix.py external_smoke.py external_bootstrap.py bench_gate.py bench_repeat.py bench_stability.py fusiondb_bench.py`

## Remaining Gap

Passing JDBC smoke is not full BenchBase/TPC-C readiness. The next production gap is running a real external harness or pgbench path and recording concrete SQL/protocol/dialect blockers.
