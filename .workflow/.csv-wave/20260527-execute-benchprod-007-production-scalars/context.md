# BENCHPROD-007 Production Scalar Types

## Purpose

Reduce official benchmark schema compatibility gaps by adding first-class execution/storage support for production scalar types used by TPC-C, pgbench, BenchBase, TSBS, LDBC, and CH-benCHmark style schemas.

## Scope

- Database body only; dashboard/ui untouched.
- Added value-level support for:
  - `DATE`
  - `TIMESTAMP`
  - `DECIMAL` / `NUMERIC`
  - `INTERVAL`
- Added schema coercion for INSERT, INSERT SELECT, UPDATE, COPY-generated INSERT, defaults, typed SQL literals, CAST, comparison, secondary index keys, primary-key lookup/range paths, and PgWire metadata/binary rows.

## Evidence

- `cargo fmt --check`
- `cargo test --test sql_expr_functions -- --nocapture`
- `cargo test --test sql_dml -- --nocapture`
- `cargo test --test sql_index_cache -- --nocapture`
- `cargo test --test pg_integration -- --nocapture`
- `cargo build --release --bin fusiondb`
- `python fusiondb_matrix.py --scale tiny --suite production --load-mode insert --run-name matrix_production_tiny_after_benchprod007_20260527`

## Benchmark Result

- Report JSON: `E:/Playground/FusionDB-bench/runs/matrix_production_tiny_after_benchprod007_20260527/matrix_summary.json`
- Report Markdown: `E:/Playground/FusionDB-bench/runs/matrix_production_tiny_after_benchprod007_20260527/matrix_summary.md`
- Production tiny matrix: `tpcc`, `memtier`, `tsbs`, `ldbc`, and `chbench` all passed with 20/20 cases and 0 errors.

## Remaining Gaps

- Full PostgreSQL-compatible `NUMERIC` arithmetic precision is still approximate internally for arithmetic operations; storage/compare/Cast/PgWire are first-class enough for benchmark schemas and filters.
- Full date/time arithmetic and interval field semantics remain future work.
- Official external tools still need installation/configuration and adapters from `BENCHPROD-020/021/022/023/024/026`.

