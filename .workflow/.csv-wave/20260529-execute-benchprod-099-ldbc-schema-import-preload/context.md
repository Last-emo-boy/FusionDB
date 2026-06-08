# BENCHPROD-099: LDBC schema/import preload and Query 14 gap

## Purpose

Advance native LDBC SNB coverage from startup/schema blockers into a real PostgreSQL implementation workload path without claiming full LDBC pass.

## Changes

- Added bounded LDBC PostgreSQL implementation `test-data` preload support to `E:\Playground\FusionDB-bench\ldbc_snb_native_smoke.py`.
- Added `--preload-postgres-test-data`, `--preload-max-rows-per-file`, and `--preload-timeout`.
- Added Java `FusionLdbcPreload` helper that creates FusionDB-compatible SNB tables and imports official PostgreSQL implementation CSV files through PostgreSQL JDBC `CopyManager`.
- Fixed FusionDB extended query parameter propagation into table scans, views, derived tables, and `UNION ALL` branches by passing `params` through `scan_table_base` / `scan_derived_table`.
- Added PgWire regression coverage for extended parameters inside a derived `UNION ALL` subquery.
- Updated LDBC docs and bootstrap hints to reflect the current blocker: `LdbcQuery14`, not Query 1 placeholder handling.

## Evidence

- `cargo test --release test_pg_protocol_extended_params_inside_derived_union --test pg_integration`: passed.
- `cargo test --release --test pg_integration`: passed, `25/25`.
- `cargo build --release --bin fusiondb`: passed.
- `python -m py_compile ldbc_snb_native_smoke.py external_bootstrap.py external_smoke.py bench_gate.py`: passed.
- LDBC minimal native command evidence:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod099_preload_params_fix_20260529\ldbc_snb_native_smoke_summary.json`
  - Status `passed`; preload copied 5 rows per CSV and `LdbcQuery1` completed 1 operation.
- LDBC expanded native command evidence:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod099_query14_gap_final_20260529\ldbc_snb_native_smoke_summary.json`
  - Status `gap`; preload copied 20 rows per CSV and 10-operation run failed on `LdbcQuery14` with `Unsupported SELECT format`.
- External smoke:
  - `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod099_query14_gap_final_20260529\external_smoke_summary.json`
  - LDBC artifact/tool available; native LDBC still has structured workload gap.
- Strict native gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod099_query14_gap_final_strict_20260529\bench_gate_summary.json`
  - Status `failed`, `59/62`; remaining failures are memtier tool/native missing and LDBC native gap.

## Current Blockers

- Native memtier remains blocked by missing real `memtier_benchmark`.
- Native LDBC is no longer blocked by Hikari startup, missing SNB tables, or Query 1 `$n` parameter propagation.
- Native LDBC expanded command is blocked by `LdbcQuery14`, whose official SQL uses recursive CTEs, arrays, `generate_subscripts`, `row_number()`, nested `WITH`, and multi-layer aggregation.

## Next Task Candidate

BENCHPROD-100 should isolate `interactive-complex-14.sql` into smaller SQL feature slices, then decide whether to implement recursive CTE/array support or provide a scoped LDBC adapter rewrite that preserves benchmark semantics.
