# BENCHPROD-009 Direct COPY Bulk Writer

## Goal

Replace the COPY SQL-string INSERT batching path with a direct row writer while preserving constraints and indexes.

## Implementation

- `src/execution/copy.rs`
  - Removed COPY batch SQL string construction and parser round-trip.
  - COPY batches now call `Executor::insert_direct_rows`.
- `src/execution/dml/insert.rs`
  - Added direct row insert helpers for COPY payload rows.
  - The direct path applies explicit column mapping, DEFAULT values, SERIAL defaults, type coercion, NOT NULL, CHECK, UNIQUE, foreign keys, primary-key conflict checks, secondary indexes, trigram indexes, HNSW indexes, and composite indexes.
- `tests/sql_dml.rs`
  - Added direct COPY constraint regression coverage for UNIQUE, CHECK, and FOREIGN KEY failures.

## Verification

- `cargo test --test sql_dml copy_from`
  - Passed: 6/6.
- `cargo test --test pg_integration copy_from_stdin`
  - Passed: 2/2.
- `cargo test --test sql_dml`
  - Passed: 43/43.
- `cargo test --test sql_view_show_constraints foreign_key`
  - Passed: 4/4.
- `cargo build --release --bin fusiondb`
  - Passed.

## Benchmark Evidence

Baseline built from clean worktree at `b9aadd9`:

- `E:\Playground\FusionDB-bench\runs\matrix_benchprod009_legacy_copy_ycsb_medium_20260608\matrix_summary.json`
- Scale: `medium`
- Suite: `ycsb`
- Load mode: `copy`
- Result: 1/1 suites passed, 0 case errors.
- YCSB setup load: `583.375 ms`.

Current direct COPY writer:

- `E:\Playground\FusionDB-bench\runs\matrix_benchprod009_direct_copy_ycsb_medium_20260608\matrix_summary.json`
- Scale: `medium`
- Suite: `ycsb`
- Load mode: `copy`
- Result: 1/1 suites passed, 0 case errors.
- YCSB setup load: `497.966 ms`.
- Delta: `-85.409 ms`, `-14.64%`.

Production target copy matrix:

- `E:\Playground\FusionDB-bench\runs\matrix_benchprod009_direct_copy_production_medium_20260608\matrix_summary.json`
- Scale: `medium`
- Suites: `tpcc`, `memtier`, `tsbs`, `ldbc`, `chbench`
- Load mode: `copy`
- Result: 5/5 suites passed, 20/20 cases passed, 0 case errors.

## Remaining Scope

This closes the SQL-string INSERT batching portion of `BENCHPROD-009`. PgWire COPY still buffers the full payload in memory before execution; streaming network ingestion remains separate from the direct row writer.
