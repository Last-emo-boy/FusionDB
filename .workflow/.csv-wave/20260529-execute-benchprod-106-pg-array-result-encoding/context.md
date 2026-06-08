# BENCHPROD-106: PgWire array result encoding

## Purpose

Continue BENCHPROD-105 from the Q14 JDBC `PgArray` decode frontier. The goal was to make the nested `path` array returned by LDBC Query 14 decodable by PostgreSQL JDBC without claiming a full native LDBC benchmark pass.

## Changes

- Added PostgreSQL brace-literal text encoding for `Value::Array`, including nested arrays such as `{{10,11},{20,21}}`.
- Added PgWire array type inference for result values and expressions:
  - array literals and casts,
  - array concat (`||`),
  - `ARRAY_AGG`,
  - array subscript projections,
  - CTE-derived projections used by Q14.
- Forced array result fields to `FieldFormat::Text` so field format metadata matches the text array payload.
- Added minimal built-in `pg_catalog.pg_type` array metadata used by PgJDBC `PgArray.getArray()` / `TypeInfoCache.getPGArrayElement`.
- Added PgWire regression coverage in `tests/pg_integration.rs` for nested `bigint[][]` metadata, brace-literal text output, and PgJDBC array element metadata query shape.

## Evidence

- `cargo fmt --check`: passed.
- `cargo test --test sql_set_subquery -- --nocapture`: passed, `25/25`.
- `cargo test --test sql_expr_functions test_string_concat_operator -- --nocapture`: passed.
- `cargo test --release --test pg_integration -- --nocapture`: passed, `26/26`.
- `cargo build --release --bin fusiondb`: passed.
- Initial BENCHPROD-106 smoke advanced from `PgArray ArrayIndexOutOfBoundsException` to a `pg_catalog.pg_type` metadata gap:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod106_pg_array_prefix40_10ops_20260529\ldbc_snb_native_smoke_summary.json`
- Final bounded native LDBC command smoke passed:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod106_pg_array_pgtype_prefix40_10ops_20260529\ldbc_snb_native_smoke_summary.json`

## Result

The bounded 40-row native LDBC command smoke completed with `status=passed`, `steps=8/8`, and `ldbc_command` exit code `0`.

The run completed 2 operations and reported metrics for:

- `LdbcQuery1`
- `LdbcQuery14`

This is meaningful bounded native-command evidence for the Q1/Q14 prefix40 path. It is not a full native LDBC benchmark pass.

## Remaining Blockers

- Full native LDBC workload coverage remains unproven beyond this bounded prefix40 command path.
- Larger preloads and additional read/update query paths still need separate evidence.
- Native memtier remains blocked by missing real `memtier_benchmark` tooling.

## Next Task Candidate

BENCHPROD-107 should broaden LDBC coverage from the prefix40 Q1/Q14 command pass to more query paths or a larger preload, while keeping bounded/isolation/preflight labels explicit.
