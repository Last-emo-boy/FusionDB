# BENCHPROD-105: Q14 generate_subscripts and array frontier

## Purpose

Continue LDBC native readiness from BENCHPROD-104. The 20-row smoke produced an explicit `search_graph row limit exceeded` gap, but a reachability probe showed the Q14 parameter pair is not connected in the first 20 `person_knows_person` rows. BENCHPROD-105 uses a 40-row reachable-prefix diagnostic smoke to expose the next real Q14 blockers.

## Changes

- Added minimal `generate_subscripts(array, dim)` support in `FROM`:
  - Handles standalone table-function scans.
  - Handles dependent comma-join expansion, e.g. `paths, generate_subscripts(path, 1) d1`.
  - Uses PostgreSQL-style 1-based indexes.
- Added array subscript evaluation for `path[d1][d2]`.
- Added `ARRAY_AGG` accumulator support.
- Added grouped projection support for `COALESCE(sum(...), 0)`.
- Fixed PgWire Describe for `SELECT * FROM (subquery) x` so extended-query clients receive field structure before rows.
- Added regression coverage in `tests/sql_set_subquery.rs`:
  - `test_generate_subscripts_from_array_literal`
  - `test_generate_subscripts_depends_on_left_row_array`
  - `test_array_agg_over_generated_subscripts`
  - `test_group_by_can_project_array_path_expression`
  - `test_group_by_projection_can_coalesce_aggregate`

## Evidence

- `cargo fmt --check`: passed.
- `cargo test --test sql_set_subquery -- --nocapture`: passed, `25/25`.
- `cargo test --test sql_expr_functions test_string_concat_operator -- --nocapture`: passed.
- `cargo test --release --test pg_integration -- --nocapture`: passed, `25/25`.
- `cargo build --release --bin fusiondb`: passed.
- LDBC reachable-prefix diagnostics:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod105_q14_reachable_prefix40_10ops_20260529\ldbc_snb_native_smoke_summary.json`
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod105_generate_subscripts_prefix40_10ops_20260529\ldbc_snb_native_smoke_summary.json`
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod105_group_projection_prefix40_10ops_20260529\ldbc_snb_native_smoke_summary.json`
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod105_pg_describe_prefix40_10ops_20260529\ldbc_snb_native_smoke_summary.json`

## Result

BENCHPROD-105 does not make LDBC pass. It advances Query 14 through multiple prior blockers:

- `Table generate_subscripts not found`
- `Unsupported expression in GROUP BY projection`
- JDBC `Received resultset tuples, but no field structure for them`

The current frontier is now JDBC array conversion for Q14 result rows:

`java.lang.ArrayIndexOutOfBoundsException` in `org.postgresql.jdbc.ArrayDecoding.buildArrayList`, reached from `PostgresDb$Query14.convertSingleResult`.

## Current Blockers

- FusionDB returns Q14 path arrays in a form that PostgreSQL JDBC cannot decode as the expected PostgreSQL array result.
- The 40-row smoke remains `status=gap`, `steps=7/8`, and `ldbc_command` exit code `1`.
- This is still non-isolation diagnostic evidence, not a full native LDBC benchmark pass.
- Native memtier remains blocked by missing real `memtier_benchmark` tooling.

## Next Task Candidate

BENCHPROD-106 should implement PostgreSQL-compatible nested array result typing/encoding for PgWire/JDBC, then rerun the 40-row reachable-prefix LDBC Q14 smoke.
