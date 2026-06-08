# BENCHPROD-111: Q14-enabled native LDBC smoke

## Purpose

Retry the native LDBC command-mode smoke after BENCHPROD-110 removed the targeted Q14 `weightedpaths` profiler timeout. The goal was to verify whether Q14 can be enabled in a native command-mode LDBC run on the preloaded PostgreSQL implementation test-data.

This task records smoke evidence only. It does not claim a full official LDBC pass.

## Changes

- Extended `E:\Playground\FusionDB-bench\ldbc_snb_native_smoke.py` with `--create-q14-indexes`.
- Added the Java helper `FusionLdbcQ14Indexes` to the native smoke harness.
- The helper creates the same Q14 support indexes used by BENCHPROD-110:
  - `CREATE INDEX idx_q14_message_creator ON message (m_creatorid)`;
  - `CREATE INDEX idx_q14_message_replyof ON message (m_c_replyof)`;
  - `CREATE INDEX idx_q14_message_id ON message (m_messageid)`.
- Extended `src/execution/expr/mod.rs` dependency extraction so `Expr::Array` recursively contributes element dependencies.
- Added regression coverage for nested `array_agg(ARRAY[...])` values through SQL and PG protocol paths:
  - `tests/sql_set_subquery.rs`;
  - `tests/pg_integration.rs`.
- Updated `E:\Playground\FusionDB-bench\external_bootstrap.py` LDBC hint text so it no longer reports old recursive blockers as the current broad-native conclusion.

## Evidence

- Initial Q14-enabled native smoke with indexes reached the LDBC command path but failed in `LdbcQuery1`:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod111_q14_enabled_full_testdata_10ops_indexes_20260529\ldbc_snb_native_smoke_summary.json`
  - Status: `gap`.
  - Error: `ClassCastException: class [Ljava.lang.String; cannot be cast to class [[Ljava.lang.Object;`.
  - Root cause: projection dependency extraction missed `Expr::Array` elements inside `array_agg(ARRAY[o_name, year, place])`.
- After the array dependency fix and release rebuild, the Q14-enabled native smoke passed:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod111_q14_enabled_full_testdata_10ops_indexes_arrayfix_20260529\ldbc_snb_native_smoke_summary.json`
  - Status: `passed`.
  - Requested operation count: 10.
  - Reported operation count: 19.
  - Reported duration: 1.566 seconds.
  - Reported throughput: 12.13 op/s.
  - `LdbcQuery1`: 1 operation, 116 ms.
  - `LdbcQuery14`: 1 operation, 1083 ms.
  - Short queries 1-7 were also exercised.

Command used for the successful smoke:

```powershell
python ldbc_snb_native_smoke.py --ldbc-artifact E:\Playground\ldbc-snb\driver\target\driver-standalone.jar --ldbc-postgres-jar E:\Playground\ldbc-snb\impls\postgres\target\postgres-1.2.0-SNAPSHOT.jar --run-mode command --ldbc-command-preset postgres-interactive --preload-postgres-test-data --preload-max-rows-per-file 0 --create-q14-indexes --operation-count 10 --duration 10 --warmup 0 --tool-timeout 240 --preload-timeout 360 --run-name ldbc_snb_native_benchprod111_q14_enabled_full_testdata_10ops_indexes_arrayfix_20260529
```

## Verification

- `python -m py_compile external_bootstrap.py ldbc_snb_native_smoke.py ldbc_q14_profile.py`: passed.
- `cargo fmt --check`: passed.
- `cargo test --test sql_set_subquery -- --nocapture`: 34/34 passed.
- `cargo test --test sql_join -- --nocapture`: 22/22 passed.
- `cargo build --release --bin fusiondb`: passed.
- `cargo test --release --test pg_integration -- --nocapture`: final rerun passed 28/28.

The first `pg_integration` attempt returned a non-zero code with only the compilation start line visible in tool output. A clean rerun completed compilation and passed all 28 tests, so the recorded verification result is the reproducible successful run.

## Result

The Q14-enabled native command-mode LDBC smoke now passes on the preloaded PostgreSQL implementation test-data with Q14 indexes enabled. The successful run includes one `LdbcQuery14` operation at 1083 ms and one `LdbcQuery1` operation at 116 ms.

This is not a full official LDBC pass. The stderr from the successful command still reports missing PostgreSQL implementation SQL files, including `interactive-complex-3-duration-as-function.sql`, `interactive-complex-4-duration-as-function.sql`, `interactive-complex-7-with-second.sql`, and multiple update SQL files. The successful operation mix is narrow and does not prove all complex queries or updates.

## Current Frontier

- BENCHPROD-112 should supply or repair the missing PostgreSQL query/update SQL files, then broaden the command-mode operation mix.
- Continue labeling results by evidence scope: profiler, smoke, bounded/native, or full official.
- Native memtier remains blocked by missing real `memtier_benchmark` tooling.
