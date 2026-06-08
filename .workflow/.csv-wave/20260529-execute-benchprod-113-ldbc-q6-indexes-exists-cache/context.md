# BENCHPROD-113: LDBC Q6 indexes and EXISTS membership cache

## Purpose

Move LDBC `LdbcQuery6` from the BENCHPROD-112 isolation timeout frontier to a completed native command-mode isolation run, then confirm the broader Q6/Q14-indexed smoke still completes.

This task records smoke and isolation evidence only. It does not claim a full official LDBC pass.

## Changes

- Updated `E:\Playground\FusionDB-bench\ldbc_snb_native_smoke.py` with an independent Q6 index creation path:
  - `LDBC_Q6_INDEX_CLASS = "FusionLdbcQ6Indexes"`;
  - generated Java source `LDBC_Q6_INDEX_SOURCE`;
  - CLI flag `--create-q6-indexes`;
  - `run_q6_index_creation(...)`;
  - summary config field `create_q6_indexes`.
- The Q6 index helper creates these indexes:
  - `idx_q6_knows_person1` on `knows(k_person1id)`;
  - `idx_q6_knows_person2` on `knows(k_person2id)`;
  - `idx_q6_message_creator` on `message(m_creatorid)`;
  - `idx_q6_message_replyof` on `message(m_c_replyof)`;
  - `idx_q6_message_id` on `message(m_messageid)`;
  - `idx_q6_message_tag_message` on `message_tag(mt_messageid)`;
  - `idx_q6_message_tag_tag` on `message_tag(mt_tagid)`;
  - `idx_q6_tag_name` on `tag(t_name)`;
  - `idx_q6_tag_id` on `tag(t_tagid)`.
- Updated `src/execution/expr/subquery.rs` with a two-table correlated `EXISTS` membership cache for Q6-like shapes:

```sql
EXISTS (
  SELECT *
  FROM tag, message_tag
  WHERE mt_messageid = outer.m_messageid
    AND mt_tagid = t_tagid
    AND t_name = $param
)
```

- The cache now recognizes a local two-table join equality, one local filter equality, and one outer probe equality.
- The final implementation uses `table_factor_schema_if_simple_table(...)` to read only `schema:<table>` metadata during plan detection, avoiding per-candidate-row scans of the local subquery tables.
- Added `tests/sql_set_subquery.rs::test_correlated_exists_two_table_membership_matches_ldbc_q6_shape`.

## Evidence

- BENCHPROD-112 starting point:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod112_isolate_q6_20260529\ldbc_snb_native_smoke_summary.json`.
  - `ldbc_command` timed out after 180 seconds with 2 of 3 Q6 operations completed.
- Q6 indexes only:
  - Summary: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod113_isolate_q6_indexes_20260529_retry\ldbc_snb_native_smoke_summary.json`.
  - Metrics: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod113_isolate_q6_indexes_20260529_retry\ldbc_results\fusiondb-results.json`.
  - `ldbc_command`: `passed`.
  - Top-level status: `gap`, by isolation marker.
  - `LdbcQuery6`: 3 ops, total 156,431 ms, throughput 0.019177784454487923 op/s, mean 52,143.666666666664 ms, min 51,382 ms, max 53,182 ms.
- Q6 indexes plus initial two-table `EXISTS` cache:
  - Summary: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod113_isolate_q6_existscache_20260529\ldbc_snb_native_smoke_summary.json`.
  - Metrics: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod113_isolate_q6_existscache_20260529\ldbc_results\fusiondb-results.json`.
  - `ldbc_command`: `passed`.
  - Top-level status: `gap`, by isolation marker.
  - `LdbcQuery6`: 3 ops, total 101,541 ms, throughput 0.029544715927556357 op/s, mean 33,847 ms, min 32,333 ms, max 34,904 ms.
- Q6 indexes plus schema-fast two-table `EXISTS` cache:
  - Summary: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod113_isolate_q6_existscache_schemafast_20260529\ldbc_snb_native_smoke_summary.json`.
  - Metrics: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod113_isolate_q6_existscache_schemafast_20260529\ldbc_results\fusiondb-results.json`.
  - `ldbc_command`: `passed`.
  - Top-level status: `gap`, by isolation marker.
  - `LdbcQuery6`: 3 ops, total 2,369 ms, throughput 1.2663571127057829 op/s, mean 789.3333333333334 ms, min 741 ms, max 831 ms.

## Final 80-Op Smoke

Final run:

```powershell
python ldbc_snb_native_smoke.py --ldbc-artifact E:\Playground\ldbc-snb\driver\target\driver-standalone.jar --ldbc-postgres-jar E:\Playground\ldbc-snb\impls\postgres\target\postgres-1.2.0-SNAPSHOT.jar --run-mode command --ldbc-command-preset postgres-interactive --preload-postgres-test-data --preload-max-rows-per-file 0 --create-q14-indexes --create-q6-indexes --operation-count 80 --duration 20 --warmup 0 --tool-timeout 240 --preload-timeout 360 --run-name ldbc_snb_native_benchprod113_q6_q14_indexes_80ops_20260529
```

Summary:

- Run summary: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod113_q6_q14_indexes_80ops_20260529\ldbc_snb_native_smoke_summary.json`.
- Metrics: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod113_q6_q14_indexes_80ops_20260529\ldbc_results\fusiondb-results.json`.
- Status: `passed`.
- Total operations: 80.
- Total duration: 44,040 ms.
- Throughput: 1.8165304268846503 op/s.
- `ldbc_query_dir`: generated run-local query directory with 37 copied SQL files and 9 compatibility files.
- Steps: 13/13 passed, including Q14 index creation and Q6 index creation.
- Complex queries sampled:
  - `LdbcQuery1`: 1 op, 101 ms.
  - `LdbcQuery2`: 1 op, 30 ms.
  - `LdbcQuery3`: 1 op, 139 ms.
  - `LdbcQuery4`: 1 op, 39,368 ms.
  - `LdbcQuery5`: 1 op, 213 ms.
  - `LdbcQuery8`: 1 op, 35 ms.
  - `LdbcQuery9`: 1 op, 91 ms.
  - `LdbcQuery10`: 1 op, 1,035 ms.
  - `LdbcQuery11`: 1 op, 40 ms.
  - `LdbcQuery14`: 1 op, 1,080 ms.
- Short queries 1-7 each ran 10 operations.

Important boundary: the final 80-op run enabled Q6 in the generated properties and created Q6 indexes, but its metrics did not sample `LdbcQuery6`. Q6 evidence remains the focused isolation command pass above.

## Verification

- `python -m py_compile ldbc_snb_native_smoke.py external_bootstrap.py`: passed.
- `cargo fmt --check`: passed.
- `cargo test --test sql_set_subquery -- --nocapture`: 35/35 passed.
- `cargo test --test sql_join -- --nocapture`: 23/23 passed.
- `cargo build --release --bin fusiondb`: passed.

## Result

`LdbcQuery6` moved from a BENCHPROD-112 180-second isolation timeout to a BENCHPROD-113 command-mode isolation pass. With Q6 indexes and the schema-fast two-table `EXISTS` membership cache, the focused Q6 run completed 3 operations with a mean runtime of about 789 ms.

The final broad 80-op native command-mode smoke also passed with both Q6 and Q14 indexes enabled. This remains command-mode smoke evidence, not a full official LDBC benchmark pass. Updates remain disabled in the generated LDBC properties (`LdbcUpdate1AddPerson_enable=false` through `LdbcUpdate8AddFriendship_enable=false`).

## Current Frontier

- BENCHPROD-114 should focus on LDBC Q4 performance.
- Starting evidence: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod113_q6_q14_indexes_80ops_20260529\ldbc_results\fusiondb-results.json`.
- Q4 is now the dominant broad-smoke bottleneck at about 39.4 seconds for one operation.
- Keep evidence labels conservative: isolation/profiler/smoke evidence is not a full official LDBC benchmark result.
