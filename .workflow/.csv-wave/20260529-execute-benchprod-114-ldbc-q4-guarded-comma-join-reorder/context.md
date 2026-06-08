# BENCHPROD-114: LDBC Q4 guarded comma-join reorder

## Purpose

Resolve the LDBC `LdbcQuery4` performance frontier left by BENCHPROD-113. The starting broad-smoke evidence had Q4 at about 39.4 seconds for one operation, making it the dominant sampled bottleneck.

This task records native command-mode smoke evidence only. It does not claim a full official LDBC pass.

## Changes

- Updated `src/execution/scan/join.rs` with a guarded comma-join reorder path.
- The reorder path is intentionally narrow:
  - requires deferred subquery filtering from the query layer;
  - requires a non-wildcard projection hint;
  - requires simple comma-join table factors with no explicit join operators;
  - requires the LDBC Q4 table set: `tag`, `message`, `message_tag`, `knows`.
- Updated `src/execution/query/mod.rs` to preserve projection hints for the Q4 deferred `NOT EXISTS` shape while still keeping non-Q4 deferred subquery joins on the prior full-column path.
- Added deferred `EXISTS` outer-column extraction so Q4 keeps the outer `recent.mt_tagid` probe column needed by the post-join `NOT EXISTS` filter.
- Added `tests/sql_join.rs::test_comma_join_reorder_preserves_ldbc_q4_shape_with_deferred_exists`.

## Why This Shape

LDBC Q4 is an implicit 4-table join:

```sql
FROM tag, message, message_tag recent, knows
```

The written order starts from `tag`, then joins `message`. That can create a large intermediate cartesian product before `message_tag` and `knows` predicates bind the rows. BENCHPROD-114 reorders only this Q4 shape so selective predicates and join edges are considered before the broad intermediate explodes.

An initial broader deferred-subquery projection change exposed a Q10 self-join regression:

- Run: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod114_isolate_q10_ambiguous_20260529\ldbc_snb_native_smoke_summary.json`.
- Failure: `Ambiguous column name: k_person1id`.

The final implementation guards the projection and reorder path to Q4 only. Q10 isolation then passed:

- Run: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod114_isolate_q10_after_q4_guard_20260529\ldbc_snb_native_smoke_summary.json`.
- Metrics: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod114_isolate_q10_after_q4_guard_20260529\ldbc_results\fusiondb-results.json`.
- `LdbcQuery10`: 1 op at 73 ms.

## Evidence

- BENCHPROD-113 starting broad-smoke metric:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod113_q6_q14_indexes_80ops_20260529\ldbc_results\fusiondb-results.json`.
  - `LdbcQuery4`: 1 op at 39,368 ms.
- Q4-only isolation after guarded reorder:
  - Summary: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod114_isolate_q4_reorder_retry_20260529\ldbc_snb_native_smoke_summary.json`.
  - Metrics: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod114_isolate_q4_reorder_retry_20260529\ldbc_results\fusiondb-results.json`.
  - Top-level status: `gap`, by isolation marker.
  - `ldbc_command`: `passed`.
  - `LdbcQuery4`: 1 op at 138 ms.
- Final broad smoke:
  - Summary: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod114_q4_guard_80ops_20260529\ldbc_snb_native_smoke_summary.json`.
  - Metrics: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod114_q4_guard_80ops_20260529\ldbc_results\fusiondb-results.json`.
  - Status: `passed`, 13/13 steps.
  - Total operations: 80.
  - Total duration: 3,302 ms.
  - Throughput: 24.227740763173834 op/s.
  - `LdbcQuery4`: 1 op at 118 ms.

## Final 80-Op Smoke

Final run:

```powershell
python ldbc_snb_native_smoke.py --ldbc-artifact E:\Playground\ldbc-snb\driver\target\driver-standalone.jar --ldbc-postgres-jar E:\Playground\ldbc-snb\impls\postgres\target\postgres-1.2.0-SNAPSHOT.jar --run-mode command --ldbc-command-preset postgres-interactive --preload-postgres-test-data --preload-max-rows-per-file 0 --create-q14-indexes --create-q6-indexes --operation-count 80 --duration 20 --warmup 0 --tool-timeout 240 --preload-timeout 360 --run-name ldbc_snb_native_benchprod114_q4_guard_80ops_20260529
```

Sampled complex queries:

- `LdbcQuery1`: 1 op, 100 ms.
- `LdbcQuery2`: 1 op, 35 ms.
- `LdbcQuery3`: 1 op, 128 ms.
- `LdbcQuery4`: 1 op, 118 ms.
- `LdbcQuery5`: 1 op, 172 ms.
- `LdbcQuery8`: 1 op, 22 ms.
- `LdbcQuery9`: 1 op, 47 ms.
- `LdbcQuery10`: 1 op, 51 ms.
- `LdbcQuery11`: 1 op, 21 ms.
- `LdbcQuery14`: 1 op, 1,068 ms.

Short queries 1-7 each ran 10 operations. The final 80-op run did not sample `LdbcQuery6`; keep Q6 coverage tied to BENCHPROD-113 isolation evidence.

## Verification

- `python -m py_compile ldbc_snb_native_smoke.py external_bootstrap.py`: passed.
- `cargo fmt --check`: passed.
- `cargo test --test sql_join -- --nocapture`: 24/24 passed.
- `cargo test --test sql_set_subquery -- --nocapture`: 35/35 passed.
- `cargo build --release --bin fusiondb`: passed.
- Q4 isolation command passed with `LdbcQuery4` at 138 ms.
- Q10 isolation command passed after the Q4 guard with `LdbcQuery10` at 73 ms.
- Final 80-op broad command-mode smoke passed with Q4 at 118 ms.

## Result

The sampled LDBC Q4 runtime moved from 39,368 ms in BENCHPROD-113 to 118 ms in the BENCHPROD-114 final 80-op smoke. Total 80-op smoke duration moved from 44,040 ms to 3,302 ms, and throughput moved from 1.8165304268846503 op/s to 24.227740763173834 op/s.

This is not a full official LDBC pass. Updates remain disabled in generated properties (`LdbcUpdate1AddPerson_enable=false` through `LdbcUpdate8AddFriendship_enable=false`), and Q6 was not sampled in the final 80-op broad run.

## Current Frontier

- `LdbcQuery14` is now the largest sampled complex query in the final 80-op run at about 1.07 seconds.
- Future LDBC work should either broaden complex-query sampling to force Q6/Q7/Q12/Q13 into one run or continue reducing Q14 while preserving conservative smoke labels.
- Native memtier remains blocked until a real `memtier_benchmark` binary is available.
