# BENCHPROD-112: LDBC run-local queryDir and 80-op native smoke

## Purpose

Remove the LDBC PostgreSQL implementation query-file loading gaps observed in BENCHPROD-111, then broaden native command-mode smoke coverage without mutating the external `E:\Playground\ldbc-snb` checkout.

This task records smoke evidence only. It does not claim a full official LDBC pass.

## Changes

- Updated `E:\Playground\FusionDB-bench\ldbc_snb_native_smoke.py` to generate a run-local `ldbc_queries` directory for each run.
- The harness copies PostgreSQL implementation query SQL files from `E:\Playground\ldbc-snb\impls\postgres\queries` and adds compatibility files expected by the driver:
  - `interactive-complex-3-duration-as-function.sql`;
  - `interactive-complex-4-duration-as-function.sql`;
  - `interactive-complex-7-with-second.sql`;
  - `interactive-update-1.sql`;
  - `interactive-update-4.sql`;
  - `interactive-update-6.sql`;
  - `interactive-update-7.sql`;
  - inline `interactive-update-6-add-post-content.sql`;
  - inline `interactive-update-6-add-post-imagefile.sql`.
- `queryDir` now points at the generated run-local directory, for example:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod112_q14_enabled_80ops_orderfix_20260529\ldbc_queries`.
- Extended interval arithmetic in `src/execution/expr/value.rs` for LDBC Q4 shapes such as `$timestamp + INTERVAL '1 days' * $durationDays`.
- Added regression coverage in `tests/sql_expr_functions.rs` for timestamp/date/interval arithmetic, including parameterized arithmetic.
- Updated ORDER BY handling in `src/execution/query/order.rs` and `src/execution/query/mod.rs` so a bare ORDER BY identifier can prefer an already projected output column over ambiguous join inputs.
- Added regression coverage in `tests/sql_join.rs` for the Q7-style `ORDER BY l_creationdate` ambiguity.

## Evidence

- Q14-enabled 10-op smoke with run-local compatibility queryDir passed:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod112_q14_enabled_10ops_compat_queries_20260529\ldbc_snb_native_smoke_summary.json`.
  - The prior `Unable to load query` stderr was gone.
- Broad 80-op smoke with compatibility queryDir initially exposed a Q4 arithmetic gap:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod112_q14_enabled_80ops_compat_queries_20260529\ldbc_snb_native_smoke_summary.json`.
  - Failure: `Type mismatch in arithmetic operation` for timestamp plus interval arithmetic.
- After the interval fix, an 80-op smoke passed:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod112_q14_enabled_80ops_intervalfix_20260529\ldbc_snb_native_smoke_summary.json`.
- Q7 isolation initially exposed ambiguous ORDER BY resolution:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod112_isolate_q7_20260529\ldbc_snb_native_smoke_summary.json`.
  - Failure: `Ambiguous column name: l_creationdate`.
- After the ORDER BY projection-preference fix, Q7 isolation command passed:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod112_isolate_q7_orderfix_20260529\ldbc_snb_native_smoke_summary.json`.
  - `ldbc_command`: `passed`, 1 operation at 70 ms.
  - Top-level isolation status remains `gap` by design because isolation mode disables other read queries.
- Q12 isolation command passed:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod112_isolate_q12_20260529\ldbc_snb_native_smoke_summary.json`.
  - `ldbc_command`: `passed`, 1 operation at 181 ms.
  - Top-level isolation status remains `gap` by design.
- Q13 isolation command passed:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod112_isolate_q13_20260529\ldbc_snb_native_smoke_summary.json`.
  - `ldbc_command`: `passed`, 1 operation at 93 ms.
  - Top-level isolation status remains `gap` by design.
- Q6 isolation remains a performance blocker:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod112_isolate_q6_20260529\ldbc_snb_native_smoke_summary.json`.
  - `ldbc_command` timed out after 180 seconds with 2 of 3 operations completed.

## Final 80-Op Smoke

Final run:

```powershell
python ldbc_snb_native_smoke.py --ldbc-artifact E:\Playground\ldbc-snb\driver\target\driver-standalone.jar --ldbc-postgres-jar E:\Playground\ldbc-snb\impls\postgres\target\postgres-1.2.0-SNAPSHOT.jar --run-mode command --ldbc-command-preset postgres-interactive --preload-postgres-test-data --preload-max-rows-per-file 0 --create-q14-indexes --operation-count 80 --duration 20 --warmup 0 --tool-timeout 240 --preload-timeout 360 --run-name ldbc_snb_native_benchprod112_q14_enabled_80ops_orderfix_20260529
```

Summary:

- Run summary: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod112_q14_enabled_80ops_orderfix_20260529\ldbc_snb_native_smoke_summary.json`.
- Metrics: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod112_q14_enabled_80ops_orderfix_20260529\ldbc_results\fusiondb-results.json`.
- Status: `passed`.
- Total operations: 80.
- Total duration: 47,971 ms.
- Throughput: 1.667674219841154 op/s.
- Query-load stderr: no `Unable to load query`; only SLF4J no-provider warnings.
- Complex queries exercised:
  - `LdbcQuery1`: 1 op, 123 ms.
  - `LdbcQuery2`: 1 op, 37 ms.
  - `LdbcQuery3`: 1 op, 189 ms.
  - `LdbcQuery4`: 1 op, 41,330 ms.
  - `LdbcQuery5`: 1 op, 247 ms.
  - `LdbcQuery8`: 1 op, 41 ms.
  - `LdbcQuery9`: 1 op, 109 ms.
  - `LdbcQuery10`: 1 op, 2,010 ms.
  - `LdbcQuery11`: 1 op, 40 ms.
  - `LdbcQuery14`: 1 op, 1,138 ms.
- Short queries 1-7 each ran 10 operations.

## Verification

- `python -m py_compile ldbc_snb_native_smoke.py external_bootstrap.py`: passed.
- `cargo fmt --check`: passed.
- `cargo test --test sql_expr_functions -- --nocapture`: 21/21 passed.
- `cargo test --test sql_join -- --nocapture`: 23/23 passed.
- `cargo build --release --bin fusiondb`: passed.

## Result

The LDBC PostgreSQL query-file loading gap is removed for native command-mode smoke runs by generating a run-local compatibility `queryDir`. The final Q14-enabled 80-op native command-mode smoke passed after the interval arithmetic and ORDER BY projection fixes.

This is not a full official LDBC pass. Updates remain disabled in the generated properties (`LdbcUpdate1AddPerson_enable=false` through `LdbcUpdate8AddFriendship_enable=false`), and the final broad run did not exercise every complex query in one stream. Q7, Q12, and Q13 have command-mode isolation pass evidence; Q6 remains a timeout/performance blocker.

## Current Frontier

- BENCHPROD-113 should focus on LDBC Q6 performance.
- Starting evidence: `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod112_isolate_q6_20260529\ldbc_snb_native_smoke_summary.json`.
- Likely Q6 directions include Q6-focused indexes such as `tag(t_name)`, `message_tag(mt_tagid)`, `message_tag(mt_messageid)`, `message(m_creatorid)`, and `knows` columns, or planner work around correlated `EXISTS` and join ordering.
- Keep future labels conservative: isolation/profiler/smoke evidence is not a full official LDBC benchmark result.
