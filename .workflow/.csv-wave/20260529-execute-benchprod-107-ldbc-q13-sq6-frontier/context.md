# BENCHPROD-107: LDBC Q13 and SQ6 frontier

## Purpose

Continue BENCHPROD-106 from bounded prefix40 Q1/Q14 evidence into broader LDBC full test-data coverage. The target was to advance real native-command workload paths without describing isolation, bounded, or preflight evidence as a full official/native benchmark pass.

## Changes

- Covered the LDBC Query 13 recursive CTE shape that preserves `path` while using `array_append(path, p2)` and `p2 <> ALL(path)`.
- Fixed Short Query 6 prepared execution where a bare aggregate projection evaluated `coalesce(min(parent), $2)` without resolving the `$2` placeholder.
- Updated `evaluate_final_group_expr` so `Expr::Value(SqlValue::Placeholder(_))` returns the matching `_params` value before falling back to literal conversion.
- Added SQL regressions in `tests/sql_set_subquery.rs` for:
  - Q13 recursive path expansion with `array_append` and `ALL(path)`;
  - literal SQ6 `coalesce(min(parent), fallback)` for a post-backed message;
  - prepared SQ6 `coalesce(min(parent), $2)` with parameter values;
  - post-to-message derivation used by the LDBC preload adapter;
  - `COPY ... NULL ''` empty fields feeding the post-to-message SQ6 path.

## Evidence

- `cargo fmt --check`: passed.
- `cargo test --test sql_set_subquery -- --nocapture`: passed, `32/32`.
- `cargo test --release --test pg_integration -- --nocapture`: passed, `27/27`.
- `cargo build --release --bin fusiondb`: passed.
- JDBC prepared SQ6 probe against an old run WAL copy:
  - before the placeholder fix: `ROWS=0`;
  - after the placeholder fix: `ROWS=1`.
- Disable-Q14 full test-data LDBC isolation run:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod107_sq6fix_disable_q14_full_testdata_10ops_20260529\ldbc_snb_native_smoke_summary.json`

## Result

The disable-Q14 full test-data isolation run completed `ldbc_command` successfully with exit code `0`. The summary status remains `gap` because the run intentionally used isolation mode with `--disable-read-query 14`.

The run reported `Operation Count: 19` and metrics for:

- `LdbcQuery1`
- `LdbcQuery13`
- `LdbcShortQuery1PersonProfile`
- `LdbcShortQuery2PersonPosts`
- `LdbcShortQuery3PersonFriends`
- `LdbcShortQuery4MessageContent`
- `LdbcShortQuery5MessageCreator`
- `LdbcShortQuery6MessageForum`
- `LdbcShortQuery7MessageReplies`

This is meaningful evidence that the full test-data command path can complete when Q14 is isolated out, and that Query 13 plus Short Queries 1-7 are now exercised. It is not a full official/native LDBC benchmark pass.

## Current Frontier

- Non-isolation full test-data LDBC remains blocked by Query 14 timeout/performance.
- Q14 is the active native LDBC frontier; disabling it is a diagnostic isolation mode only.
- The configured PostgreSQL implementation still logs missing update-query files, so this run is read-focused and does not prove update coverage.
- Native memtier remains blocked by missing real `memtier_benchmark` tooling.

## Next Task Candidate

BENCHPROD-108 should target Q14 runtime/performance in the non-isolation full test-data path, preferably with a focused Q14 profiling harness and clear separation between targeted diagnostics and full benchmark evidence.
