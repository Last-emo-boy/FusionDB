# BENCHPROD-444 Execution Context

## Outcome
Phase 9 perf iteration designed by a read-only investigation workflow (3 agents + synthesis) on
current main, implemented directly on main. Addresses the BENCHPROD-437 caveat: filtered + LIMIT
full scans still materialized ALL ~228k KV pairs before decoding; now they stream and stop early.

## Why a read-only design workflow (not worktree implementation)
`isolation: 'worktree'` anchors worktrees at the session-start commit (032e052), so worktree agents
are blind to the 436-443 commits this change builds on. Used read-only agents (which see current
main) to produce a code-grounded plan; implemented on main.

## Implementation (src/execution/scan/mod.rs)
- New `FilteredLimitScanVisitor` (impl `ScanVisitor`) that decodes each streamed (key,value) using
  logic byte-identical to the serial full-scan loop (key-only / zero-column / projected /
  full-row+row_cache), evaluates the selection, collects matches, and returns `false` to stop once
  `rows.len() == limit`. Evaluate errors are captured into `visitor.error` (visit returns bool) and
  re-raised after the drive call.
- In `scan_single_table`'s `!index_used` branch, for `selection.is_some() && limit.is_some()`, drive
  the scan via `scan_routed_data_prefixes_for_each(table, txn, None, &mut visitor)` and return early.
  `limit=None` is passed to the driver on purpose: its limit counts VISITED pairs, not matched rows;
  the visitor self-stops at `limit` matches. The `StopAwareScanVisitor` wrapper propagates the stop
  across shard prefixes, so the storage layer never reads/materializes past the needed matches.
- Untouched (mutually exclusive on `limit`): the BENCHPROD-440 rayon parallel path
  (`selection.is_some() && limit.is_none()`), the no-selection storage-limit pushdown, the index
  paths, and the 16 full-scan callers that pass `None`.

## Verification
- `cargo fmt --check`, `git diff --check`, `cargo check --bins` passed.
- `cargo test --test sql_stream_scan` (5 new tests: PK-order, fewer-than-limit, equivalence vs
  non-streamed full set, projected, OFFSET) passed.
- `cargo test --test sql_select` (BENCHPROD-437 limit tests, now exercising this streaming path)
  and `cargo test --lib` passed.

## Benchmark (large, 228,488 rows, single node) — before 444 -> after 444
- AND filter (WHERE + LIMIT 50): 56ms -> 3.55ms  (~16x; storage now stops after ~1k rows)
- OR filter  (WHERE + LIMIT 50): 35ms -> 11.2ms  (~3x)
- Unchanged / no regression (no pushed LIMIT, so not streamed): Full scan 243->238ms,
  BETWEEN 236->218ms, LIKE 232->212ms, ORDER BY val DESC 317->315ms.
- Cumulative campaign effect on AND filter: 250ms (pre-437) -> 56ms (437/440) -> 3.55ms (444) ~ 70x.

## Deferred (honest)
- The ~243ms UNLIMITED full-scan floor is NOT addressed here: it is decode-CPU bound (440 already
  parallelizes it; materialization is the smaller share), so streaming-serial would regress it. The
  right lever is projection-pushdown decode (decode only predicate columns) — a separate ticket.
