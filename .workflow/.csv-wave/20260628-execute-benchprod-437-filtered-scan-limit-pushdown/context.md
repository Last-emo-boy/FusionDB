# BENCHPROD-437 Execution Context

## Outcome

Completed `BENCHPROD-437`, a Phase 9 performance iteration selected by an adversarial
investigate-and-rank workflow over the LARGE-scale benchmark hot spots. It both speeds up filtered
`LIMIT` queries and fixes a pre-existing aggregate correctness bug surfaced while writing the tests.

## Implementation

### Part A — LIMIT pushdown into filtered scans (performance)

- Relaxed `push_down_limit` in `handle_query_inner` (`src/execution/query/mod.rs`) so a `LIMIT`
  (plus `OFFSET`) is pushed into a single-table scan even when a `WHERE` clause is present, as long
  as the query is unordered, non-`DISTINCT`, and the projection has no aggregate or window function.
- The full-table-scan loop in `src/execution/scan/mod.rs` already early-breaks once it has collected
  `offset+limit` matching rows; previously the limit was dropped for filtered queries (only PK-ordered
  queries pushed it), so every row was decoded and predicate-evaluated. Now the scan stops early.
- Added `projection_contains_aggregate_or_window` + `expr_contains_window_function` helpers to gate
  the pushdown precisely (aggregates / window functions / `DISTINCT` must observe the full row set).
- Result-preserving: `LIMIT` without `ORDER BY` returns "the first N matching rows in an unspecified
  order"; the early break collects the first `offset+limit` matches in the same scan order.

### Part B — Aggregate LIMIT truncation fix (correctness)

- Found a pre-existing bug: `trim_query_rows_in_place(&mut rows, offset, limit)` ran BEFORE the
  `is_count_star` and bare-aggregate (`SUM`/`MIN`/`MAX`/`AVG`/`COUNT(DISTINCT)`/...) computations, so
  `SELECT COUNT(*)/SUM(x) ... WHERE ... LIMIT n` aggregated over only `n` rows (e.g. `COUNT(*) ... LIMIT 1`
  returned `1`). This only manifested when the query carried a `LIMIT` (which makes it skip the
  no-limit column-aggregate fast paths) plus a `WHERE`.
- Fix: aggregates now consume all matching rows; `LIMIT`/`OFFSET` is applied to their single-row
  result instead. The row-trimming for plain row projections moved to just before window-function
  handling, leaving the plain-SELECT and window paths byte-identical to before.

## Verification

- `cargo fmt --check` passed.
- `git diff --check` passed.
- `cargo check --bins` passed.
- `cargo test --test sql_select` passed (31 tests), including 4 new tests:
  `test_filtered_limit_returns_exactly_n_matching_rows`, `test_filtered_limit_offset_returns_exactly_n_rows`,
  `test_filtered_limit_does_not_truncate_bare_aggregate` (the correctness regression guard:
  `COUNT(*)=41` / `SUM=1230` over 41 matching rows under `LIMIT 1`), and
  `test_filtered_limit_distinct_not_truncated_before_dedup`.
- `cargo test --lib` passed (338 tests).
- `cargo test --test pg_integration shard_owner` passed (7 tests, no regression).
- Benchmark (medium scale) before/after: see "Benchmark" below.

## Benchmark (medium, 50,555 rows, single node)

Targeted queries carry a selective `WHERE` plus `LIMIT 50`, so the scan now early-breaks:

| Query                       | Before  | After   | Delta          |
|-----------------------------|---------|---------|----------------|
| AND filter (WHERE + LIMIT)  | 18.7 ms | 7.22 ms | ~2.6x faster   |
| OR filter (WHERE + LIMIT)   | 17.9 ms | 6.20 ms | ~2.9x faster   |
| Base category avg           | 10.90ms | 8.06 ms | improved       |

Unchanged (as expected — no LIMIT or many rows returned, so no early break is possible):
Full scan 24.5->23.3 ms, BETWEEN 20.6->20.3 ms, LIKE prefix 18.8->17.3 ms, IN list 22.4->19.5 ms.
Index/point queries unchanged (already sub-ms). The improvement beat the ranker's "~halved" estimate
at this scale because decode + per-row `evaluate_expr` dominated the pre-change cost; at much larger
scale the storage-materialization floor (still eager) will cap the win until the streaming follow-up.

## Investigation Provenance

Candidate selected via the `fusiondb-next-iter-investigate` workflow (4 parallel root-cause agents +
adversarial ranking). The ranker recommended this LIMIT pushdown as highest impact-per-risk, with a
verified honest caveat: storage still eagerly materializes all KV pairs for the selection case
(`scan_routed_data_prefixes_for_table` is called with `scan_limit=None` when a `WHERE` is present),
so only the decode + per-row `evaluate_expr` work short-circuits — expect a partial (not total) cut on
the AND/OR filter queries. Full storage-level short-circuit needs the streaming
`scan_routed_data_prefixes_for_each` + `StopAwareScanVisitor` path, a deliberately separate follow-up.

## Remaining / Next Candidates (from the same workflow)

- Per-row predicate-eval constant factor: hoist per-list-item column-type resolution out of the IN-list
  loop (`expr/mod.rs`), drop the per-row identifier `clone()` in `resolve_column_index` — targets the
  worst LARGE hot spot (IN list 358ms).
- Stream `COUNT(DISTINCT)` / `SELECT DISTINCT` via `ScanVisitor` instead of materializing all KV pairs.
- Parallelize the un-indexed full scan decode+filter via the existing `parallel_filter_rows` (rayon).
- Correctness: `RIGHT`/`FULL OUTER JOIN` execute with inner-join semantics (split into two tickets).
- Cheap syntax completions: `TRIM`, `EXTRACT QUARTER/WEEK`, `POSITION`, `GREATEST`/`LEAST`.
