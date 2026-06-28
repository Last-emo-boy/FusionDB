# BENCHPROD-440 Execution Context

## Outcome
Phase 9 perf iteration (workflow-implemented in an isolated worktree, then integrated). Targets
unindexed full-scan queries (LARGE: Full scan 242ms, BETWEEN 267ms, LIKE 307ms — single-threaded).

## Implementation (src/execution/scan/mod.rs)
- The full-table-scan branch decoded + predicate-evaluated rows in a serial loop. For the
  WITH-selection, no-pushed-limit case above a row-count threshold, it now decodes+filters in parallel
  via rayon (reusing the existing `parallel_filter_rows` pattern already used on the index post-filter
  branch). `rayon par_iter().filter().collect()` preserves input order, so results are order-identical.
- Guards: skips key_only_scan / zero_column_projection; when a limit IS pushed (BENCHPROD-437), keeps
  the serial early-break path so the early termination is not lost.

## Verification
- `cargo fmt --check`, `git diff --check`, `cargo check --bins` passed.
- `cargo test --test sql_full_scan_parallel` (parallel-vs-serial equivalence incl. ordering, >1000 rows)
  passed; `cargo test --lib` passed.
