# BENCHPROD-158 Small Dedup Fast Path

## Goal

Avoid unnecessary hash-set work when shared query row deduplication receives 0 or 1 rows.

## Implementation

- `src/execution/query/mod.rs`
  - `deduplicate_rows` now returns 0/1 row inputs directly.
  - `deduplicate_rows_with_seen` has a matching small-input branch while still returning the seen set required by recursive `UNION`.
- `tests/sql_set_subquery.rs`
  - Added single-row `UNION` coverage to protect the small-result distinct path.

## Verification

- `cargo test --test sql_set_subquery test_union_distinct_single_row -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 42/42.
- `cargo test --test sql_select`
  - Passed: 27/27.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-158` is complete. Shared row deduplication now skips hash-set construction for ordinary 0/1 row callers while preserving recursive `UNION` seen-row tracking.
