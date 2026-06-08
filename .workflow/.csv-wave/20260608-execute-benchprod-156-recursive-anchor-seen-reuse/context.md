# BENCHPROD-156 Recursive Anchor Seen Reuse

## Goal

Avoid rebuilding the recursive `UNION` seen-row set after anchor deduplication.

## Implementation

- `src/execution/query/mod.rs`
  - Added `deduplicate_rows_with_seen`, which returns both deduplicated rows and the populated `HashSet`.
  - Kept `deduplicate_rows` as the existing wrapper for shared callers.
  - Recursive `UNION` now reuses the anchor deduplication set for seen-row tracking instead of cloning `all_rows` into `row_hash_set`.
- `tests/sql_set_subquery.rs`
  - Added coverage where the recursive anchor side emits duplicate rows before recursion.

## Verification

- `cargo test --test sql_set_subquery test_with_recursive_union_deduplicates_anchor_seen_rows -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 41/41.
- `cargo test --test sql_select`
  - Passed: 27/27.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-156` is complete. Recursive `UNION` now carries forward the HashSet already built while deduplicating anchor rows, removing an extra clone-and-hash pass over the anchor result.
