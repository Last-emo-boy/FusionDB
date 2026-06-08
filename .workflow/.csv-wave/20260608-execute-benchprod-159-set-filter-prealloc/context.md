# BENCHPROD-159 Set Filter Preallocation

## Goal

Reduce result vector reallocations while `INTERSECT` and `EXCEPT` filter left-side rows against the right-side row set.

## Implementation

- `src/execution/query/mod.rs`
  - Added `filter_rows_by_membership`.
  - Preallocates the filtered result vector with `rows.len()`, using the left input as the upper bound.
  - Replaced `filter(...).collect()` in `INTERSECT` and `EXCEPT`.
- `tests/sql_set_subquery.rs`
  - Added coverage where all left rows match the right side, so `INTERSECT` keeps every row and `EXCEPT` returns empty.

## Verification

- `cargo test --test sql_set_subquery test_intersect_except_with_all_left_rows_matched -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 43/43.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-159` is complete. `INTERSECT` and `EXCEPT` now avoid growth reallocations while constructing filtered left-side row results.
