# BENCHPROD-162 UNION Window Empty Side

## Goal

Avoid allocating and draining a fresh window vector when `UNION ALL LIMIT/OFFSET` has one empty side.

## Implementation

- `src/execution/query/mod.rs`
  - `concat_set_rows_window` now detects empty left or right rows after the out-of-range check.
  - Applies `trim_set_rows_in_place` to the non-empty side and returns it directly.
  - Existing mixed-side window concatenation remains unchanged.
- `tests/sql_set_subquery.rs`
  - Added coverage for `UNION ALL ... LIMIT/OFFSET` with an empty left side.

## Verification

- `cargo test --test sql_set_subquery test_union_all_window_with_empty_left_side -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 46/46.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-162` is complete. Empty-side `UNION ALL` window concatenation now reuses the non-empty input vector and trims it in place.
