# BENCHPROD-163 Empty-left Set Shortcut

## Goal

Avoid right-side hash set construction when `INTERSECT` or `EXCEPT` has an empty left input.

## Implementation

- `src/execution/query/mod.rs`
  - `INTERSECT` now returns empty if either side is empty.
  - `EXCEPT` now returns empty immediately when the left side is empty.
  - Existing empty-right `EXCEPT` behavior remains unchanged.
- `tests/sql_set_subquery.rs`
  - Added coverage for empty-left `INTERSECT` and `EXCEPT` with a non-empty right side.

## Verification

- `cargo test --test sql_set_subquery test_intersect_except_with_empty_left_side -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 47/47.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-163` is complete. Empty-left `INTERSECT` and `EXCEPT` now avoid right-side hashing and return the known empty result directly.
