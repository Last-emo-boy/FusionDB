# BENCHPROD-160 Empty-right Set Shortcut

## Goal

Avoid unnecessary right-side hash set construction and left-side filtering when `INTERSECT` or `EXCEPT` has an empty right input.

## Implementation

- `src/execution/query/mod.rs`
  - `INTERSECT` with empty right rows now returns `Vec::new()` directly.
  - `EXCEPT` with empty right rows now returns `left_rows` directly.
  - Existing DISTINCT, ORDER BY, and LIMIT/OFFSET handling still runs afterward.
- `tests/sql_set_subquery.rs`
  - Added coverage for empty-right `INTERSECT` and `EXCEPT`, including duplicate left-side rows for distinct semantics.

## Verification

- `cargo test --test sql_set_subquery test_intersect_except_with_empty_right_side -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 44/44.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-160` is complete. Empty-right `INTERSECT` and `EXCEPT` now avoid work that cannot affect the final result.
