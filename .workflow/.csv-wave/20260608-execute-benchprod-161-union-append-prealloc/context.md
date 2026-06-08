# BENCHPROD-161 UNION Append Preallocation

## Goal

Avoid avoidable work while `UNION` and `UNION ALL` append right-side rows into the combined set-operation result.

## Implementation

- `src/execution/query/mod.rs`
  - Added empty-left and empty-right shortcuts for the non-window-pushed `UNION` path.
  - Calls `reserve(right_rows.len())` before extending left rows with right rows.
  - Existing DISTINCT, ORDER BY, and LIMIT/OFFSET handling still runs afterward.
- `tests/sql_set_subquery.rs`
  - Added `UNION ALL` coverage where the left side is empty and the right side supplies all rows.

## Verification

- `cargo test --test sql_set_subquery test_union_all_with_empty_left_side -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 45/45.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-161` is complete. `UNION` append now avoids empty-side append work and reduces growth reallocations when combining non-empty inputs.
