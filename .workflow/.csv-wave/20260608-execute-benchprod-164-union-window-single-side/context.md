# BENCHPROD-164 UNION Window Single Side

## Goal

Avoid allocating and draining a new window vector when a `UNION ALL LIMIT/OFFSET` window falls entirely within one input side.

## Implementation

- `src/execution/query/mod.rs`
  - `concat_set_rows_window` now trims and returns `left_rows` directly when the requested window is fully inside the left input.
  - It trims and returns `right_rows` directly when the window starts at or after the right input begins.
  - Mixed windows still use the existing preallocated concatenation path.
- `tests/sql_set_subquery.rs`
  - Added coverage for both left-only and right-only `UNION ALL` windows.

## Verification

- `cargo test --test sql_set_subquery test_union_all_window_with_single_side_ranges -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 48/48.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-164` is complete. Single-side `UNION ALL` windows now reuse the original input vector and trim it in place.
