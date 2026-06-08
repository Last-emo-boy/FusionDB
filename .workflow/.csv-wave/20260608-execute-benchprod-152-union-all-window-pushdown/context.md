# BENCHPROD-152 UNION ALL Unordered Window Pushdown

## Goal

Avoid building a full concatenated `UNION ALL` result when there is no outer `ORDER BY` and the outer query has `LIMIT/OFFSET`.

## Implementation

- `src/execution/query/mod.rs`
  - Added `concat_set_rows_window`.
  - Parsed set-operation `LIMIT/OFFSET` before constructing the set result.
  - For `UNION ALL` without `ORDER BY`, copies only the requested window from left and right rows.
  - Leaves `ORDER BY`, `UNION` distinct, `INTERSECT`, and `EXCEPT` on their existing paths.
- `tests/sql_set_subquery.rs`
  - Added no-order `UNION ALL ... LIMIT ... OFFSET` coverage.

## Verification

- `cargo test --test sql_set_subquery test_union_all_limit_offset_without_order_by -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery test_union_all_order_by_limit_offset -- --nocapture`
  - Passed.
- `cargo fmt --check`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 37/37.
- `cargo test --test sql_select`
  - Passed: 27/27.

## Result

`BENCHPROD-152` is complete. Unordered `UNION ALL` now pushes the result window into concatenation and avoids materializing rows outside the requested `LIMIT/OFFSET` range.
