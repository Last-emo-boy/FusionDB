# BENCHPROD-151 Set-Operation Result Window In-Place Trim

## Goal

Reduce allocation in the final `LIMIT/OFFSET` step for `UNION`, `UNION ALL`, `INTERSECT`, and `EXCEPT` queries.

## Implementation

- `src/execution/query/mod.rs`
  - Added `trim_set_rows_in_place`.
  - Replaced `combined.into_iter().skip(offset).take(limit).collect()` with in-place prefix drain and truncate.
  - Handles `offset >= rows.len()` by clearing the result directly.
- `tests/sql_set_subquery.rs`
  - Added a `UNION ALL ... ORDER BY ... LIMIT ... OFFSET` case where offset exceeds the number of result rows.

## Verification

- `cargo test --test sql_set_subquery test_union_all_order_by_limit_offset -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery test_union_all_order_by_limit_offset_beyond_rows -- --nocapture`
  - Passed.
- `cargo fmt --check`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 36/36.
- `cargo test --test sql_select`
  - Passed: 27/27.

## Result

`BENCHPROD-151` is complete. Set-operation result windowing now avoids allocating a replacement result vector for the final `LIMIT/OFFSET` trim.
