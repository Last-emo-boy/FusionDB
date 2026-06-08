# BENCHPROD-150 Group-By Order-Limit In-Place Trim

## Goal

Reduce allocation in the simple `GROUP BY ... ORDER BY ... LIMIT/OFFSET` fast path after top-N preselection has already narrowed the candidate rows.

## Implementation

- `src/execution/query/column_scan.rs`
  - Added `trim_rows_in_place`.
  - Replaced `rows.drain(..).skip(offset).take(take).collect::<Vec<_>>()` with in-place prefix drain and truncate.
  - Handles `offset >= rows.len()` by clearing the result directly.
- `tests/sql_group_aggregate.rs`
  - Added coverage for an offset beyond the number of groups.

## Verification

- `cargo test --test sql_group_aggregate test_group_by_aggregate_order_by_limit_offset_topn_window -- --nocapture`
  - Passed.
- `cargo test --test sql_group_aggregate test_group_by_order_by_limit_offset_beyond_groups -- --nocapture`
  - Passed.
- `cargo fmt --check`
  - Passed.
- `cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `cargo test --test sql_select`
  - Passed: 27/27.
- `cargo test --test sql_join`
  - Passed: 30/30.

## Result

`BENCHPROD-150` is complete. The group-by order-limit fast path now trims result windows in place and avoids allocating a replacement result vector for the final `LIMIT/OFFSET` step.
