# BENCHPROD-170 Join Aggregate In-Place Trim

## Goal

Avoid rebuilding join group aggregate result rows when applying `LIMIT` and `OFFSET`.

## Implementation

- `src/execution/query/mod.rs`
  - Renamed the query-level trim helper to `trim_query_rows_in_place` to avoid colliding with the column-scan helper.
  - Updated set-operation callers to use the renamed helper.
  - Replaced join group aggregate `skip`/`take`/`collect` result trimming with in-place trimming.
- `tests/sql_join.rs`
  - Added `test_join_group_by_aggregate_fast_path_order_limit_offset` to cover `ORDER BY ... LIMIT ... OFFSET` on the join aggregate fast path.

## Verification

- `cargo test --test sql_join test_join_group_by_aggregate_fast_path_order_limit_offset -- --nocapture`
  - Passed.
- `cargo test --test sql_join`
  - Passed: 31/31.
- `cargo test --test sql_set_subquery`
  - Passed: 48/48.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-170` is complete. Join group aggregate result windows now trim in place while set-operation window behavior remains covered.
