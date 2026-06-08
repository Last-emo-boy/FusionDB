# BENCHPROD-167 Column Aggregate Predicate Scratch Preallocation

## Goal

Avoid first-use growth allocation for predicate scratch values in column-scan aggregate and distinct fast paths.

## Implementation

- `src/execution/query/column_scan.rs`
  - Added `ColumnPredicateScanPlan::scratch_values` to size scratch vectors from the predicate column count.
  - Column-scan aggregate visitors now initialize `predicate_values` with predicate-sized capacity.
  - Distinct and count-distinct local predicate scratch buffers now use the same helper.
  - Existing predicate decoding, matching, grouping, and distinct behavior remain unchanged.

## Verification

- `cargo test --test sql_group_aggregate test_group_by_count_with_simple_where_column_scan -- --nocapture`
  - Passed with 0 matched tests; coverage retained through the full `sql_group_aggregate` suite.
- `cargo test --test sql_group_aggregate test_group_by_aggregates_with_simple_where_uses_column_scan -- --nocapture`
  - Passed.
- `cargo test --test sql_select test_select_distinct_with_simple_where_uses_column_scan -- --nocapture`
  - Passed.
- `cargo test --test sql_group_aggregate test_count_distinct_with_simple_where_uses_column_scan -- --nocapture`
  - Passed.
- `cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `cargo test --test sql_select`
  - Passed: 27/27.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-167` is complete. Column-scan aggregate and distinct predicate scratch buffers now start with the capacity required by the predicate plan.
