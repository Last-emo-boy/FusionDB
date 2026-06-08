# BENCHPROD-176 Column Predicate Index Preallocation

## Goal

Avoid first-growth allocation for column-scan predicate column index storage.

## Implementation

- `src/execution/query/column_scan.rs`
  - Changed `simple_column_predicate_scan_plan` `column_indices` from `Vec::new()` to `Vec::with_capacity(predicates.len())`.
  - Existing predicate collection, operator support, column resolution, and value coercion remain unchanged.

## Verification

- `cargo test --test sql_group_aggregate test_group_by_aggregates_with_multi_predicate_partial_decode -- --nocapture`
  - Passed.
- `cargo test --test sql_group_aggregate test_group_by_aggregates_reuses_multi_predicate_column_values -- --nocapture`
  - Passed.
- `cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-176` is complete. Column-scan predicate plans now preallocate both term storage and column index storage from the predicate count.
