# BENCHPROD-166 Column Predicate Scratch Preallocation

## Goal

Avoid first-use growth allocation for predicate scratch values in distinct column-scan fast paths.

## Implementation

- `src/execution/query/column_scan.rs`
  - `count_distinct_column_scan` now preallocates `predicate_values` using the predicate column count.
  - `distinct_column_scan` uses the same predicate column-count capacity.
  - Existing predicate decoding, matching, and distinct behavior remain unchanged.

## Verification

- `cargo test --test sql_select test_select_distinct_with_simple_where_uses_column_scan -- --nocapture`
  - Passed.
- `cargo test --test sql_group_aggregate test_count_distinct_with_simple_where_uses_column_scan -- --nocapture`
  - Passed.
- `cargo test --test sql_select`
  - Passed: 27/27.
- `cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-166` is complete. Distinct column-scan fast paths now size their predicate scratch vector before row iteration starts.
