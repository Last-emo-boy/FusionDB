# BENCHPROD-165 Distinct Column Rows Preallocation

## Goal

Reduce growth reallocations while `SELECT DISTINCT` single-column scans build output rows.

## Implementation

- `src/execution/query/column_scan.rs`
  - Introduced `distinct_capacity = kv_pairs.len().min(4096)`.
  - Reused that bounded capacity for both the distinct-value `HashSet` and the output `rows` vector.
  - Predicate filtering and value decoding behavior remain unchanged.

## Verification

- `cargo test --test sql_select test_select_distinct_with_simple_where_uses_column_scan -- --nocapture`
  - Passed.
- `cargo test --test sql_select`
  - Passed: 27/27.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-165` is complete. Distinct single-column scans now preallocate their output row vector using the same bounded capacity already used for distinct tracking.
