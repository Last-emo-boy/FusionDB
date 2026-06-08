# BENCHPROD-154 In-place Row Deduplication

## Goal

Reduce allocation overhead in shared row deduplication used by `SELECT DISTINCT`, recursive CTE deduplication, and distinct set operations.

## Implementation

- `src/execution/query/mod.rs`
  - Changed `deduplicate_rows` to take ownership of the input rows as mutable state.
  - Uses `Vec::retain` with the existing `HashSet` to preserve first-seen unique rows.
  - Removes the separate `unique_rows` output vector allocation.
- `tests/sql_set_subquery.rs`
  - Added duplicate-input `UNION` coverage where both sides contain repeated values.

## Verification

- `cargo test --test sql_set_subquery test_union_distinct_with_duplicate_inputs -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 39/39.
- `cargo test --test sql_select`
  - Passed: 27/27.
- `cargo fmt --check`
  - Passed.

## Result

`BENCHPROD-154` is complete. Shared query row deduplication now avoids allocating a second result vector while preserving distinct semantics for set operations and `SELECT DISTINCT`.
