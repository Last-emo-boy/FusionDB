# BENCHPROD-153 Set Right Hash Preallocation

## Goal

Reduce rehashing while building right-side row lookup sets for `INTERSECT` and `EXCEPT`.

## Implementation

- `src/execution/query/mod.rs`
  - Added `row_hash_set`.
  - Builds the right-side `HashSet<Vec<Value>>` with `HashSet::with_capacity(rows.len())` before extending rows.
  - Reused it for both `INTERSECT` and `EXCEPT`.
- `tests/sql_set_subquery.rs`
  - Added coverage where the right side contains duplicate values and both `INTERSECT` and `EXCEPT` must retain normal set semantics.

## Verification

- `cargo test --test sql_set_subquery test_intersect_except_with_duplicate_right_rows -- --nocapture`
  - Passed.
- `cargo fmt --check`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 38/38.
- `cargo test --test sql_select`
  - Passed: 27/27.

## Result

`BENCHPROD-153` is complete. `INTERSECT` and `EXCEPT` now preallocate right-side row hash tables using the right input size while preserving set behavior with duplicate right-side rows.
