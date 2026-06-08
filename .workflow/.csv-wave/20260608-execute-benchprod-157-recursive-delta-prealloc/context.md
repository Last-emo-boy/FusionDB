# BENCHPROD-157 Recursive Delta Preallocation

## Goal

Reduce reallocations while a recursive CTE builds the next iteration's delta rows.

## Implementation

- `src/execution/query/mod.rs`
  - Changed `new_delta` from `Vec::new()` to `Vec::with_capacity(next_rows.len())`.
  - Uses the recursive term output length as the upper bound for rows that can enter the next delta.
  - Leaves duplicate filtering, row budget checks, and `UNION ALL` behavior unchanged.

## Verification

- `cargo test --test sql_set_subquery recursive_union -- --nocapture`
  - Passed: 4/4.
- `cargo test --test sql_set_subquery`
  - Passed: 41/41.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-157` is complete. Recursive CTE iteration now preallocates the next delta vector using the recursive term row count, avoiding growth reallocations when many candidate rows survive into the next iteration.
