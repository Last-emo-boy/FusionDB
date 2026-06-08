# BENCHPROD-195 Set-Query ORDER BY Row Preallocation

## Goal

Avoid implicit vector growth while set-query `ORDER BY` prepares indexed rows and restores sorted rows for both full sorts and limit/offset Top-N windows.

## Implementation

- `src/execution/query/mod.rs`
  - Added `sort_indexed_rows_with_window` as a private helper for set-query indexed row sorting.
  - Preallocates the indexed row vector from the input row count.
  - Preallocates the restored row vector from the sorted/truncated indexed row count.
  - Keeps original index tie ordering, `select_nth_unstable_by` window truncation, and full-sort behavior unchanged.

## Verification

- `cargo test --test sql_set_subquery test_union_all_order_by_limit_offset -- --nocapture`
  - Passed: 2/2 filtered tests.
- `cargo test --test sql_set_subquery`
  - Passed: 48/48.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-195` is complete. Set-query `ORDER BY` sorting now uses explicitly preallocated intermediate row vectors for known-size indexed sorting paths.
