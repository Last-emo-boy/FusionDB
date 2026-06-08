# BENCHPROD-173 Join Output Preallocation

## Goal

Avoid starting the regular join output row buffer from zero capacity.

## Implementation

- `src/execution/scan/join.rs`
  - Changed `scan_join` regular output rows from `Vec::new()` to `Vec::with_capacity(...)`.
  - Capacity is derived from `limit` and input row counts.
  - Left outer joins use the left-side row count as the expected minimum output.
  - Preallocation is capped at 4096 rows to avoid excessive allocation on large joins.
  - Existing hash join, nested loop join, projection, and stop conditions remain unchanged.

## Verification

- `cargo test --test sql_join test_inner_join -- --nocapture`
  - Passed: 3 matched tests.
- `cargo test --test sql_join test_left_join -- --nocapture`
  - Passed: 2 matched tests.
- `cargo test --test sql_join`
  - Passed: 31/31.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-173` is complete. The regular join path now avoids first-growth allocation for its output row buffer.
