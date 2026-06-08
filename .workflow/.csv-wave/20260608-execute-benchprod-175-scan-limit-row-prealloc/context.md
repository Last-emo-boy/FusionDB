# BENCHPROD-175 Base Scan Limit Row Preallocation

## Goal

Avoid first-growth allocation for base table scan result rows when `LIMIT` information is already available.

## Implementation

- `src/execution/scan/mod.rs`
  - Replaced base scan `rows` initialization with `Vec::with_capacity(row_capacity)`.
  - `row_capacity` uses `effective_limit` when storage pushdown is safe, otherwise the query `limit`.
  - Preallocation is capped at 4096 rows.
  - Unlimited scans still start with zero initial capacity.
  - Existing scan ordering, index usage, filtering, and projection behavior remain unchanged.

## Verification

- `cargo test --test sql_select test_select_with_limit_offset -- --nocapture`
  - Passed.
- `cargo test --test sql_select test_select_order_by_limit_offset -- --nocapture`
  - Passed.
- `cargo test --test sql_index_cache test_select_order_by_primary_key_limit_offset -- --nocapture`
  - Passed.
- `cargo test --test sql_index_cache test_primary_key_range_order_limit_offset_pushdown -- --nocapture`
  - Passed.
- `cargo test --test sql_select`
  - Passed: 27/27.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-175` is complete. Limited base scans now start with a bounded result row capacity instead of growing from an empty vector.
