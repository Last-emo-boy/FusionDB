# BENCHPROD-178 Primary BETWEEN Row Id Preallocation

## Goal

Avoid first-growth allocation for row id sets in primary-key `BETWEEN` index planning.

## Implementation

- `src/execution/scan/index_plan.rs`
  - Changed the primary-key `BETWEEN` branch from `HashSet::new()` to `HashSet::with_capacity(kv.len())` after `scan_range`.
  - Existing range bounds, row id extraction, exactness, and ordering behavior remain unchanged.

## Verification

- `cargo test --test sql_select test_between -- --nocapture`
  - Passed.
- `cargo test --test sql_index_cache test_select_with_commuted_primary_key_range -- --nocapture`
  - Passed.
- `cargo test --test sql_index_cache test_primary_key_range_reuses_row_cache -- --nocapture`
  - Passed.
- `cargo test --test sql_index_cache`
  - Passed: 37/37.
- `cargo test --test sql_select`
  - Passed: 27/27.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-178` is complete. Primary-key `BETWEEN` scans now size the row id set from the returned range entries.
