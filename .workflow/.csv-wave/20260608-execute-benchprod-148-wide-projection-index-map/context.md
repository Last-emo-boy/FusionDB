# BENCHPROD-148 Wide Scan Projection Index Map

## Goal

Reduce projection planning overhead for wide table scans without regressing the narrow-table path that dominates small queries.

## Implementation

- `src/execution/scan/mod.rs`
  - Added `projection_indices_for_scan`.
  - Preserved the previous linear, case-insensitive scan for small projection/schema combinations.
  - Added a one-pass `HashMap` from lower-case column name to column index when `projection.len() * schema.columns.len()` is large enough to avoid repeated full schema walks.
- `tests/sql_select.rs`
  - Added a wide-table projection regression that selects four columns from a 13-column row.
  - The test corrupts the unused tail column; a full-row decode would fail, while the projected partial decode succeeds.

## Verification

- `cargo test --test sql_select test_wide_select_projection_skips_unused_tail_decode -- --nocapture`
  - Passed.
- `cargo fmt --check`
  - Passed.
- `cargo test --test sql_select`
  - Passed: 27/27.
- `cargo test --test sql_index_cache`
  - Passed: 36/36.
- `cargo test --test sql_join`
  - Passed: 30/30.

## Result

`BENCHPROD-148` is complete. Wide scan projection setup now avoids repeated schema scans while keeping the cheap linear path for small scans and retaining partial decode behavior for projected columns.
