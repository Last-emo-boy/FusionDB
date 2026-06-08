# BENCHPROD-168 Indexed Aggregate Row Id Streaming

## Goal

Avoid buffering candidate row ids for indexed column aggregate scans.

## Implementation

- `src/execution/query/column_scan.rs`
  - Removed the intermediate `row_ids` vector from `simple_column_aggregate_index_scan`.
  - Primary-key probes now fetch the single data row directly.
  - BTree index probes now parse each index entry row id, build the data key, fetch the row, and visit it immediately.
  - Existing aggregate state finalization, predicate decoding, and matching behavior remain unchanged.

## Verification

- `cargo test --test sql_group_aggregate test_filtered_count_uses_index_candidates_and_required_columns_only -- --nocapture`
  - Passed.
- `cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-168` is complete. Indexed column aggregate scans no longer allocate or populate an intermediate row id buffer before visiting candidate rows.
