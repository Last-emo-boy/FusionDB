# BENCHPROD-147 Join Probe Projection Alignment and Cache Reuse

## Goal

Harden join index probe projection pruning so qualified right-side columns stay aligned with the right table schema and cached full rows can be reused safely by projected probe paths.

## Implementation

- `src/execution/scan/join.rs`
  - Added base-schema projection resolution for stage join projections, including qualified aliases such as `r.name` mapped back to base column `name`.
  - Reused `row_cache` hits in projected primary-key and BTree join probe fetch paths before reading and decoding row bytes from storage.
  - Preserved the existing `RowDecoder::decode_partial` contract, which returns a full-width sparse row with `Null` in skipped column slots, so downstream join projection keeps schema and row positions aligned.
- `tests/sql_join.rs`
  - Added coverage for `SELECT l.id, r.name` where the right-side selected column appears after the primary-key join column.
  - Added coverage proving a projected primary-key join probe can reuse a cached full right row instead of decoding corrupted storage bytes.

## Root Cause

The join probe path can prune the right-side row decode when the join key is guaranteed by an indexed primary-key probe. That optimization is only safe if the reduced right projection still maps qualified join-stage column names back to the base table schema and if the decoded row keeps full schema positions for later join projection.

`RowDecoder::decode_partial` already returns a full-width sparse row. Treating its output as a short projected row would shift values away from their schema positions and make a valid right-side projected column, such as `r.name`, appear as `Null` in the final result.

## Verification

- `cargo test --test sql_join test_primary_key_join_probe_aligns_projected_right_column_after_key -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_primary_key_join_probe_projection_reuses_right_row_cache -- --nocapture`
  - Passed.
- `cargo fmt --check`
  - Passed.
- `cargo test --test sql_join`
  - Passed: 30/30.

## Result

`BENCHPROD-147` is complete. Join probe projection pruning now keeps right-side projected columns aligned, continues to skip unnecessary storage decode for unused right columns, and reuses cached full rows on projected probe paths.
