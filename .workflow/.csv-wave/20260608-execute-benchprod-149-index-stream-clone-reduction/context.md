# BENCHPROD-149 Large Index Stream Fetch Clone Reduction

## Goal

Reduce avoidable per-row clone and allocation overhead in the large index scan row-fetch stream path.

## Implementation

- `src/execution/scan/index_plan.rs`
  - Added `primary_key_row_from_parts`, which builds a key-only primary-key row from schema width, primary-key index, and precomputed primary-key type metadata.
  - Kept `primary_key_row_from_id` as the schema-based wrapper for existing callers.
- `src/execution/scan/mod.rs`
  - Changed the large index fetch stream to share table name via `Arc<str>`.
  - Changed projected decode indices from per-row `Vec` clones to shared `Arc<[usize]>`.
  - Removed per-row schema column clones and temporary `TableSchema` construction in the key-only stream branch.
- `tests/sql_index_cache.rs`
  - Added an 80-row primary-key `IN` projection regression. The row count crosses `SMALL_INDEX_FETCH_THRESHOLD`, so it covers the stream branch.
  - Corrupts payload bytes for every row; the key-only projection succeeds only if the payload is not decoded.

## Verification

- `cargo test --test sql_index_cache test_primary_key_in_projection_stream_skips_payload_decode -- --nocapture`
  - Passed.
- `cargo fmt --check`
  - Passed.
- `cargo test --test sql_index_cache`
  - Passed: 37/37.
- `cargo test --test sql_select`
  - Passed: 27/27.
- `cargo test --test sql_join`
  - Passed: 30/30.

## Result

`BENCHPROD-149` is complete. Large index fetch streams now avoid per-row schema/projection vector clones while preserving key-only partial decode behavior.
