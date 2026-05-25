# TASK-073 Execution Report

## Summary

- Task: Optimize BM25 limited top-k search.
- Scope: database core storage layer only.
- Result: completed.

## Changes

- Added `InvertedIndex::search_bm25_limited`.
- Kept `InvertedIndex::search_bm25` as the full-result API.
- Used `select_nth_unstable_by` to retain top-k candidates before sorting only the returned slice.
- Routed `FusionStorage::bm25_search` through the limited API.

## Verification

- `cargo fmt --check`
- `cargo check --lib`
- `cargo test --lib storage::inverted_index -- --nocapture`
- `cargo test --lib storage::fusion -- --nocapture`
