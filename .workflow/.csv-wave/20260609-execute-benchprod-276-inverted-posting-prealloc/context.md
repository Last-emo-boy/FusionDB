# BENCHPROD-276 Inverted Posting List Preallocation

## Goal

Avoid the first growth step for newly-created inverted-index term posting lists.

## Implementation

- `src/storage/inverted_index.rs`
  - `InvertedIndex::add_document` now uses `or_insert_with(|| Vec::with_capacity(1))` for new term posting lists.
  - Existing posting lists keep their current allocation and append behavior.
  - Added `add_document_preallocates_new_posting_lists` to verify the first posting list receives initial capacity.
  - Tokenization, BM25 scoring, persistence, and FTS SQL behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test add_document_preallocates_new_posting_lists -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::inverted_index::tests -- --nocapture`
  - Passed: 6/6.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache fts -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-276` is complete. New inverted-index posting lists now preallocate capacity for their immediately inserted first posting.
