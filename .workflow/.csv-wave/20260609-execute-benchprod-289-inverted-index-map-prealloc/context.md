# BENCHPROD-289 Inverted-Index Map Preallocation

## Goal

Avoid the first growth step for a newly constructed inverted index when the first document is inserted.

## Implementation

- `src/storage/inverted_index.rs`
  - Added `postings_map()` with capacity for the first term entry.
  - Added `doc_lengths_map()` with capacity for the first document-length entry.
  - Changed `InvertedIndex::new()` to initialize both top-level maps through those constructors.
  - Added unit coverage for first-document map preallocation.
  - Tokenization, posting-list updates, average document length, BM25 scoring, duplicate document semantics, and result ordering are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test new_preallocates_first_document_maps -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::inverted_index::tests -- --nocapture`
  - Passed: 7/7.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests::hybrid_search -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-289` is complete. New inverted indexes now reserve top-level map capacity for the first indexed document.
