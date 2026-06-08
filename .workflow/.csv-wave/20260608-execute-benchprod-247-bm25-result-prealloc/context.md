# BENCHPROD-247 BM25 Result Preallocation

## Goal

Avoid implicit vector growth while converting BM25 score map entries into the result buffer that is later limited and sorted.

## Implementation

- `src/storage/inverted_index.rs`
  - Replaced `scores.into_iter().collect()` with `Vec::with_capacity(scores.len())`.
  - Pushed moved score entries into the preallocated result vector.
  - Preserved score calculation, HashMap iteration semantics, `select_nth_unstable_by` top-k limiting, truncation, and final BM25 score sort.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::inverted_index::tests -- --nocapture`
  - Passed: 5/5 target module tests; remaining binaries had 0 matching tests.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::fusion::tests::hybrid_search_limited_results_are_sorted_by_rrf_score -- --nocapture`
  - Passed: 1/1 target unit test; remaining binaries had 0 matching tests.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::fusion::tests::hybrid_search_zero_limit_skips_work -- --nocapture`
  - Passed: 1/1 target unit test; remaining binaries had 0 matching tests.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

A first attempt to run both hybrid-search filters in one `cargo test` command was rejected by Cargo because it accepts only one test-name filter. No tests ran in that failed command; both filters were then run separately and passed.

## Result

`BENCHPROD-247` is complete. BM25 search now preallocates the result vector from the known score-map size before sorting and limiting.
