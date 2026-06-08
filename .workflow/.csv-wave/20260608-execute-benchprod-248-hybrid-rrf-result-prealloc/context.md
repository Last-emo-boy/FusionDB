# BENCHPROD-248 Hybrid RRF Result Preallocation

## Goal

Avoid implicit vector growth while converting hybrid-search RRF score map entries into the result buffer that is later limited and sorted.

## Implementation

- `src/storage/fusion.rs`
  - Replaced `rrf_scores.into_iter().collect()` with `Vec::with_capacity(rrf_scores.len())`.
  - Pushed moved RRF score entries into the preallocated result vector.
  - Preserved RRF score calculation, HashMap iteration semantics, `select_nth_unstable_by` top-k limiting, truncation, and final score sort.

## Verification

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

## Result

`BENCHPROD-248` is complete. Hybrid search now preallocates the RRF result vector from the known score-map size before sorting and limiting.
