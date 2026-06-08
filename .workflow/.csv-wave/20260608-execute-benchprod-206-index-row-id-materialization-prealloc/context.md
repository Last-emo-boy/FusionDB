# BENCHPROD-206 Index Row Id Materialization Preallocation

## Goal

Avoid implicit vector growth while unordered index scans materialize row ids into the sortable vector used before scan windowing and row fetch.

## Implementation

- `src/execution/scan/mod.rs`
  - Replaced unordered index row id `collect()` with a vector preallocated from `index_plan.row_ids.len()`.
  - Preserved `sort_unstable()` for unordered row id sets.
  - Preserved ordered row id reuse, limit truncation, exact filtering, and row fetch behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache`
  - Passed: 37/37.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-206` is complete. Unordered index scan row id materialization now preallocates the sortable row id vector from the known row id count.
