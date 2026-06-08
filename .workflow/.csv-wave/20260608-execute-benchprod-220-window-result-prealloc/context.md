# BENCHPROD-220 Window Projection Result Preallocation

## Goal

Avoid implicit vector growth while SELECT projection precomputes window function results for each projection column.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced `window_results` `map().collect()` with `Vec::with_capacity(select.projection.len())`.
  - Preserved one result slot per projection column.
  - Preserved window function detection and non-window projection behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_window`
  - Passed: 4/4.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_select`
  - Passed: 27/27.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-220` is complete. SELECT window projection precomputation now preallocates result vectors from known projection counts.
