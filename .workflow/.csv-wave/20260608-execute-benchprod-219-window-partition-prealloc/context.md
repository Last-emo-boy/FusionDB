# BENCHPROD-219 Window Partition Preallocation

## Goal

Avoid implicit vector growth while window function evaluation builds row-index partitions from known row and partition counts.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced the no-`PARTITION BY` row index `collect()` with `Vec::with_capacity(rows.len())`.
  - Replaced `HashMap` partition values `collect()` with `Vec::with_capacity(partitions.len())`.
  - Preserved window partition membership, row indexes, partition key evaluation, and per-partition ordering.

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

`BENCHPROD-219` is complete. Window function partition construction now preallocates row-index and partition-value vectors from known counts.
