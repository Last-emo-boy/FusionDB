# BENCHPROD-278 Window Partition Bucket Preallocation

## Goal

Avoid the first growth step for newly-created window partition row-index buckets.

## Implementation

- `src/execution/query/mod.rs`
  - Added `window_partition_bucket`, which creates a partition row-index bucket with capacity 1.
  - Window partition grouping now uses this helper when a new partition key is inserted.
  - Added unit coverage for the helper capacity.
  - Partition expression evaluation, ordering, and window function output behavior are unchanged.

## Verification

- First attempt at `cargo test window_partition_bucket_preallocates_first_row_index -- --nocapture`
  - Failed before test execution with `os error 112` due to E: drive space exhaustion while writing Cargo artifacts.
  - After verifying paths under `E:\Playground\FusionDB\target\debug`, removed Cargo-generated `.pdb` files and reran verification.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test window_partition_bucket_preallocates_first_row_index -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_window test_window_row_number -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_window -- --nocapture`
  - Passed: 4/4.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery window -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full. This run also required clearing Cargo-generated PDB files from `target/debug` after path verification because E: had insufficient free space.

## Result

`BENCHPROD-278` is complete. Window partition grouping now preallocates new row-index buckets for the first index they immediately receive.
