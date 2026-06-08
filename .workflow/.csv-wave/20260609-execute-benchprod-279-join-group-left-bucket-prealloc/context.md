# BENCHPROD-279 Join Group Left Bucket Preallocation

## Goal

Avoid the first growth step for newly-created join group aggregate left-side row buckets.

## Implementation

- `src/execution/query/mod.rs`
  - Added `join_group_left_bucket`, which creates a left-side row bucket with capacity 1.
  - Join group aggregate left row grouping now uses this helper when a new join key is inserted.
  - Added unit coverage for the helper capacity.
  - Join key grouping, aggregate computation, and result semantics are unchanged.

## Verification

- First attempt at `cargo test join_group_left_bucket_preallocates_first_row -- --nocapture`
  - Failed before test execution during MSVC linking with PDB limit errors: `LNK1318` and `LNK1140`.
  - After verifying paths under `E:\Playground\FusionDB\target\debug`, removed 23 Cargo-generated `.pdb` files, freeing 3,834,004,908 bytes.
  - Reran verification with `CARGO_PROFILE_TEST_DEBUG=0`.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test join_group_left_bucket_preallocates_first_row -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_join test_join_group_by_count_sum_fast_shape -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_join test_join_group_by_aggregate_fast_path_matches_chbench_shape -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_join -- --nocapture`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory. This run also required clearing Cargo-generated PDB files from `target/debug` after path verification because MSVC hit PDB limits and the drive was under disk pressure.

## Result

`BENCHPROD-279` is complete. Join group aggregate fast path now preallocates new left-side row buckets for the first row they immediately receive.
