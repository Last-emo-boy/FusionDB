# BENCHPROD-277 Hash Join Bucket Preallocation

## Goal

Avoid the first growth step for newly-created hash join match buckets.

## Implementation

- `src/execution/scan/join.rs`
  - Added `join_match_bucket`, which creates a match bucket with capacity 1.
  - The build-right hash join path now uses this helper for new key buckets.
  - The build-left hash join path now uses this helper for new key buckets.
  - Added unit coverage for the helper capacity.
  - Join key extraction, residual predicates, left outer null row emission, and output ordering are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test join_match_bucket_preallocates_first_match -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_inner_join -- --nocapture`
  - Passed: 3/3.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_left_join -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join -- --nocapture`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-277` is complete. Hash join build maps now preallocate new match buckets for the first row reference they immediately receive.
