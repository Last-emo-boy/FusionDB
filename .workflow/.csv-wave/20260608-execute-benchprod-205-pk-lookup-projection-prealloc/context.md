# BENCHPROD-205 Primary-Key Lookup Projection Preallocation

## Goal

Avoid implicit vector growth while primary-key point lookup scans trim the projected decode index list to skip the guaranteed key column.

## Implementation

- `src/execution/scan/mod.rs`
  - Replaced projection index `filter().collect()` with an explicitly preallocated vector sized from `indices.len()`.
  - Preserved the direct clone path when no primary-key column is excluded.
  - Preserved projected decode behavior and primary-key value restoration.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_primary_key_lookup_projection_skips_where_key_decode -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_primary_key_only_equality_projection -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_primary_key_projection_reuses_full_row_cache -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache`
  - Passed: 37/37.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-205` is complete. Primary-key lookup projection trimming now preallocates lookup index vectors from known projection index counts.
