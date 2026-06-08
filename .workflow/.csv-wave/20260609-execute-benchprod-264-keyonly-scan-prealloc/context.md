# BENCHPROD-264 Key-Only Scan Column Set Preallocation

## Goal

Avoid initial `HashSet` growth while `scan_single_table` checks whether a primary-key-only projection can use the key-only scan path.

## Implementation

- `src/execution/scan/mod.rs`
  - The `WHERE` column extraction set in the key-only scan eligibility check now uses `schema.columns.len()` as its capacity.
  - The `ORDER BY` column extraction set in the key-only scan eligibility check now uses `schema.columns.len()` as its capacity.
  - Key-only eligibility behavior is unchanged.
- `tests/sql_index_cache.rs`
  - Added `test_primary_key_only_projection_with_pk_order`.
  - The test covers a primary-key-only projection with primary-key filtering and ordering.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_select test_wide_select_projection_skips_unused_tail_decode -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_primary_key_only_projection_with_pk_order -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_primary_key_only_equality_projection -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_primary_key_in_projection_stream_skips_payload_decode -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_primary_key_equality_projection_skips_unused_column_decode -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-264` is complete. Key-only scan eligibility checks now preallocate temporary extracted-column sets from schema width.
