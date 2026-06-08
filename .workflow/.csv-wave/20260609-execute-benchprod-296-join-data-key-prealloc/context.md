# BENCHPROD-296 Join Data Key Preallocation

## Goal

Avoid generic `format!` allocation work when join probe hot paths build `data:<table>:<row_id>` keys from known component lengths.

## Implementation

- `src/execution/scan/join.rs`
  - Added `join_data_key_for_row_id()` with explicit capacity for `data:<table>:<row_id>`.
  - Replaced `format!` key construction in primary-key join probe projection fetch.
  - Replaced `format!` key construction in indexed join probe projection fetch.
  - Added unit coverage for helper output and capacity.
  - Generated key bytes, row-cache keys, storage lookups, row decoding, projection behavior, join ordering, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test join_data_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_join -- --nocapture`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-296` is complete. Join probe data-key construction now reserves capacity from known table and row-id lengths.
