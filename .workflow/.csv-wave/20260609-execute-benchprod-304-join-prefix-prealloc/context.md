# BENCHPROD-304 Join Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when join planning and indexed join probes build data and index scan prefixes from known component lengths.

## Implementation

- `src/execution/scan/join.rs`
  - Added `join_data_prefix_for_table()` with explicit capacity for `data:<table>:`.
  - Added `join_index_prefix_for_value()` with explicit capacity for `index:<table>:<column>:<value>:`.
  - Replaced `format!` prefix construction in indexed join probes.
  - Replaced `format!` prefix construction in comma-join row-count fallback.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, index scans, row-count fallback, join probe rows, projection behavior, join ordering, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test join_ -- --nocapture`
  - Passed: matching tests.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_join -- --nocapture`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-304` is complete. Join prefix construction now reserves capacity from known component lengths.
