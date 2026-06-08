# BENCHPROD-308 ANALYZE Data Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when ANALYZE builds `data:<table>:` scan prefixes from known table-name length.

## Implementation

- `src/execution/analyze.rs`
  - Added `analyze_data_prefix_for_table()` with explicit capacity for `data:<table>:`.
  - Replaced `format!` prefix construction in ANALYZE table-stat scans.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, transaction scan prefixes, row-cache lookups, row decoding, distinct stats, min/max stats, EXPLAIN statistics, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test analyze_ -- --nocapture`
  - Passed: helper and matching analyze tests.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl -- --nocapture`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-308` is complete. ANALYZE data-prefix construction now reserves capacity from known table-name length.
