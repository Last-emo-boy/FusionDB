# BENCHPROD-332 Query Schema Key Preallocation

## Goal

Avoid generic `format!` allocation work when query paths build `schema:<table>` lookup keys from known table-name length.

## Implementation

- `src/execution/query/mod.rs`
  - Added `query_schema_key_for_table()` with explicit capacity for `schema:<table>`.
  - Replaced materialized CTE schema storage and cleanup key construction.
  - Replaced join aggregate schema lookup key construction.
  - Replaced DISTINCT fast path schema lookup key construction.
  - Replaced simple aggregate and COUNT DISTINCT schema lookup key construction.
  - Replaced GROUP BY fast path schema lookup key construction.
  - Added unit coverage for helper output and capacity.
  - Generated schema key bytes, CTE materialization, join aggregate behavior, DISTINCT fast paths, aggregate fast paths, GROUP BY behavior, and SELECT behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test query_schema_key -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_set_subquery -- --nocapture`
  - Passed: 48/48.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_join -- --nocapture`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_group_aggregate -- --nocapture`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_select -- --nocapture`
  - Passed: 27/27.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

The `query_schema_key` filter matched both the new query helper test and the existing subquery helper test.

`sql_group_aggregate` printed Cargo package-cache file-lock waits before passing.

`git diff --check` printed the existing CRLF normalization warning for `src/execution/query/mod.rs` while exiting successfully.

## Result

`BENCHPROD-332` is complete. QUERY schema-key construction now reserves capacity from known table-name length.
