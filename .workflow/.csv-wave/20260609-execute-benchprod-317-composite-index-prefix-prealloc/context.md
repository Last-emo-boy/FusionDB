# BENCHPROD-317 Composite Index Prefix Preallocation

## Goal

Avoid intermediate joined-column allocation when composite index key and scan paths build `index:<table>:<columns>:` prefixes from known table and column name lengths.

## Implementation

- `src/execution/composite_index.rs`
  - Replaced `composite_index_prefix()` `format!` plus `columns.join(",")` with one explicit-capacity `String`.
  - Pushed comma-separated columns directly into the prefix buffer.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, composite index keys, scan prefixes, range scans, `SHOW INDEXES` output, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test composite_index_prefix -- --nocapture`
  - Passed: helper 1/1 and matching `sql_index_cache` regression 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_ddl test_create_table_table_level_composite_primary_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-317` is complete. Composite index prefix construction now reserves capacity from known table and column name lengths.
