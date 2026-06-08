# BENCHPROD-313 CTE Data Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when materialized CTE cleanup scans build `data:<cte>:` prefixes from known CTE-name length.

## Implementation

- `src/execution/query/mod.rs`
  - Added `materialized_cte_data_prefix_for_name()` with explicit capacity for `data:<cte>:`.
  - Replaced `format!` prefix construction in `clear_materialized_cte()`.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, `scan_prefix` inputs, delete keys, and CTE query results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test materialized_cte_data_prefix -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_set_subquery test_cte_basic -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_set_subquery test_recursive_cte_alias_can_rename_prefix_columns -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-313` is complete. Materialized CTE cleanup data-prefix construction now reserves capacity from known CTE-name length.
