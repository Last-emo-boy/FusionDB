# BENCHPROD-300 Materialized CTE Data Key Preallocation

## Goal

Avoid generic `format!` allocation work when materialized CTE row writes build `data:<cte>:<row_id>` keys from known component lengths.

## Implementation

- `src/execution/query/mod.rs`
  - Added `materialized_cte_data_key_for_row_id()` with explicit capacity for `data:<cte>:<row_id>`.
  - Replaced `format!` key construction in materialized CTE temporary row writes.
  - Added unit coverage for helper output and capacity.
  - Generated key bytes, row-cache invalidation keys, CTE cleanup prefixes, recursive CTE replacement, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test materialized_cte_data_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_set_subquery test_cte_basic -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_set_subquery test_recursive_cte_alias_can_rename_prefix_columns -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_set_subquery -- --nocapture`
  - Passed: 48/48.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

Focused CTE tests were launched in parallel and printed Cargo file-lock wait messages before passing.

## Result

`BENCHPROD-300` is complete. Materialized CTE data-key construction now reserves capacity from known CTE name and row-id lengths.
