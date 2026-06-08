# BENCHPROD-331 Subquery Schema Key Preallocation

## Goal

Avoid generic `format!` allocation work when subquery local-scope paths build `schema:<table>` lookup keys from known table-name length.

## Implementation

- `src/execution/expr/subquery.rs`
  - Added `subquery_schema_key_for_table()` with explicit capacity for `schema:<table>`.
  - Replaced schema lookup key construction in `table_factor_schema_if_simple_table()`.
  - Replaced schema lookup key construction in `append_local_scope_for_factor()`.
  - Added unit coverage for helper output and capacity.
  - Generated schema key bytes, subquery materialization, correlated EXISTS membership, CTE behavior, recursive query behavior, and join-side query behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test subquery_schema_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_set_subquery subquery -- --nocapture`
  - Passed: 5/5.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_set_subquery exists -- --nocapture`
  - Passed: 6/6.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_set_subquery -- --nocapture`
  - Passed: 48/48.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_join -- --nocapture`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

An attempted `sql_join` test filter matched 0 tests and is not counted as verification; full `sql_join` was run afterward.

`git diff --check` printed the existing CRLF normalization warning for `src/execution/expr/subquery.rs` while exiting successfully.

## Result

`BENCHPROD-331` is complete. SUBQUERY schema-key construction now reserves capacity from known table-name length.
