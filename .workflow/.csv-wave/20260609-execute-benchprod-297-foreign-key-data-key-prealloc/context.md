# BENCHPROD-297 Foreign-Key Data Key Preallocation

## Goal

Avoid generic `format!` allocation work when foreign-key parent existence checks build `data:<table>:<row_id>` keys from known component lengths.

## Implementation

- `src/execution/foreign_key.rs`
  - Added `foreign_key_data_key_for_row_id()` with explicit capacity for `data:<table>:<row_id>`.
  - Replaced `format!` key construction in single-column primary-key FK parent checks.
  - Replaced `format!` key construction in composite primary-key parent row checks.
  - Added unit coverage for helper output and capacity.
  - Generated key bytes, transaction get lookups, missing-parent errors, parent-reference blocking, and FK semantics are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test foreign_key_data_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_view_show_constraints -- --nocapture`
  - Passed: 16/16.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml test_fusion_storage_tpcc_order_fk_chain_after_many_customers -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml test_copy_from_csv_enforces_constraints_on_direct_path -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-297` is complete. Foreign-key parent data-key construction now reserves capacity from known table and row-id lengths.
