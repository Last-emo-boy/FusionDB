# BENCHPROD-309 Foreign-Key Data Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when foreign-key table value scans build `data:<table>:` prefixes from known table-name length.

## Implementation

- `src/execution/foreign_key.rs`
  - Added `foreign_key_data_prefix_for_table()` with explicit capacity for `data:<table>:`.
  - Replaced `format!` prefix construction in `table_has_column_values()`.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, transaction scan prefixes, row-cache lookups, row decoding, FK child/parent checks, COPY constraint checks, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test foreign_key_data_ -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_view_show_constraints -- --nocapture`
  - Passed: 16/16.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml test_copy_from_csv_enforces_constraints_on_direct_path -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml test_fusion_storage_tpcc_order_fk_chain_after_many_customers -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

Focused FK tests were launched in parallel and printed Cargo file-lock wait messages before passing.

## Result

`BENCHPROD-309` is complete. Foreign-key data-prefix construction now reserves capacity from known table-name length.
