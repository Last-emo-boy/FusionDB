# BENCHPROD-312 Query Aggregate Data Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when query aggregate fast paths build `data:<table>:` prefixes from known table-name length.

## Implementation

- `src/execution/query/mod.rs`
  - Added `aggregate_data_prefix_for_table()` with explicit capacity for `data:<table>:`.
  - Replaced `format!` prefix construction in the aggregate `COUNT` prefix-count fast path.
  - Replaced `format!` prefix construction in the primary-key `MIN/MAX` key-bound fast path.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, `count_prefix` inputs, first/last key bounds, primary-key extraction, and SQL result rows are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test aggregate_data_prefix -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache test_select_count_primary_key_uses_prefix_count -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache test_select_qualified_min_max_primary_key_uses_key_bounds -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_view_show_constraints test_select_count_not_null_column_uses_prefix_count -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-312` is complete. Query aggregate data-prefix construction now reserves capacity from known table-name length.
