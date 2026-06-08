# BENCHPROD-294 Index-Scan Data Key Preallocation

## Goal

Avoid generic `format!` allocation work when index scan hot paths build `data:<table>:<row_id>` keys from known component lengths.

## Implementation

- `src/execution/scan/index_plan.rs`
  - Added `data_key_for_row_id()` with explicit capacity for `data:<table>:<row_id>`.
  - Added `data_key_upper_bound_for_row_id()` with explicit capacity for the NUL-suffixed primary-key range upper bound.
  - Replaced `format!` key construction in full-row fetch, primary-key `IN`, primary `LIKE` prefix scan, and primary `BETWEEN` range scan paths.
  - Added unit coverage for helper output and capacity.
  - Generated key bytes, row-cache keys, scan prefixes, range upper bounds, index exactness, row decoding, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test data_key -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml -- --nocapture`
  - Passed: 44/44.
  - The command emitted existing SSTable retry warnings after the passing result and exited successfully.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-294` is complete. Index scan primary-key data-key construction now reserves capacity from known table and row-id lengths.
