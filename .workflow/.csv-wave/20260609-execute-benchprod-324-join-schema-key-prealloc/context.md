# BENCHPROD-324 Join Schema Key Preallocation

## Goal

Avoid generic `format!` allocation work when join paths build `schema:<table>` lookup keys from known table-name length.

## Implementation

- `src/execution/scan/join.rs`
  - Added `join_schema_key_for_table()` with explicit capacity for `schema:<table>`.
  - Replaced schema lookup key construction in join base scan.
  - Replaced schema lookup key construction in comma-join reorder planning.
  - Replaced schema lookup key construction in right probe schema loading.
  - Replaced schema lookup key construction in first-relation projection pruning.
  - Added unit coverage for helper output and capacity.
  - Generated schema key bytes, join planning, projection pruning, row-cache behavior, and SQL join results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test join_schema_key -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_join test_join_base_scan_reuses_row_cache -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_join -- --nocapture`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

`git diff --check` printed the existing CRLF normalization warning for `src/execution/scan/join.rs` while exiting successfully.

## Result

`BENCHPROD-324` is complete. Join schema-key construction now reserves capacity from known table-name length.
