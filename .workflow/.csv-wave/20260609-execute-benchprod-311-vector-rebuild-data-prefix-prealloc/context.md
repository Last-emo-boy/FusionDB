# BENCHPROD-311 Vector Rebuild Data Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when Fusion storage vector-index rebuild scans build `data:<table>:` prefixes from known table-name length.

## Implementation

- `src/storage/fusion.rs`
  - Added `vector_rebuild_data_prefix_for_table()` with explicit capacity for `data:<table>:`.
  - Replaced `format!` prefix construction in `FusionStorage::rebuild_vector_index()`.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, transaction scan prefixes, decoded row IDs, HNSW batches, and SQL HNSW query results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test vector_rebuild_data_prefix -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test fusion_rebuild_vector_index_decodes_only_hnsw_columns -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_returning_upsert_vector_rbac test_hnsw_order_by_projection -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

The focused helper test printed Cargo file-lock wait messages before passing.

## Result

`BENCHPROD-311` is complete. Fusion storage vector-index rebuild data-prefix construction now reserves capacity from known table-name length.
