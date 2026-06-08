# BENCHPROD-286 Vector Index Map Preallocation

## Goal

Avoid the first growth step when VectorIndex creates its default index wrapper.

## Implementation

- `src/storage/vector_index.rs`
  - Added `vector_index_map`, which creates the top-level index wrapper map with capacity 1.
  - `VectorIndex::new` now uses this helper.
  - Added unit coverage for the helper capacity.
  - Lazy wrapper creation, vector insertion, HNSW rebuild, delete, and search behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test vector_index_map_preallocates_default_index_slot -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::vector_index::tests -- --nocapture`
  - Passed: 7/7.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests::fusion_rebuild_vector_index_decodes_only_hnsw_columns -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-286` is complete. VectorIndex now preallocates the top-level index wrapper map for the default index slot.
