# BENCHPROD-275 Vector Batch Wrapper Preallocation

## Goal

Avoid an avoidable vector-map allocation adjustment when `VectorIndex::batch_insert` creates a fresh HNSW wrapper.

## Implementation

- `src/storage/vector_index.rs`
  - Added `HnswIndexWrapper::with_vector_capacity`.
  - Added `VectorIndex::get_or_create_wrapper_with_capacity`.
  - `VectorIndex::batch_insert` now passes `items.len()` into wrapper creation for a fresh index.
  - Existing wrappers still use `wrapper.vectors.reserve(items.len())` before batch upsert.
  - Added unit coverage for wrapper vector-map capacity.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test wrapper_with_vector_capacity_reserves_vectors -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::vector_index::tests -- --nocapture`
  - Passed: 6/6.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test fusion_rebuild_vector_index_decodes_only_hnsw_columns -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac hnsw -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-275` is complete. Fresh vector index wrappers created by batch insertion now preallocate their vector map from the incoming batch size while preserving HNSW behavior.
