# BENCHPROD-257 FBTree Node Preallocation

## Goal

Avoid repeated small vector growth while constructing FBTree leaf and inner nodes during bulk load.

## Implementation

- `src/storage/fbtree.rs`
  - Added `empty_leaf_node` for FANOUT-sized leaf buffers.
  - Added `empty_inner_node` for FANOUT-sized child buffers and `FANOUT - 1` anchor buffers.
  - Reused the helpers in `FBTree::new` and `FBTree::bulk_load`.
  - Added direct unit coverage for node capacities and basic `get` / `scan` behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::fbtree::tests -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::fusion::tests::fusion_immutable_memtable_prefix_scan_covers_all_large_fbtree_keys -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-257` is complete. FBTree node construction now preallocates from the fixed fanout bound while preserving bulk-load, lookup, and scan behavior.
