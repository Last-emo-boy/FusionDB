# BENCHPROD-258 FBTree Bulk-Load Buffer Preallocation

## Goal

Avoid repeated vector growth in FBTree bulk-load outer buffers by using iterator size hints and the fixed `FANOUT` bound.

## Implementation

- `src/storage/fbtree.rs`
  - Added `node_group_count` for fanout group estimation.
  - Added `bulk_load_node_capacity` for leaf + inner arena capacity estimation.
  - `bulk_load` now preallocates the leaf id list from the iterator size hint.
  - `bulk_load` now preallocates the node arena from the estimated full tree node count.
  - Each inner build level now preallocates `next_level_ids` from the current level count.
  - Extended unit coverage for capacity helpers and arena capacity.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::fbtree::tests -- --nocapture`
  - Passed: 3/3.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::fusion::tests::fusion_immutable_memtable_prefix_scan_covers_all_large_fbtree_keys -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-258` is complete. FBTree bulk loading now preallocates leaf, arena, and next-level buffers without changing tree construction behavior.
