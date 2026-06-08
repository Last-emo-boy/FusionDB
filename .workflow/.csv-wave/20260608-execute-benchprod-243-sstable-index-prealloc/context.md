# BENCHPROD-243 SSTable Index Array Preallocation

## Goal

Avoid implicit vector growth and an extra map traversal while reconstructing SSTable parallel index arrays during `SsTable::open`.

## Implementation

- `src/storage/sstable.rs`
  - Replaced separate `index.keys().cloned().collect()` and `index.values().cloned().collect()` calls with `Vec::with_capacity(index.len())`.
  - Filled `index_keys` and `index_offsets` in one ordered pass over the deserialized `BTreeMap`.
  - Preserved `BTreeMap` ordering and positional alignment between each block key and offset.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_fusion_storage_sstable_seek_finds_tpcc_district_mid_block -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_fusion_storage_prefix_scan_seeks_inside_sstable_block -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test fusion_reopen_uses_fresh_memtable_id_after_existing_sstables -- --nocapture`
  - Passed: 1/1 target unit test; remaining binaries had 0 matching tests.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test fusion_last_reads_visible_key_from_sstable -- --nocapture`
  - Passed: 1/1 target unit test; remaining binaries had 0 matching tests.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

The two `sql_dml` focused storage tests printed a transient SSTable open retry warning before passing. The final test result was green in both cases.

## Result

`BENCHPROD-243` is complete. SSTable index key and offset arrays now preallocate from the known deserialized index length and are filled together in a single ordered pass.
