# BENCHPROD-213 Trigram Column Index Preallocation

## Goal

Avoid implicit vector growth while DML paths discover indexed text columns that need trigram index maintenance.

## Implementation

- `src/execution/dml/mod.rs`
  - Replaced `indexed_trigram_text_columns` `filter_map().collect()` with `Vec::with_capacity(schema.columns.len())`.
  - Preserved text type detection and BTree/FTS index-type filtering.
  - Preserved schema column order in the returned index list.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_indexed_text_insert_updates_trigram_index_on_fusion_storage -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_plain_text_insert_skips_trigram_index_on_fusion_storage -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_update_refreshes_trigram_index_on_fusion_storage -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_delete_removes_trigram_index_on_fusion_storage -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache`
  - Passed: 37/37.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-213` is complete. DML trigram text column discovery now preallocates column index vectors from schema column counts.
