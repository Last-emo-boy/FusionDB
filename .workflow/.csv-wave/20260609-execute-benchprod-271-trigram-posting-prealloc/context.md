# BENCHPROD-271 Trigram Posting Map Preallocation

## Goal

Avoid initial `HashMap` growth when trigram indexing first creates table, column, and row-id maps.

## Implementation

- `src/storage/trigram.rs`
  - `TrigramIndex::add_with_id_str` now creates a new table map with capacity 1.
  - A new column posting map now uses the first row's deduplicated trigram count as its capacity.
  - A new row-id map now starts with capacity 1.
  - Existing trigram posting, search, update, and delete behavior is unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::trigram::tests -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_indexed_text_insert_updates_trigram_index_on_fusion_storage -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_create_index_backfills_trigram_index_on_fusion_storage -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_update_refreshes_trigram_index_on_fusion_storage -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_delete_removes_trigram_index_on_fusion_storage -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-271` is complete. Trigram insertion now preallocates new internal maps from known first-insert sizes.
