# BENCHPROD-281 Obsolete SSTable Path Buffer Preallocation

## Goal

Avoid a growth step when collecting obsolete SSTable file paths that are ready for deletion.

## Implementation

- `src/storage/fusion.rs`
  - Added `obsolete_sstable_path_buffer`, which creates a `PathBuf` vector with the requested capacity.
  - `collect_obsolete_sstables` now allocates the deletion path buffer from `obsolete.len()` while holding the obsolete list lock.
  - Added unit coverage for the helper capacity.
  - Arc strong-count checks, retained obsolete SSTables, deletion ordering, and async file removal behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test obsolete_sstable_path_buffer_reserves_current_obsolete_len -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests::fusion_compaction_defers_obsolete_sstable_delete_until_readers_drop -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests -- --nocapture`
  - Passed: 14/14.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-281` is complete. Fusion storage obsolete SSTable cleanup now preallocates the ready-to-delete path buffer from the current obsolete list length.
