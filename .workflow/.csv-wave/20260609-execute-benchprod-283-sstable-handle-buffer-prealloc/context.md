# BENCHPROD-283 SSTable Handle Buffer Preallocation

## Goal

Avoid the first growth step when Fusion storage startup opens the first existing SSTable.

## Implementation

- `src/storage/fusion.rs`
  - Added `sstable_handle_buffer`, which creates a loaded SSTable handle vector with capacity 1.
  - Startup SSTable loading now uses this helper before opening existing SSTable files.
  - Added unit coverage for the helper capacity.
  - SSTable candidate scanning, sorted load order, active memtable id selection, and timestamp restore behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test sstable_handle_buffer_preallocates_first_sstable -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests::fusion_reopen_uses_fresh_memtable_id_after_existing_sstables -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests -- --nocapture`
  - Passed: 16/16.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-283` is complete. Fusion storage startup now preallocates the loaded SSTable handle list for the first opened SSTable.
