# BENCHPROD-282 SSTable File Candidate Preallocation

## Goal

Avoid the first growth step when Fusion storage startup discovers the first existing `.sst` file.

## Implementation

- `src/storage/fusion.rs`
  - Added `sstable_file_candidate_buffer`, which creates an SSTable file candidate vector with capacity 1.
  - Startup SSTable discovery now uses this helper before scanning the SSTable directory.
  - Added unit coverage for the helper capacity.
  - Directory scanning, SSTable ID parsing, sorted load order, SSTable opening, and timestamp restore behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test sstable_file_candidate_buffer_preallocates_first_file -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests::fusion_reopen_uses_fresh_memtable_id_after_existing_sstables -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests -- --nocapture`
  - Passed: 15/15.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-282` is complete. Fusion storage startup now preallocates the SSTable file candidate list for the first discovered SSTable file.
