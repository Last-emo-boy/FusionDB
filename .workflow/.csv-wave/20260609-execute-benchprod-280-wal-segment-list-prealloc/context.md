# BENCHPROD-280 WAL Segment List Preallocation

## Goal

Avoid the first growth step for WAL segment discovery when the base WAL segment exists.

## Implementation

- `src/storage/wal.rs`
  - Added `wal_segment_list`, which creates a WAL segment vector with capacity 1.
  - `find_segments` now uses this helper before checking the base WAL file and scanning rotated segment files.
  - Added unit coverage for the helper capacity.
  - Segment path construction, base-file existence checks, rotated segment parsing, sorting, replay, and truncate behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test test_wal_segment_list_preallocates_base_segment -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::wal::tests -- --nocapture`
  - Passed: 8/8.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::memory::tests -- --nocapture`
  - Passed: 7/7.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-280` is complete. WAL segment discovery now preallocates the segment list for the common base segment insertion.
