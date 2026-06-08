# BENCHPROD-284 Immutable Memtable Buffer Preallocation

## Goal

Avoid the first growth step when Fusion storage records the first flushed immutable memtable.

## Implementation

- `src/storage/fusion.rs`
  - Added `immutable_memtable_buffer`, which creates an immutable memtable queue with capacity 1.
  - Fusion storage initialization now uses this helper for `immutable_memtables`.
  - Added unit coverage for the helper capacity.
  - Flush visibility, SSTable registration, scan, and compaction behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test immutable_memtable_buffer_preallocates_first_flush -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests::fusion_flush_candidate_remains_visible_until_sstable_registration -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests -- --nocapture`
  - Passed: 17/17.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-284` is complete. Fusion storage now preallocates the immutable memtable queue for the first flushed memtable.
