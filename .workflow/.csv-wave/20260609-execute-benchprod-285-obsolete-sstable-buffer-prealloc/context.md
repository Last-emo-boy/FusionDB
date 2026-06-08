# BENCHPROD-285 Obsolete SSTable Buffer Preallocation

## Goal

Avoid the first growth step when Fusion storage records the first obsolete SSTable after compaction.

## Implementation

- `src/storage/fusion.rs`
  - Added `obsolete_sstable_buffer`, which creates an obsolete SSTable queue with capacity 1.
  - Fusion storage initialization now uses this helper for `obsolete_sstables`.
  - Added unit coverage for the helper capacity.
  - Compaction replacement, Arc strong-count checks, deferred deletion, and file removal behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test obsolete_sstable_buffer_preallocates_first_compaction_output -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests::fusion_compaction_defers_obsolete_sstable_delete_until_readers_drop -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests -- --nocapture`
  - Passed: 18/18.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-285` is complete. Fusion storage now preallocates the obsolete SSTable queue for the first compaction output retained for deferred deletion.
