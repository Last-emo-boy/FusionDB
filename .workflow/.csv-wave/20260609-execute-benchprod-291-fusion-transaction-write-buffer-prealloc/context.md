# BENCHPROD-291 Fusion Transaction Write Buffer Preallocation

## Goal

Avoid the first growth step for a newly started Fusion transaction when it records its first buffered write.

## Implementation

- `src/storage/fusion.rs`
  - Added `transaction_write_buffer()` with capacity for the first buffered write entry.
  - Changed `FusionStorage::begin_transaction()` to use the helper for `FusionTransaction.write_buffer`.
  - Added unit coverage for first-write buffer preallocation.
  - MVCC read timestamps, buffered write order, conflict checks, WAL generation, memtable updates, rollback, and read-your-own-writes behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test fusion_transaction_write_buffer_preallocates_first_write -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests -- --nocapture`
  - Passed: 19/19.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml test_fusion_storage_prefix_scan_seeks_inside_sstable_block -- --nocapture`
  - Passed: 1/1.
  - The command emitted an existing SSTable retry warning after the passing result and exited successfully.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-291` is complete. New Fusion transactions now reserve one write-buffer slot for the first put or delete entry.
