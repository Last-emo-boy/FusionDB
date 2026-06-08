# BENCHPROD-293 SSTable Block Entry Buffer Preallocation

## Goal

Avoid the first growth step for SSTable iterator block entry buffers when decoding entries from a data block.

## Implementation

- `src/storage/sstable.rs`
  - Added `block_entry_buffer()` to initialize the iterator queue with first-entry capacity.
  - Added `block_entry_reserve_count()` to derive a bounded reserve count from the parsed block entry count.
  - Changed `SsTable::new_iterator()` to use the preallocated block entry buffer.
  - Changed block parsing to reserve capacity after reading the block count, capped by `block_len / 8` because each encoded entry has at least two 4-byte length fields.
  - SSTable IO, CRC handling, lower-bound filtering, decoded entry order, Fusion scans, compaction, and DML behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::sstable::tests -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::fusion::tests -- --nocapture`
  - Passed: 20/20.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_dml -- --nocapture`
  - Passed: 44/44.
  - The command emitted existing SSTable retry warnings after the passing result and exited successfully.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-293` is complete. SSTable iterators now reserve block entry buffer capacity before pushing decoded entries.
