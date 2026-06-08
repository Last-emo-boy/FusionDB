# BENCHPROD-273 WAL Replay Aggregate Entry Reserve

## Goal

Avoid repeated `Vec` growth while WAL replay aggregates entries from non-empty segment files.

## Implementation

- `src/storage/wal.rs`
  - `WalManager::replay` now reserves `all_entries` using `replay_entry_capacity_hint(file_len)` before replaying each non-empty segment.
  - The capacity hint is the existing bounded file-size-derived helper.
  - Added `test_wal_replay_reserves_total_entries_from_segment_hint` to prove the returned replay vector preserves the segment-derived capacity hint.
  - WAL record decoding, ordering, truncation, and persistence behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test test_wal_replay_reserves_total_entries_from_segment_hint -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::wal::tests -- --nocapture`
  - Passed: 7/7.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-273` is complete. WAL replay now reserves its aggregate result buffer from bounded per-segment file-size hints before extending with decoded entries.
