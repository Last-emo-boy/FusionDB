# BENCHPROD-260 WAL Replay Entry Preallocation

## Goal

Avoid initial vector growth while replaying WAL segment records during storage recovery.

## Implementation

- `src/storage/wal.rs`
  - Added `MIN_WAL_REPLAY_RECORD_BYTES` and `MAX_WAL_REPLAY_PREALLOC_ENTRIES`.
  - Added `replay_entry_capacity_hint`.
  - `replay_single_file` now creates `entries` with `Vec::with_capacity(...)`.
  - The hint is capped at 8192 entries so large WAL segments do not cause excessive preallocation.
  - Added unit coverage for the capacity hint.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::wal::tests -- --nocapture`
  - Passed: 6/6.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::fusion::tests::fusion_reopen_restores_current_ts_from_all_sstable_keys -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-260` is complete. WAL segment replay now preallocates its entry buffer from a bounded file-size hint while preserving replay and partial-tail truncation behavior.
