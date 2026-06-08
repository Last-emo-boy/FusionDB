# BENCHPROD-244 WAL Batch Preallocation

## Goal

Avoid implicit vector growth while cloning a synchronous WAL append batch from a borrowed input slice into the owned send buffer.

## Implementation

- `src/storage/wal.rs`
  - Replaced `entries.iter().map(...).collect()` in `WalManager::append_batch` with `Vec::with_capacity(entries.len())`.
  - Cloned `Put` and `Delete` variants into the owned vector with explicit pushes.
  - Added `test_wal_append_batch_replay_preserves_order` to cover batch append replay order and payload preservation.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::wal::tests::test_wal_append_batch_replay_preserves_order -- --nocapture`
  - Passed: 1/1 target unit test; remaining binaries had 0 matching tests.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::wal::tests -- --nocapture`
  - Passed: 5/5 target WAL module tests; remaining binaries had 0 matching tests.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-244` is complete. The synchronous WAL batch wrapper now preallocates its owned clone buffer from the known input slice length and preserves existing WAL entry order and clone semantics.
