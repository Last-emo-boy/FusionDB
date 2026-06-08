# BENCHPROD-252 Obsolete SSTable Deletion Preallocation

## Goal

Avoid implicit vector growth while collecting obsolete SSTable paths that are ready for file deletion after compaction.

## Implementation

- `src/storage/fusion.rs`
  - Added `ready_to_delete.reserve(obsolete.len())` in `collect_obsolete_sstables`.
  - Uses the existing `obsolete_sstables` write-lock scope, where the obsolete list length is already known.
  - Preserves `Arc::strong_count` gating, `Vec::remove` behavior, path cloning, and lock release before asynchronous file deletion.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::fusion::tests::fusion_compaction_defers_obsolete_sstable_delete_until_readers_drop -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test storage::fusion::tests::fusion_get_uses_latest_mvcc_timestamp_after_compaction -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-252` is complete. Obsolete SSTable collection now preallocates the deletion-path buffer from the known obsolete list upper bound without changing reader-safe deletion semantics.
