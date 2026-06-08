# BENCHPROD-029 Production Storage Soak and VACUUM

## Goal

Add a production storage soak loop that exercises checkpoint, VACUUM-equivalent compaction, graceful restart, forced-kill recovery, visibility checks, secondary-index checks, and SSTable growth guards.

## Implementation

- `E:\Playground\FusionDB-bench\storage_soak.py`
  - Added an isolated FusionDB release-server soak runner.
  - Executes insert / update / delete cycles against `soak_kv`.
  - Calls HTTP `/checkpoint` and `/compact`; `/compact` is the current VACUUM-equivalent storage compaction endpoint.
  - Performs periodic graceful restarts and forced kills, then validates `SELECT COUNT(*)` and indexed bucket counts after restart.
  - Emits `storage_soak_summary.json` and `.md` under `runs\<run-name>\`.
- `E:\Playground\FusionDB-bench\README.md`
  - Documented tiny development smoke and medium multi-hour production soak commands.
- `src/storage/fusion.rs`
  - Fixed reopened `FusionStorage` so active MemTable IDs start after the highest existing SSTable ID instead of reusing `1.sst`.
  - Restores `current_ts` by scanning loaded SSTable keys, not just first/last metadata keys.
  - Changed `FusionTransaction::get` to choose the latest visible MVCC timestamp across memtables and SSTables instead of trusting SSTable ID order after compaction.
  - Changed immutable MemTable range scans to use `SkipMap.range(...)` as the correctness path instead of the optional FBTree read index.
  - Serialized compaction with an async mutex, deferred obsolete SSTable file deletion until no reader holds the old `Arc<SsTable>`, and made SSTable iterator read errors propagate during range scans and compaction.

## Root Cause

The first tiny soak failed after cycle 4 forced-kill recovery:

```text
cycle_4_after_forced_kill count mismatch: observed=12; expected=80
```

Two storage recovery assumptions were unsafe:

- Reopening storage created the active MemTable with ID `1`, so post-restart flush could overwrite an existing `1.sst`.
- `get` treated newer SSTable IDs as newer data, but compaction writes older candidate data into a new high-ID SSTable.

The timestamp restore path also only inspected first/last SSTable keys, which could restore a stale `current_ts` when the highest commit timestamp was in the middle of an SSTable.

The first medium multi-hour run then failed at cycle 655:

```text
cycle_655 count mismatch: observed=143611; expected=144100
```

A targeted 700-cycle rerun after switching immutable MemTable scans away from FBTree crossed cycle 655 but failed at cycle 687:

```text
cycle_687 count mismatch: observed=150651; expected=151140
```

Both failed runs returned the correct row count after reopening the same data directory, which showed the rows were durable and the defect was runtime scan visibility. A storage-level diagnostic reproduced the issue while a scan held old SSTable handles during compaction. Compaction removed old SSTables from the live list and immediately deleted their files; a concurrent range scan could still hold `Arc<SsTable>` values for those files, and the scan path treated iterator read errors as end-of-iterator, producing a smaller count. The fix keeps retired SSTables in an obsolete list until readers release them and propagates iterator read errors instead of silently truncating scans.

## Verification

- `cargo test fusion_reopen_uses_fresh_memtable_id_after_existing_sstables`
  - Failed before the fix; passed after the fix.
- `cargo test fusion_get_uses_latest_mvcc_timestamp_after_compaction`
  - Failed before the fix; passed after the fix.
- `cargo test fusion_reopen_restores_current_ts_from_all_sstable_keys`
  - Passed after the fix.
- `cargo test --lib storage::fusion::tests::`
  - Passed: 13/13.
- `cargo test --test sql_dml`
  - Passed: 43/43.
- `cargo test --test sql_index_cache`
  - Passed: 36/36.
- `cargo test --test sql_group_aggregate`
  - Passed: 49/49.
- `cargo build --release --bin fusiondb`
  - Passed.
- `python storage_soak.py --scale tiny --run-name storage_soak_benchprod029_tiny_20260608_fix --max-sstable-count 16 --fail-on-gap`
  - Passed: 4 cycles, 25/25 steps, final live rows 80, final SSTable count 3.
- `python storage_soak.py --scale small --run-name storage_soak_benchprod029_small_20260608_fix --max-sstable-count 32 --fail-on-gap`
  - Passed: 30 cycles, 168/168 steps, final live rows 2640, final SSTable count 3, 30 checkpoint loops, 15 compact loops, 14 completed compactions, 8 graceful restarts, 7 forced kills.
- `python storage_soak.py --scale medium --cycles 700 --checkpoint-every 2 --compact-every 4 --restart-every 30 --kill-every 90 --max-sstable-count 64 --fail-on-gap --run-name storage_soak_benchprod029_medium_700cycles_20260608_121332_deferreddelete`
  - Passed: 700 cycles, final live rows 154000, final SSTable count 3, 350 checkpoint loops, 175 compact loops, 7 completed compactions, 16 graceful restarts, 7 forced kills, 724 visibility checks.
- `python storage_soak.py --scale medium --duration-seconds 7200 --checkpoint-every 2 --compact-every 4 --restart-every 30 --kill-every 90 --max-sstable-count 64 --fail-on-gap --run-name storage_soak_benchprod029_medium_7200s_20260608_124142_deferreddelete`
  - Passed: 1522 cycles, final live rows 334840, elapsed 7201.438 seconds, final summary SSTable count 4/64, 761 checkpoint loops, 380 compact/VACUUM loops, 7 completed compactions, 34 graceful restarts, 16 forced kills, 1573 visibility checks, 0 failed steps.

## Result

This iteration adds the storage soak harness and fixes the recovery/compaction correctness issues exposed by tiny, small, targeted medium, and final medium multi-hour soak runs. `BENCHPROD-029` is complete: the medium 7200s run passed with checkpoint, compact/VACUUM, graceful restart, forced-kill recovery, visibility checks, secondary-index checks, and SSTable growth guard coverage.
