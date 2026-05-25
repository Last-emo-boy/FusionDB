# TASK-058 Compaction Candidate Allocation

Scope: `src/storage/fusion.rs`

Implemented:
- Added `COMPACTION_FANIN` for the fixed four-SSTable compaction fan-in.
- Replaced candidate `Vec<Arc<SsTable>>` construction with `[Arc<SsTable>; COMPACTION_FANIN]`.
- Replaced old SST ID `Vec<u64>` collection with fixed-size candidate ID comparison.

Validation is recorded in `.workflow/.csv-wave/20260525-verify-compaction-candidates-array/verification.json`.
