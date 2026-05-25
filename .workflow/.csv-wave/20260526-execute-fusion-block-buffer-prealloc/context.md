# TASK-116 Execution Context

- Target: `src/storage/fusion.rs`.
- Change: introduced `SSTABLE_BLOCK_BUFFER_CAPACITY` for the existing 4096-byte block flush threshold.
- Change: flush, compaction, and shutdown SSTable block buffers now preallocate to that threshold.
- Rationale: each block buffer is repeatedly extended until the same threshold before flush, so matching initial capacity avoids early growth.
