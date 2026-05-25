# TASK-118 Execution

Target: `src/storage/fusion.rs`

Change:
- Added `sstables_vec.reserve(files.len())` after sorting discovered `.sst` files and before opening them.

Behavior:
- SSTable discovery, sort order, and best-effort open behavior are unchanged.
- The optimization only avoids repeated growth of the loaded SSTable vector on startup.
