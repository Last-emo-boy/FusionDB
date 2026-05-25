# TASK-115 Execution Context

- Target: `src/storage/fusion.rs`.
- Change: visible range memtable snapshot storage now preallocates from immutable memtable count plus active memtable.
- Change: visible range memtable iterator storage now preallocates from collected memtable count.
- Rationale: `for_each_visible_range` always visits one active memtable and the current immutable memtable snapshot, so these counts give conservative capacity bounds.
