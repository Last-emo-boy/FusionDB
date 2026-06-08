# BENCHPROD-292 Fusion Merge Heap Preallocation

## Goal

Avoid the first growth step for Fusion merge heaps when the number of candidate iterators is already known.

## Implementation

- `src/storage/fusion.rs`
  - Added `merge_heap(capacity)` to create `BinaryHeap<MergeItem>` with an explicit capacity.
  - Compaction now initializes its merge heap with the number of SSTable iterators in the compaction fan-in.
  - Visible-range scans now initialize their merge heap with capacity for the write-buffer iterator, memtable iterators, and SSTable iterators.
  - The visible-range SSTable snapshot is cloned before heap creation so capacity and iterator creation use the same snapshot.
  - Merge ordering, MVCC visibility, deduplication, visitor short-circuiting, compaction output, and DML behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test merge_heap_reserves_candidate_iterators -- --nocapture`
  - Passed: 1/1.
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

`BENCHPROD-292` is complete. Fusion merge heaps now reserve capacity from known iterator counts before the first push.
