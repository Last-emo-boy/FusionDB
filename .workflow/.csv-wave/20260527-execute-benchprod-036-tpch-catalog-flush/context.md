# BENCHPROD-036 TPC-H Catalog Flush Visibility

Date: 2026-05-27
Scope: FusionDB database core only; dashboard/ui excluded.

## Objective

Fix the full medium benchmark failure where the TPC-H-like suite loaded data and created indexes, but subsequent queries reported missing `bench_tpch_*` tables.

## Root Cause

`FusionStorage::flush_loop` removed an immutable memtable from `immutable_memtables` before the memtable had been written to an SSTable and registered in `sstables`.

During that gap, a new transaction could not see keys that had moved out of the active memtable but were not yet visible through SSTables. TPC-H medium setup triggers this path during load/index creation, which made `schema:bench_tpch_*` temporarily invisible to later queries.

## Implementation

- Added `next_memtable_to_flush()` to select a flush candidate without removing it from the visible immutable list.
- Added `mark_memtable_flushed()` to remove the immutable memtable only after its SSTable has been opened and registered.
- Updated `flush_loop` so immutable memtables remain visible throughout disk flush.
- Added a FusionStorage regression test proving a queued flush candidate remains readable until it is explicitly marked flushed.

## Verification

- `cargo fmt --check`
- `cargo check --lib`
- `cargo test fusion_flush_candidate_remains_visible_until_sstable_registration -- --nocapture`
- `cargo build --release --bin fusiondb`
- `python fusiondb_matrix.py --scale medium --suite tpch --load-mode insert --allow-failures --run-name matrix_tpch_medium_after_benchprod036_20260527`
- `python fusiondb_matrix.py --scale medium --suite all --load-mode insert --allow-failures --run-name matrix_all_medium_after_benchprod036_20260527`

## Result

TPC-H medium after fix:

- Report: `E:/Playground/FusionDB-bench/runs/matrix_tpch_medium_after_benchprod036_20260527/matrix_summary.md`
- Suite status: `passed`
- Cases: `5`
- Case errors: `0`
- Avg P95: `1.169 ms`
- Avg ops/sec: `1190.6`

Full medium matrix after fix:

- Report: `E:/Playground/FusionDB-bench/runs/matrix_all_medium_after_benchprod036_20260527/matrix_summary.md`
- Suite pass rate: `9/9`
- Case pass rate: `39/39`

The previous full medium baseline was `8/9` suites and `34/39` cases because TPC-H failed with missing-table errors.

## Next TASK Signals

- `BENCHPROD-032`: TPC-C OrderStatus p95 remains high at `39.258 ms`; optimize indexed top-k/subquery path.
- `BENCHPROD-033`: CH-benCHmark customer order join p95 remains high at `27.895 ms`; improve join planning.
- `BENCHPROD-037`: ANN HNSW nearest neighbor p95 remains high at `44.823 ms`; add recall/build metrics and optimize vector path.
- `BENCHPROD-038`: YCSB short range scan p95 remains high at `11.314 ms`; improve primary-key range scan path.
