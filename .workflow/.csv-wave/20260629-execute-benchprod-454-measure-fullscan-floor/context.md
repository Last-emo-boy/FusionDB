# BENCHPROD-454 Execution Context — Measure the single-node full-scan floor

Goal: measure where the ~238ms single-node full-table-scan floor goes and decide whether parallel
range-merge (456) is worth implementing, gated on cores >= 4 and "allocator is not the ceiling".

## Baseline (large scale, 50k base rows = ~100k bench rows, glibc malloc, commits through 449)
Captured via `BENCH_SCALE=large python benchmark.py` against a release server. Full-scan-bound family:
- Full scan (val=X)        230.9 ms  (P50 221.9, P90 380.9; CV 29.9%; 49 rows)
- BETWEEN range            231.0 ms  (5136 rows)
- LIKE prefix              202.6 ms  (5000 rows)
- ORDER BY val DESC L50    394.3 ms  (full scan + sort)
- GROUP BY + HAVING        144.1 ms  (full scan + group)
- Index scan (val=X)         1.3 ms  -> Index speedup 172.6x over the full scan
Fast streaming paths (NOT full-materialize): COUNT(*) 18.2 ms, SUM(amount) 17.2 ms.

## Cost structure (from code + prior tickets)
The filtered full scan (`scan_single_table`, selection present) runs in two phases:
- Phase 1 (SERIAL): `scan_routed_data_prefixes_for_table` -> `for_each_visible_range` — an N-way
  BinaryHeap MVCC merge over the active/immutable memtables + all overlapping SSTables, deduping to
  the latest visible version and materializing EVERY live (key, value) into a `Vec<(Vec<u8>,Vec<u8>)>`
  (~2 allocations/row -> ~200k allocs for 100k rows).
- Phase 2 (PARALLEL, BENCHPROD-440): rayon `par_iter` decodes + evaluates the WHERE predicate.
BENCHPROD-440 made phase 2 parallel and it did NOT move the Full-scan number -> the floor is phase 1
(the serial heap-merge + per-row key/value clones), not decode/filter. Cores available: 12 (>= 4 gate met).

## Allocator gate — UNRESOLVED IN THIS ENVIRONMENT
Phase 1 is allocation-heavy (~200k clones on a serial path), so glibc malloc arena locking is a prime
suspect and the decisive gate for whether parallel range-merge will scale (parallel allocation would
contend on a global allocator). Tried to A/B a custom allocator (mimalloc, the standard low-risk win):
it BUILDS and runs in a pure foreground process, but is killed with signal 16 (exit 144) the moment the
process is backgrounded with `&` / detached by the harness — a sandbox/container incompatibility, not a
code defect. It could not be benchmarked here, so it was REVERTED (unverifiable changes are not committed
under the benchmark-gated discipline). Recommendation for the real (non-sandboxed) deployment: A/B
mimalloc or jemalloc first; if it materially lowers the Full-scan number, allocation is the ceiling and
a better allocator should land before/with parallel range-merge.

## Decision for BENCHPROD-456 (parallel range-merge)
Cores (12) clear the >= 4 gate; phase 1 is confirmed the floor; phase 2 is already parallel. Parallel
range-merge is justified. Design: split the routed data prefix range `[prefix, prefix+0xFF)` into K
disjoint sub-ranges and run K concurrent `scan_range` calls (tokio, multi-threaded runtime), then
concatenate in sub-range order (disjoint ranges -> disjoint, already-ordered rows; MVCC read_ts is
shared so visibility is identical). The crux is data-aware split-point derivation: naive byte-space
split is unbalanced for clustered integer PKs (encode_i64_comparable clusters sequential ids in a narrow
sub-range). Use SSTable block-index first-keys as sorted distribution samples and pick K-1 quantiles, or
decode the PK range from first/last keys for integer PKs and partition the id range; fall back to serial
for non-integer PKs or below a row-count threshold. Gate the parallel path the same way the existing
phase-2 rayon path is gated (row-count threshold). Requires the BENCHPROD-453 `merge_visible_range`
extraction first, full verification gate, adversarial review, and an A/B benchmark to confirm the win
(and rule out allocator contention) before commit.

## Status
Measurement complete; baseline recorded. 456 implementation deferred to a focused effort (large
concurrency change requiring A/B verification). No code committed for 454 (measurement only).
