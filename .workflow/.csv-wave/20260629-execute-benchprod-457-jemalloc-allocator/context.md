# BENCHPROD-457 Execution Context — jemalloc global allocator

Resolves the allocator gate that BENCHPROD-454 identified and BENCHPROD-456 left open. The full-scan
path clones ~2 allocations per row; once 456 parallelized the merge across cores, glibc malloc's arena
locking became the ceiling (456 stayed below linear speedup). Switched the server binary's
`#[global_allocator]` to jemalloc via `tikv-jemallocator` (one line in `src/main.rs` + the dep).

## Why jemalloc and not mimalloc
454 first tried mimalloc; it builds and runs as a pure foreground process but is killed with signal 16
the instant the server is backgrounded/detached in this sandbox (its purge background thread). jemalloc
spawns no background thread by default, so it survives backgrounding here — and is benchmark-verifiable
(TiKV-grade, battle-tested).

## Benchmark (large, 12 cores) — on top of 456's parallel scan
Full scan 148.6→84.4 ms, BETWEEN 148.9→112.1, LIKE 147.0→119.1, GROUP BY+HAVING 95.3→42.4,
ORDER BY val DESC 291.7→106.4, COUNT(*) 21.4→6.5, SUM 21.8→8.4; Base-category avg 56.6→32.8 ms.
**Cumulative vs the pre-456 serial+glibc baseline:** Full scan 230.9→84.4 (2.7×), GROUP BY+HAVING
144.1→42.4 (3.4×), ORDER BY val DESC 394.3→106.4 (3.7×), COUNT(*) 18.2→6.5 (2.8×), Base avg
79.4→32.8 (2.4×). Allocation is pervasive, so every query class improved (not just full scans). This
confirms the 454 hypothesis: allocation, not decode or merge CPU, was the dominant single-node cost.

## Verification
lib 349 passed (lib tests use the default allocator — jemalloc is only the server binary's global
allocator, so they are unaffected); fmt/diff-check clean; cargo check --bins clean. The full large
benchmark ran end-to-end (all categories incl. writes + concurrent throughput) with no new errors — the
29/157 concurrent-write 400s are the same pre-existing MVCC write-contention errors as the glibc
baseline (35/164), read-heavy 0. No adversarial review needed: a `#[global_allocator]` swap has no
algorithmic surface; the A/B benchmark + full-suite run + test gate are the verification.

## Note
A drop-in allocator swap is the highest-ROI change of the perf track. Remaining single-node levers are
deeper (fusing the WHERE filter into the parallel merge is blocked by the async-merge `tokio::spawn`
`'static` requirement vs the `&self` decode/eval path; projection-pushdown decode). See
[[build-test-bench-env]] and [[benchprod-campaign]].
