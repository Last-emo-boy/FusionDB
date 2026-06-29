# Benchmark archive — large scale, HTTP, after BENCHPROD-462

Full-suite `BENCH_SCALE=large python3 benchmark.py` over HTTP (single node, `[distributed] enabled =
false`) on the release binary built at commit `9d67bf1` (BENCHPROD-462). This run is a **regression /
health check**: BENCHPROD-450/451/452/459/460/461/462 are all DISTRIBUTED-only changes (the fan-out
paths are inert in single-node mode), so no single-node delta is expected — numbers should sit within
run-to-run noise of the jemalloc (P9-7) baseline.

## Setup
- 228,488 rows loaded in 7,227 ms (31,615 rows/sec). 12 cores; jemalloc global allocator.

## Headline numbers (avg latency, ms)
| Category | This run | Notes |
|---|---|---|
| Base avg | 35.30 | jemalloc baseline ≈ 32.8 (prior runs 32.9 / 39.1) — within noise |
| Full scan (val=X) | 93.1 | scan floor (alloc-bound; parallel range-merge + jemalloc) |
| BETWEEN range | 103.3 | |
| LIKE prefix | 133.4 | |
| IN list | 148.9 | slowest Base query (unindexed full scan) |
| ORDER BY val DESC L50 | 105.8 | |
| COUNT(*) | 7.2 | |
| SUM(amount) | 8.4 | |
| GROUP BY category | 0.6 | (result-cache hit) |
| GROUP BY + HAVING | 44.8 | |
| Revenue by category (JOIN) | 78.4 | |
| Index speedup | 115.8× | scan 93.1ms → index 0.80ms |

## Category averages
E-commerce 1.42 · Financial 2.01 · Analytics 12.69 · Stress 12.10 · Inventory 15.28 · Risk 1.82 ·
ColumnScan 11.23.

## Concurrent throughput (16 threads, 1600 ops)
- Read-heavy (80:20): 1291 ops/s, errors 0
- Balanced (50:50): 1417 ops/s, errors 32
- Write-heavy (20:80): 1380 ops/s, errors 172

The 32 / 172 errors are HTTP 400s on the write-heavy mixes — **pre-existing MVCC write-conflict
(serialization-failure) behavior** under concurrent conflicting writes (read-heavy is 0 errors),
unrelated to the distributed grouped fan-out work.

## Verdict
No single-node regression — all categories within run-to-run noise of the jemalloc baseline, as expected
for distributed-only changes. (`benchmark_report_large_http.json` is gitignored; this is the archived
summary.)
