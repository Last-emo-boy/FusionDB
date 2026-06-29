# BENCHPROD-456 Execution Context — Parallel range-merge for full scans

The benchmark-moving follow-up to the 454 measurement. 454 established the unindexed full-scan floor
(~231 ms large-scale) is the SERIAL phase-1 materialize in `for_each_visible_range` (an N-way MVCC heap
merge over memtables + SSTables that clones every key+value into a Vec), not decode (440's parallel
decode didn't move it); 12 cores clear the >=4 gate.

## Implementation (commits 6cd6d4f [453] + 1be4cbd [456])
- 453 (refactor, no behavior change): extracted `merge_visible_range(mem_tables, sstables,
  write_buffer, read_ts, start, end, visit)` from `for_each_visible_range`. It takes borrowed snapshot
  pieces (not `&self`) so one consistent snapshot can be shared across sub-range merges.
- 456: `FusionTransaction::scan_range_parallel(start, end)` snapshots once (Arc memtables/sstables/
  write_buffer + single `read_ts`), calls `integer_pk_range_splits`, `tokio::spawn`s one
  `merge_visible_range` per disjoint sub-range collecting `Vec<(k,v)>`, awaits all, concatenates in
  order. Correctness: disjoint key sub-ranges + shared snapshot + one read_ts ⇒ byte-identical to the
  serial scan, no cross-boundary dedup needed.
- `integer_pk_range_splits`: K = min(available_parallelism, 8); `first()`/`last()` give the min/max
  keys; requires `prefix ++ 16 ASCII hex` (the `encode_i64_comparable` integer-PK encoding) with equal
  prefixes; interpolates K-1 boundary u64s **in u128** (span can approach u64::MAX); gates on
  `max_u-min_u >= 8192`. Returns None (⇒ serial) otherwise.
- Wiring: `Transaction::scan_prefix_parallel` (trait default = serial `scan_prefix`); FusionTransaction
  overrides (serial when `LIMIT` present, else parallel); `scan_routed_prefixes` calls it. So every
  HTTP/pgwire query whose phase-1 is an unbounded integer-PK full scan benefits; MemoryStorage and
  small/limited/non-integer-PK scans stay serial.

## Benchmark A/B (large, 12 cores, glibc malloc)
Full scan 230.9→148.6 ms (1.55×), BETWEEN 231.0→148.9 (1.55×), LIKE 202.6→147.0 (1.38×),
GROUP BY+HAVING 144.1→95.3 (1.51×), ORDER BY val DESC 394.3→291.7 (1.35×); Base-category avg
79.4→56.6 ms. CV also dropped (Full scan 29.9%→13.5%). COUNT(*)/SUM unchanged (streaming fast-paths,
not full-materialize). Below linear because glibc malloc arena locking contends on the per-row clones —
a faster allocator (mimalloc/jemalloc) would scale further but is unverifiable in this sandbox (see
[[build-test-bench-env]]: mimalloc dies under process backgrounding here).

## Verification
lib 349 (incl. parallel==serial equivalence over SSTable+active memtable+MVCC overwrites+tombstone, and
a full-i64-span overflow regression), fmt/diff-check clean, large benchmark A/B. A 3-dimension
adversarial review (correctness-coverage, split-derivation, concurrency-fallback; find+verify, ~430k
tokens) found ONE confirmed bug — `span * (i as u64)` overflowed u64 for wide id spans (debug panic /
release silent corruption) — fixed with u128 interpolation + the regression test; all other findings
refuted (uppercase-hex cosmetic is harmless; probe-latency refuted).
