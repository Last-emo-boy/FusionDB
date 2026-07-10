---
title: "Architecture Constraints"
readMode: required
priority: high
category: arch
keywords:
  - architecture
  - module
  - layer
  - boundary
  - dependency
  - structure
---

# Architecture Constraints

## Module Structure

## Layer Boundaries

## Dependency Rules

## Technology Constraints

## Entries



<spec-entry category="arch" keywords="performance,planner,index,topk,explain" date="2026-07-08" title="Next performance backlog priorities" description="Prioritized roadmap for next FusionDB optimization turns" source="delegate:fdb-opt-backlog-v2">

### Next performance backlog priorities

Current P0/P1 roadmap from subagent analysis: extend covering/INCLUDE index support beyond primary key plus indexed column; feed ANALYZE statistics into cost and join planning with histograms/MCV/multi-column stats; add ordered composite index paths for ORDER BY LIMIT Top-K; use index key streams for DISTINCT, COUNT DISTINCT, and low-cardinality GROUP BY; then evaluate EXPLAIN ANALYZE actual rows/q-error observability.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,topk,frontier,zone-map,composite-covering" date="2026-07-09" title="Post Part 28 frontier optimization roadmap" description="Next optimization queue from literature-backed subagent research" source="working-tree">

### Post Part 28 frontier optimization roadmap

After Part 27/28, delegate frontier-literature-next-optimizations searched current RocksDB iterator/prefix-seek practices, PostgreSQL/MySQL/SQLite ORDER BY index behavior, ClickHouse skipping indexes, DuckDB zonemaps, Parquet page indexes, Top-K survey work, and learned-index papers. The next P0 is block-level SQL index-prefix / zone-map pruning: push the existing SSTable-level SQL index-prefix and range-local frontier idea down into block properties so Fusion can avoid opening SSTable iterators and reading blocks before .rseek is relevant. P1 candidates are direction-aware composite index metadata / descending key encoding, residual-predicate exact streaming Top-K with late materialization, and composite INCLUDE / covering Top-K. P2 candidates are partitioned index/filter metadata and richer reverse Top-K read options. P3 is learned/PGM/ALEX-style SSTable sidecars, fail-open to sparse index + rseek, only after block-level deterministic pruning is exhausted.

</spec-entry>

<spec-entry category="arch" keywords="benchmark,xlarge,performance,join,topk" date="2026-07-08" title="XLARGE benchmark baseline 2026-07-08" description="xlarge HTTP benchmark now completes with 0 errors; lists remaining hotspots" source="codex:xlarge-bench-2026-07-08">

### XLARGE benchmark baseline 2026-07-08

After enabling CROSS JOIN LIMIT pushdown, BENCH_SCALE=xlarge BENCH_PROTO=http completes on the local 12-core/31GiB machine with 2,285,324 rows loaded in 112,012 ms (20,403 rows/sec), 99 benchmarks, and 0 errors. CROSS JOIN (small) averages 12.759 ms instead of killing the server. Remaining top hotspots: BETWEEN range 2224.597 ms, full scan val equality 1591.497 ms, 3-table JOIN 1356.043 ms, LIKE prefix 1097.895 ms, IN list 1085.042 ms, revenue by category 1047.335 ms, ORDER BY val DESC LIMIT 50 990.706 ms, DISTINCT with WHERE 598.212 ms.

</spec-entry>

<spec-entry category="arch" keywords="benchmark,xlarge,join,limit,performance" date="2026-07-08" title="3-table JOIN LIMIT targeted benchmark 2026-07-08" description="Targeted xlarge 3-table JOIN LIMIT improves from 1356 ms to about 905 ms warm avg" source="codex:multi-join-limit-bench-2026-07-08">

### 3-table JOIN LIMIT targeted benchmark 2026-07-08

After extending simple multi-table INNER JOIN LIMIT eligibility and applying the row goal only on the final join step, the xlarge-loaded benchmark query SELECT u.name, o.id, oi.product_id FROM users u INNER JOIN orders o ON u.id = o.user_id INNER JOIN order_items oi ON o.id = oi.order_id LIMIT 100 returned 100 rows with warm avg 904.987 ms across 10 measured HTTP runs on the local dataset. The previous full xlarge report baseline for 3-table JOIN was 1356.043 ms avg, so this is roughly a 33% targeted improvement without relying on intermediate truncation.

</spec-entry>

<spec-entry category="arch" keywords="research,performance,cbo,topk,join,statistics" date="2026-07-08" title="Research-backed optimization backlog v3" description="Consolidated delegate research backlog for next FusionDB optimization turns" source="delegate:fdb-topk-cbo-join-research-v2">

### Research-backed optimization backlog v3

Delegate research completed for Top-K, CBO/statistics, and join row-goal. P0 tasks: version ANALYZE stats before adding MCV/histogram fields; build a shared selectivity estimator used by EXPLAIN, scan planning, and join reorder; add join cardinality using left_rows * right_rows / max(NDV); keep multi-join LIMIT row goals off intermediate materialized results; design costed indexed nested-loop/hash join candidates with row goal and index availability; add benchmark coverage for indexed ORDER BY LIMIT because the current ORDER BY val DESC hotspot uses the unindexed bench table; pure ordered secondary index scan is safe first for ASC NOT NULL single-column BTree, while DESC needs storage reverse scan or descending key encoding. Sources checked include PostgreSQL ORDER BY indexes/planner stats, MySQL ORDER BY optimization, SQLite query planner, and Leis et al. q-error/JOB paper.

</spec-entry>

<spec-entry category="arch" keywords="stats,cbo,analyze,mcv,histogram,hll,ndv,selectivity" date="2026-07-08" title="Stats V2 implementation backlog" description="Concrete TableStats V2 backlog from MCV/histogram and NDV/HLL research" source="delegate:fdb-stats-v2-mcv-hist-v1+fdb-stats-ndv-hll-v1">

### Stats V2 implementation backlog

Research for TableStats V2 and ANALYZE NDV/HLL completed. Next P0: add header-based version dispatch so V2 bytes cannot be half-decoded as V1; extend runtime stats with analyzed_rows/sampled, most_common_values, histogram, and NDV metadata while mapping V1 to empty MCV/histogram exact stats; replace duplicate selectivity formulas in EXPLAIN, scan planning, and join planning with a shared estimator; use MCV for equality/IN, equi-depth histograms for range/BETWEEN and prefix LIKE where ordering is valid, and left_rows * right_rows / max(NDV) for equality join cardinality; replace unbounded HashSet distinct collection with adaptive NDV collection that stays exact for low NDV and switches to stable-hash HLL for high NDV. Key risks: stable SQL-equality hashing for Value, Float/NaN ordering, stats sampling bias, and current full-table KV materialization in ANALYZE.

</spec-entry>

<spec-entry category="arch" keywords="stats,cbo,hll,ndv,mcv,histogram,estimator" date="2026-07-08" title="Stats estimator next implementation slice" description="Next CBO implementation slice after shared estimator" source="codex:stats-v2-estimator-2026-07-08">

### Stats estimator next implementation slice

After StatsEstimator centralization, the next CBO slice should implement adaptive NDV collection behind the existing distinct_count metadata: exact set for low NDV, stable SQL-value hashing, HyperLogLog for high NDV, and a clear Exact versus Estimated marker. Then feed MCV equality/IN selectivity and histogram range selectivity into StatsEstimator without changing scan/join/explain callers again.

</spec-entry>

<spec-entry category="arch" keywords="stats,cbo,mcv,histogram,join-cardinality,ndv" date="2026-07-08" title="MCV histogram and join cardinality next slices" description="Next CBO slices after adaptive NDV/HLL" source="delegate:fdb-mcv-hist-impl-plan-v1+fdb-join-card-est-impl-plan-v1">

### MCV histogram and join cardinality next slices

Subagent plans are complete after adaptive NDV/HLL. Next implementation candidates: fill existing MostCommonValue and HistogramBucket fields using bounded frequency maps and samples, then extend StatsEstimator with value-aware equality/IN/range/LIKE formulas; separately add NDV-based equality join cardinality rows = left_non_null * right_non_null / max(left_ndv, right_ndv) for join reorder and EXPLAIN. Keep row-goal boundaries unchanged.

</spec-entry>

<spec-entry category="arch" keywords="join,reorder,cbo,ndv,cardinality,performance" date="2026-07-08" title="Join reorder NDV cardinality follow-up" description="Next step: apply NDV join estimates to actual join reorder" source="codex:ndv-equality-join-explain-2026-07-08">

### Join reorder NDV cardinality follow-up

After adding NDV equality join estimates to EXPLAIN, the next performance slice should reuse the same StatsEstimator API inside scan/join.rs reorder_comma_join_from. Build directional equality join edges, estimate projected rows for each candidate connected to the placed set, and use projected rows as the main tie-breaker among connected candidates while preserving final-step LIMIT row-goal semantics.

</spec-entry>

<spec-entry category="arch" keywords="benchmark,join,reorder,ndv,analyze" date="2026-07-08" title="Stats-aware join reorder benchmark coverage" description="benchmark.py Part 10 measures NDV join reorder before and after ANALYZE." source="main@8a12c0f">

### Stats-aware join reorder benchmark coverage

benchmark.py now includes Part 10, a skewed 3-table stats-aware join reorder workload. Setup creates jr_hub, jr_high, and jr_low with controlled NDV: no-stats fallback should prefer the smaller low-NDV table first, while ANALYZE enables NDV projected-row join reorder to prefer the unique high-NDV table first. The small HTTP validation loaded 8,295 rows with 0 errors; NDV join reorder no stats averaged 3.376 ms, ANALYZE stats took 3.673 ms, and with stats averaged 2.070 ms. The report summary now only treats note-bearing Concurrent category rows as concurrent throughput.

</spec-entry>

<spec-entry category="arch" keywords="benchmark,matrix,parts,selection,report" date="2026-07-08" title="Benchmark part and matrix selection" description="benchmark.py can run targeted benchmark slices and records selection metadata." source="main@8a12c0f">

### Benchmark part and matrix selection

benchmark.py now supports BENCH_PARTS and BENCH_MATRIX while preserving default full-suite behavior. BENCH_PARTS accepts ids, ranges, keys, and tags such as 10, 1-2,join_ndv; BENCH_MATRIX currently maps presets like join_ndv, selectivity, topk, groupby, analyze, and planner to registered parts. Reports include protocol, seed, selection metadata, and per-benchmark part_id/part_key/part_title. Default full reports keep benchmark_report_<scale>_<proto>.json; non-full runs write a slugged report such as benchmark_report_small_http_parts_10.json. Verified with parser unit snippets, python3 -m py_compile benchmark.py, git diff --check, and BENCH_SCALE=small BENCH_PROTO=http BENCH_PARTS=10 python3 benchmark.py producing 3 join_ndv benchmarks with correct selection metadata.

</spec-entry>

<spec-entry category="arch" keywords="benchmark,wide_scan,predicate_first,compaction,warmup" date="2026-07-08" title="Wide-row predicate-first benchmark isolation" description="Part 11 wide-row scan benchmark is isolated and configurable; cold compaction still affects first measurements." source="main@8a12c0f">

### Wide-row predicate-first benchmark isolation

benchmark.py Part 11 now provides a non-default BENCH_MATRIX=wide_scan workload with configurable BENCH_WIDE_ROWS and BENCH_WIDE_PAYLOAD_BYTES. When wide_scan is the only selected part, setup only drops/creates/loads bench_wide to reduce unrelated compaction. The wide table has id, flag, bucket, measure, and four payload columns; Part 11 records 0.1%, 1%, 10%, and 50% selectivity cases plus fallback/full controls with metadata. Validation on 2026-07-08 small HTTP loaded 20,000 wide rows in 1,880 ms with 0 errors and wrote benchmark_report_small_http_matrix_wide_scan.json. Measured first-case latency is still contaminated by post-load cold storage/background compaction; after the service stabilizes, direct probes showed bucket=0, bucket<1, bucket<10, fallback OR, and full controls in roughly 27-35 ms. Treat cold and warm results separately before drawing predicate-first conclusions.

</spec-entry>

<spec-entry category="arch" keywords="benchmark,wide_scan,warmup,stabilization,compaction" date="2026-07-08" title="Wide-row benchmark first-pass warm stabilization" description="Part 11 separates first-pass and warm wide-row measurements with a stabilization probe loop." source="main@8a12c0f">

### Wide-row benchmark first-pass warm stabilization

benchmark.py Part 11 now reports wide-row scan cases in two phases: first-pass and warm. First-pass is a single measured execution before benchmark stabilization and query-specific warmup; warm results run after a stabilization probe loop and per-query warmup. The stabilization probe uses SELECT id FROM bench_wide WHERE bucket = -1, controlled by BENCH_WIDE_STABILIZE, BENCH_WIDE_STABILIZE_MAX_PROBES, BENCH_WIDE_STABILIZE_WINDOW, and BENCH_WIDE_STABILIZE_CV_PCT, and records probe count, recent average, recent CV, probe times, and stabilized status in warm metadata. Small HTTP validation on 2026-07-08 produced 7 first-pass and 7 warm rows with 0 errors; stabilization converged in 3 probes with recent_avg_ms 100.165 and recent_cv_pct 6.613. Warm wide scans were generally 50-70 ms while first-pass exposed post-load compaction/cold-storage spikes up to ~570 ms.

</spec-entry>

<spec-entry category="arch" keywords="benchmark,metrics,wide_scan,block_cache,row_cache,slow_query" date="2026-07-08" title="Benchmark per-case metrics delta" description="Benchmark reports now include per-case /metrics counter deltas." source="main@8a12c0f">

### Benchmark per-case metrics delta

benchmark.py now captures FusionDB /metrics snapshots around each measurement window and writes metrics_delta into report rows. Warmup executions are excluded from the delta; first-pass rows capture one measured query, and warm rows capture only measured iterations after stabilization/query warmup. The tracked counters include query_count, query_total_us, slow_query_count, row_read_count, row_cache_hit_count, row_write_count, block_cache hit/miss/insert/eviction counts and bytes, and WAL counters. Smoke validation with BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=wide_scan BENCH_WIDE_ROWS=2000 produced 14 rows with metrics_delta; examples included first-pass bucket=0 query_count=1, slow_query_count=1, block_cache_hit_count=7406, block_cache_miss_count=812, and warm fallback OR query_count=5 with cache deltas. This makes wide-row scan interpretation evidence-based rather than latency-only.

</spec-entry>

<spec-entry category="arch" keywords="benchmark,sstable,range,upper-bound,metrics" date="2026-07-08" title="SSTable range-bound benchmark matrix" description="Repeatable benchmark matrix for SSTable range upper-bound impact" source="main@8a12c0f">

### SSTable range-bound benchmark matrix

benchmark.py Part 15 / BENCH_MATRIX=sstable_range_bound creates bench_sst_bound with configurable BENCH_BOUND_ROWS, BENCH_SST_BOUND_PAYLOAD_BYTES, and BENCH_SST_BOUND_SNAPSHOT_ROUNDS. It checkpoints after load and updates to create SSTables, then measures first-pass and warm PK ranges plus full-scan control. Interpret block_read_requests_per_query and cold_block_loads_per_query as primary evidence; latency alone is secondary. Small smoke on 2026-07-08 after PK AND range planning showed 1-row range 12 block requests/query, empty range 8, 100-row range 40, full scan control 120.

</spec-entry>

<spec-entry category="arch" keywords="sstable,prefix-bloom,mvcc,filter,scan" date="2026-07-08" title="Prefix Bloom implementation constraints" description="Safe prefix Bloom constraints from research subagent" source="main@8a12c0f">

### Prefix Bloom implementation constraints

Prefix Bloom for FusionDB SSTable scans must be a versioned optional filter built from decoded MVCC user keys, not raw internal keys. Use only negative checks to skip an SSTable when the query range is proven prefix-safe, such as scan_prefix(prefix) or [prefix, prefix_end(prefix)). Old SSTables, extractor mismatch, arbitrary ranges, or decode failures must fail open. Table/shard prefixes are the safest first extractor; finer index-value or PK bucket prefixes should wait for dedicated benchmarks and compatibility metadata.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,performance,sstable,planner,subagent" date="2026-07-08" title="Next optimization roadmap after block-prefix benchmark" description="Prioritized next optimization directions from storage and SQL research delegates" source="main@8a12c0f">

### Next optimization roadmap after block-prefix benchmark

Two read-only Delegate Subagents completed on 2026-07-09. Storage ranking: first add MVCC user-key Bloom/filter for point reads so FusionTransaction::get can skip SSTables by decoded user key; second add read-block policy/no-fill cache and reduce block miss syscall cost; third extend block properties toward MVCC-safe SQL zone maps; later adaptive Bloom sizing, level/overlap-aware compaction, and reverse SSTable iterator. SQL ranking: first implement true DESC/composite ordered index Top-K because current secondary/composite DESC paths collect then reverse; second add SSTable SQL zone-map pruning; third use index key streams for DISTINCT/COUNT DISTINCT/low-cardinality GROUP BY; later MCV/histogram stats, costed join algorithm selection, late-materialized Top-K, and broader vectorized execution. Sources cited by delegates include RocksDB prefix/Bloom/block cache/compaction docs, Pebble block properties, ClickHouse skipping indexes, Parquet page index, DuckDB zonemaps/vector format, PostgreSQL B-tree ORDER BY/LIMIT and planner stats, MySQL loose index scan, CockroachDB vectorized/spilling docs, MonetDB/X100, Vertica materialization, and Monkey Bloom allocation.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,storage,no-fill,topk,index,syscall" date="2026-07-08" title="Post no-fill storage and Top-K follow-up" description="Next roadmap after no-fill cache policy and ordered-index delegate review" source="main@8a12c0f">

### Post no-fill storage and Top-K follow-up

Delegate research on 2026-07-09 confirmed two next high-value directions. Storage P1: reduce SSTable block miss syscall cost after no-fill by reusing an iterator file handle or read-at/pread style helper; current miss path still opens, seeks, and reads for every cold block. SQL P0/P1: implement true ordered index Top-K rather than collecting then reversing for DESC; pure ORDER BY indexed_col LIMIT currently needs a planner path, ASC can be the first safe slice, DESC requires reverse storage scan or descending key encoding, and composite/mixed directions require direction-aware index metadata.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,sstable,io,pread,readahead,file-handle" date="2026-07-08" title="SSTable IO follow-up after local file reuse" description="Next IO roadmap after SSTable file-handle reuse" source="main@8a12c0f">

### SSTable IO follow-up after local file reuse

After local iterator/find_ge file-handle reuse, next storage IO slices should evaluate positioned read/read_at or pread semantics to avoid seek cursor mutation, then consider bounded readahead only for proven large scans. Keep per-iterator/local handles rather than a shared seek+read handle across tasks; any global SSTable file cache must account for obsolete file deletion and fd limits. Continue using counters sstable_block_file_open_count, sstable_block_read_bytes, block_cache_miss_count, and block_cache_fill_skip_count as primary benchmark evidence.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,topk,index,desc,composite,explain" date="2026-07-08" title="Indexed Top-K follow-up after ASC slice" description="Follow-up roadmap after single-column ASC indexed Top-K" source="codex:index-topk-asc-2026-07-09">

### Indexed Top-K follow-up after ASC slice

The implemented ASC slice intentionally excludes nullable columns, DESC, composite indexes, strings/decimals/floats, expression/alias ORDER BY, and sharded scans. Next work should prioritize true reverse index/SSTable iteration or descending key encoding for DESC Top-K, then composite direction-aware ordered scans, EXPLAIN visibility for ORDER BY-driven access paths, and broader type support only after proving index key ordering matches SQL Value::compare and NULL semantics.

</spec-entry>

<spec-entry category="arch" keywords="topk,desc,reverse,types,roadmap" date="2026-07-09" title="Indexed Top-K follow-up queue after EXPLAIN" description="Next indexed Top-K performance tasks after EXPLAIN visibility" source="main@8a12c0f">

### Indexed Top-K follow-up queue after EXPLAIN

Subagent results on 2026-07-09: DESC Top-K should wait for a real bounded reverse range scan API across Memory/Fusion/SSTable rather than planner-only reverse(); implement storage scan_range_reverse before opening DESC gate. Safe type expansion can be a later small patch for BOOLEAN/BOOL plus date/time aliases such as DATE32, TIMESTAMPTZ, and INTERVAL prefix, but TEXT/STRING, DECIMAL/NUMERIC, and FLOAT/DOUBLE require order-preserving key encoding and explicit SQL ordering semantics. EXPLAIN observability is complete for current ASC slice; next benchmark-visible performance slice should be either reverse range scan infrastructure or safe ordered type expansion with tests.

</spec-entry>

<spec-entry category="arch" keywords="topk,desc,reverse,storage,mvcc,sstable" date="2026-07-09" title="DESC Top-K requires storage reverse range scan" description="Concrete implementation queue for DESC indexed Top-K" source="main@8a12c0f">

### DESC Top-K requires storage reverse range scan

Delegate analysis on 2026-07-09 confirmed that opening DESC secondary BTree ORDER BY/LIMIT requires a storage-level scan_range_reverse(start,end,limit) over visible user keys. Planner-only reverse is insufficient because the current DESC branch scans the full index range then reverses/truncates, and repeated last() is MVCC/tombstone unsafe. Recommended sequence: add Transaction::scan_range_reverse contract; implement Memory reverse merge with tombstones; implement SSTable bounded reverse iterator over user-key bounds; implement Fusion reverse visible merge that groups internal versions by user key and applies limit after MVCC collapse; then open DESC SQL/EXPLAIN gate and add SQL covering, fallback, and Part 20 benchmark cases.

</spec-entry>

<spec-entry category="arch" keywords="fusion,reverse,mvcc,topk,desc" date="2026-07-09" title="Fusion reverse visible merge remains DESC Top-K gate" description="Remaining gate after SSTable reverse iterator" source="main@8a12c0f">

### Fusion reverse visible merge remains DESC Top-K gate

After Memory reverse scan and SSTable user-key bounded reverse iterator, SQL DESC Top-K remains gated on FusionTransaction overriding scan_range_reverse with a true reverse visible MVCC merge. The SSTable reverse iterator returns raw internal entries and for the same user key returns older internal versions before newer ones in reverse order, so Fusion must drain all versions for each user key across write buffer, memtables, immutable memtables, and SSTables; choose the newest version with ts <= read_ts; treat tombstone winner as deleted; and consume limit only after emitting visible PUT rows. Do not open planner/EXPLAIN DESC gate until that Fusion layer is implemented and benchmarked.

</spec-entry>

<spec-entry category="arch" keywords="topk,desc,reverse,planner,benchmark" date="2026-07-09" title="DESC Top-K gate after Fusion reverse merge" description="Next gate after storage reverse merge" source="main@8a12c0f">

### DESC Top-K gate after Fusion reverse merge

Storage reverse infrastructure is now present across Memory, SSTable, and Fusion: Memory has direct reverse merge, SSTable has user-key bounded raw reverse iterator, and Fusion has visible MVCC scan_range_reverse. The next gate for SQL DESC Top-K is planner/index integration plus benchmark evidence: wire secondary BTree DESC ORDER BY/LIMIT to scan_range_reverse, update EXPLAIN to distinguish bounded DESC index scan, add SQL correctness tests for covering/non-covering/tombstone/null fallback cases, and run Part 20 or an equivalent benchmark to prove DESC no longer materializes the full index range before enabling the production fast path.

</spec-entry>

<spec-entry category="arch" keywords="benchmark,xlarge,index,distinct,groupby,cache,performance" date="2026-07-09" title="Indexed DISTINCT xlarge debug bench 2026-07-09" description="xlarge index_distinct debug bench plus cache-busting correction for GROUP BY COUNT" source="main@index-distinct-xlarge-debug-2026-07-09">

### Indexed DISTINCT xlarge debug bench 2026-07-09

Ran BENCH_SCALE=xlarge BENCH_MATRIX=index_distinct against a temporary debug FusionDB server on port 18091 using an empty /tmp data dir. The machine sustained the matrix without memory pressure: 1,500,000 rows loaded, peak observed RSS about 2.17 GB, data dir about 293 MB before shutdown. Debug load was slow: 2,023,745 ms total, 741 rows/sec; index builds took 20,481 ms. Reported Part 21 averages: DISTINCT full scan 1784.8 ms, DISTINCT index key stream 687.2 ms, COUNT DISTINCT full scan 1187.3 ms, COUNT DISTINCT index key stream 691.5 ms, nullable COUNT DISTINCT key stream 735.6 ms. GROUP BY COUNT reported 1.3-1.5 ms in the standard report but that is cache-contaminated by the grouped aggregate query-result cache. Manual cache-busting comments measured more realistic uncached averages: GROUP BY full scan 3013.6 ms, GROUP BY index key stream 1538.1 ms, GROUP BY nullable fallback 2994.9 ms, DISTINCT full 3260.1 ms, DISTINCT index 1139.5 ms, COUNT DISTINCT full 2935.5 ms, COUNT DISTINCT index 920.0 ms. Conclusion: current tight index key-stream roughly halves uncached GROUP BY COUNT and improves DISTINCT/COUNT DISTINCT by about 2.9x-3.2x on this debug xlarge run, but future commercial claims need release builds, cache-busting or cache-disabled methodology, RSS/allocator metrics, and index-entry visited counters.

</spec-entry>

<spec-entry category="arch" keywords="groupby,index,summary,mvcc,roadmap" date="2026-07-09" title="GROUP BY COUNT exact summary index roadmap" description="Research-backed roadmap for exact O(NDV) GROUP BY COUNT summary indexes" source="main@8a12c0f">

### GROUP BY COUNT exact summary index roadmap

Delegate research on 2026-07-09 found that storage-layer raw counts cannot safely answer exact GROUP BY COUNT over secondary-index value runs in FusionDB because SSTable entry_count counts internal versions, while correct SQL results require MVCC visible merge, tombstone suppression, and write-buffer/SSTable shadowing. MySQL/PostgreSQL/SQLite loose or skip scans help DISTINCT/MIN/MAX-style enumeration but do not provide per-group COUNT(*) without run-count capability. The recommended P0 for true O(NDV) GROUP BY COUNT is an exact maintained summary table or aggregate index keyed like index_count:<table>:<column>:<value_key>, updated transactionally on CREATE INDEX backfill, INSERT, DELETE, and UPDATE old/new value changes, with zero counts tombstoned and query fallback to the current key-stream path for missing/legacy/malformed summaries.

</spec-entry>

<spec-entry category="arch" keywords="groupby,count,summary,index" date="2026-07-09" title="GROUP BY COUNT maintained summary index" description="Exact maintained GROUP BY COUNT summary index lifecycle" source="main@8a12c0f">

### GROUP BY COUNT maintained summary index

GROUP BY col, COUNT(*) over a single-column non-primary NOT NULL BTree index uses a maintained summary keyspace: index_count:<table>:<column>:<value_key> stores an 8-byte little-endian i64 count, and index_count_meta:<table>:<column> stores v1:<total_entries>:<group_count>. CREATE INDEX backfills counts and marker in the same transaction. INSERT/UPDATE/DELETE adjust the same count key and meta in the row transaction when the encoded value_key changes. DROP INDEX and DROP TABLE delete summary keyspaces; TRUNCATE clears entries but writes empty v1:0:0 metadata so future DML keeps maintaining the summary. Query and EXPLAIN use summary before the old tight key-stream; missing or malformed summary returns Ok(None) and falls back.

</spec-entry>

<spec-entry category="arch" keywords="startup,recovery,manifest,lsm,wal" date="2026-07-09" title="Startup recovery manifest roadmap" description="Research-backed startup recovery roadmap" source="main@8a12c0f">

### Startup recovery manifest roadmap

Delegate research and code analysis on 2026-07-09 agree that commercial LSM engines avoid startup O(data) recovery with MANIFEST/CURRENT, per-SSTable properties, sequence/high-watermark checkpoints, and bounded WAL replay. FusionDB P0 after timestamp cache: add startup phase metrics, persist durable next_ts/high-watermark outside SST data, and add SSTable format vNext max_ts/min_ts properties. P1: implement MANIFEST/CURRENT as source of truth for live SSTables and WALs, support manifest rollover/snapshot, and lazy/deferred SSTable open so server can listen before loading all index/filter metadata. P2: repair mode scans directory/SST data only when manifest is missing or corrupt; add crash-window tests for SST fsync, manifest edit, WAL tail, missing WAL, orphan SST, and legacy metadata fallback.

</spec-entry>

<spec-entry category="arch" keywords="startup,sstable,lazy,metadata,rocksdb" date="2026-07-09" title="SSTable lazy metadata roadmap" description="Research-backed lazy SSTable metadata roadmap" source="main@8a12c0f">

### SSTable lazy metadata roadmap

Delegate research on 2026-07-09 found RocksDB/LevelDB/Pebble split table open into footer/descriptors, reader cache, block cache, and lazy or partitioned index/filter loading. FusionDB P0 after parallel open: add per-phase SsTable::open metrics for footer/index/filter/meta decode bytes and time. P1: introduce SSTable format vNext with a small fixed descriptor/properties block containing first/last key, max_ts, entry_count, and block handles, so startup need not deserialize all index/filter/block_properties. P2: implement metadata block cache, partitioned index/filter, table reader/file handle LRU, and MANIFEST-driven lazy table open; normal startup should load descriptors only, with repair mode handling directory/SST scans.

</spec-entry>

<spec-entry category="arch" keywords="startup,sstable,lazy,metadata,priority" date="2026-07-09" title="SSTable open next optimization priority" description="Measured priority for lazy block properties and index format" source="main@8a12c0f">

### SSTable open next optimization priority

Measured SSTable open phase metrics on 2026-07-09 show meta/block_properties decode dominates remaining warm startup: meta decode 5.23s cumulative versus index decode 2.92s, with only about 0.18s combined read time. Next optimization priority should be a vNext/lazy metadata path: store a small descriptor/properties block with first/last key, max_ts, entry_count and block handles; load block_properties/table_prefixes lazily or via metadata block cache; then rework index into sorted-vector or partitioned index format to avoid bincode BTreeMap decode and duplicate key vectors. File I/O tuning is lower priority for this workload.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,sstable,index,startup,partitioned-index,manifest" date="2026-07-09" title="SSTable index optimization next roadmap" description="Next steps after direct vector index decode" source="main@8a12c0f">

### SSTable index optimization next roadmap

Delegate research on 2026-07-09 converged on the next startup path after direct vector decode: first add a benchmark matrix for startup index loading with http_ready_ms, sstable_open_index_decode_us, first-query penalty, RSS, and compaction noise gates; then implement a versioned custom/arena-backed index format or binary index sidecar cache; then move to RocksDB-style partitioned/two-level index and metadata block cache; finally put live SSTable descriptors into MANIFEST/CURRENT so normal startup can open descriptors before full table readers. Avoid simple lazy full-index decode unless first-query latency is explicitly budgeted.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,sstable,index,partitioned,manifest,benchmark" date="2026-07-09" title="SSTable index cache next roadmap after FICX v3" description="Roadmap from delegates and RocksDB/LSM best-practice review" source="main@8a12c0f">

### SSTable index cache next roadmap after FICX v3

Delegate research and web review on 2026-07-09 converge on the next storage roadmap: first add a dedicated sstable_startup_index benchmark matrix measuring http_ready_ms, RSS, first point/range query penalty, canonical index vs sidecar read/decode, and sidecar hit/miss/stale/invalid/write counters; then add MANIFEST/CURRENT as the transactional source of truth for live SSTable descriptors; then implement two-level/partitioned SSTable indexes with a pinned top-level index and metadata block cache; then use the same infrastructure for partitioned filters and later range filters. RocksDB partitioned index/filter docs support top-level-only residency plus on-demand index/filter partitions to reduce cache pollution and IO.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,startup,manifest,partitioned-index,benchmark" date="2026-07-09" title="Startup index benchmark gates before MANIFEST" description="Benchmark gates before MANIFEST and partitioned index work" source="main@8a12c0f">

### Startup index benchmark gates before MANIFEST

Delegate reviews cdx-startup-index-bench-gate and cdx-manifest-index-gates on 2026-07-09 recommended blocking MANIFEST/CURRENT and two-level partitioned index work on a dedicated startup/index benchmark gate. Required evidence: sidecar hit/miss/stale/invalid/write counters by scenario, http_ready_ms, RSS at readiness and after first query, first point/range query latency, live_sstable_count, sstable_open_meta_decode_us, compaction_run_count/input/output, and report metadata for source data dir, scenario workdir, copied-vs-direct data, data_dir bytes, and first-query SQL. Do not mutate SST mtime to create stale sidecars because that also invalidates timestamp/descriptor caches; mutate only *.idxcache version/fingerprint/payload.

</spec-entry>

<spec-entry category="arch" keywords="manifest,current,sstable,startup,lsm" date="2026-07-09" title="Live SSTable MANIFEST/CURRENT skeleton" description="Snapshot-style live SSTable manifest and strict startup semantics" source="main@8a12c0f">

### Live SSTable MANIFEST/CURRENT skeleton

FusionDB now persists a snapshot-style SSTable MANIFEST-000001 plus CURRENT under the SSTable directory. Normal startup uses CURRENT/MANIFEST as the source of truth; legacy startup scans numeric *.sst only when CURRENT is absent. If CURRENT exists but manifest validation fails or a referenced SSTable cannot open, startup returns FusionError instead of silently scanning the directory. Future work should evolve this snapshot into an append-only edit log with rollover, WAL tracking, next_file_number, and timestamp high-watermark.

</spec-entry>

<spec-entry category="arch" keywords="manifest,editlog,wal,rollover,roadmap" date="2026-07-09" title="Manifest edit-log next stage" description="Next architecture stage after manifest metrics and orphan gate" source="main@8a12c0f">

### Manifest edit-log next stage

Delegate research on 2026-07-09 recommends evolving the snapshot MANIFEST/CURRENT skeleton into a v2 append-only manifest with length+CRC records, Snapshot/AddSstable/Compact/SetHighWatermark edits, rollover to MANIFEST-<seq>, strict CURRENT semantics, WAL replay floor tracking, and metrics for replay bytes/records/us plus append sync latency. The immediate prerequisite now satisfied is manifest startup observability and orphan benchmark gating.

</spec-entry>

<spec-entry category="arch" keywords="manifest,wal,floor,recovery,editlog" date="2026-07-09" title="Manifest v2 WAL floor prerequisite" description="Constraint for future manifest v2 WAL floor implementation" source="main@8a12c0f">

### Manifest v2 WAL floor prerequisite

Before implementing manifest v2 WAL floor tracking, do not treat the current WalManager::truncate behavior as a WAL floor implementation because it removes all WAL segments. The v2 design should track a durable replay floor/high-watermark in append-only manifest edits with length+CRC records and only delete WAL data that is provably below the persisted floor after manifest sync and CURRENT semantics are satisfied. Replay metrics added in this stage provide the observability needed to gate bounded WAL recovery.

</spec-entry>

<spec-entry category="arch" keywords="wal,floor,manifest,delete,crash" date="2026-07-09" title="WAL floor deletion ordering" description="Safe WAL floor publication and physical deletion ordering" source="main@8a12c0f">

### WAL floor deletion ordering

Research agent guidance from LevelDB/RocksDB/Pebble: WAL floor should be a durable log-number/cursor tracked by manifest edits, not current WalManager::truncate. Correct order is write and sync SST, append and sync manifest edit containing new SST and wal_floor/min_unflushed_log, durably install CURRENT if needed, publish the in-memory version, then asynchronously delete WAL files below the durable floor and not pinned by checkpoint/backup/replication. Crash after manifest before delete is safe; delete before manifest is unsafe. Future gates should check replay bytes equal WAL >= durable floor and WAL retained bytes stay bounded under periodic flush.

</spec-entry>

<spec-entry category="arch" keywords="manifest,record,current,crc32c,editlog" date="2026-07-09" title="Manifest v2 record framing and CURRENT install" description="Manifest v2 append-only record format and atomic CURRENT ordering" source="main@8a12c0f">

### Manifest v2 record framing and CURRENT install

Manifest v2 should move from snapshot JSON to an append-only VersionEdit log. Use a LevelDB/Pebble legacy-style physical record layer: 32 KiB blocks, 7-byte headers crc32c + len + type, FULL/FIRST/MIDDLE/LAST fragmentation, CRC32C Castagnoli over type||payload, and prefix recovery only for torn tail records. Edit schema should include snapshot, AddSstable/DeleteSstable/Compact, SetNextFileNumber, SetHighWatermark, and SetWalReplayFloor. CURRENT install order: write+sync MANIFEST-N, fsync manifest directory, write+sync same-dir CURRENT tmp, rename to CURRENT, fsync directory. Normal startup may use legacy scan only when CURRENT is absent/new DB; corrupt CURRENT/MANIFEST or missing referenced SST must fail fast outside explicit repair mode.

</spec-entry>

<spec-entry category="arch" keywords="manifest,high-watermark,prefix,crash" date="2026-07-09" title="Manifest edit prefix legality follow-up" description="P1 follow-up for per-edit prefix legality" source="main@8a12c0f">

### Manifest edit prefix legality follow-up

Next manifest v2 semantic hardening: every durable edit prefix must replay into a legal state. Current AddSstable/Compact can add an SSTable whose max_ts exceeds the current high_watermark, relying on a later SetHighWatermark; a crash between those edits leaves a complete but invalid manifest prefix. Decide whether SetHighWatermark must be written before Add/Compact, whether Add/Compact should carry or advance the high watermark atomically, or whether a batch edit is needed. Add a test that every complete prefix of a manifest replay is valid before integrating v2 into FusionStorage.

</spec-entry>

<spec-entry category="arch" keywords="manifest,rocksdb,pebble,versionedit,current" date="2026-07-09" title="Manifest v2 research direction RocksDB Pebble" description="Research-backed manifest v2 direction" source="main@8a12c0f">

### Manifest v2 research direction RocksDB Pebble

External research on LevelDB/RocksDB/Pebble manifest practice recommends FusionDB v2 follow RocksDB/Pebble semantics: MANIFEST is the single trusted VersionEdit log, CURRENT/marker is the single trusted entry point, normal startup must not guess the highest MANIFEST, and each durable edit prefix must replay into a legal version. Flush/compaction edits should atomically include live file delta plus next_file_number, high_watermark, and wal_replay_floor, rather than splitting AddSstable and SetHighWatermark into crash-sensitive separate records. On recovered torn tail, preferred production behavior is to roll over to a new snapshot MANIFEST and install CURRENT; truncate(valid_bytes)+sync before append is an acceptable repair for the current independent helper, but integration should favor rollover.

</spec-entry>

<spec-entry category="arch" keywords="manifest,versionedit,fusionstorage,append,startup" date="2026-07-09" title="Manifest v2 append-only FusionStorage next boundary" description="Next boundary after v2 Snapshot integration is append-only VersionEdit persistence" source="main@8a12c0f">

### Manifest v2 append-only FusionStorage next boundary

After v2 snapshot integration, the next FusionStorage manifest step should move flush/compaction persistence from full Snapshot rewrite to append-only ManifestEdit::VersionEdit records. Each durable record should atomically carry live file delta, next_file_number, high_watermark, and eventually wal_replay_floor; append and sync must happen before publishing the in-memory live version. Keep legacy JSON fallback read-only and keep rollover limited to size/torn-tail/snapshot cases.

</spec-entry>

<spec-entry category="arch" keywords="wal,floor,manifest,recovery,crash" date="2026-07-09" title="WAL floor metadata before deletion" description="Implement WAL floor metadata and tests before any physical WAL deletion" source="main@8a12c0f">

### WAL floor metadata before deletion

Research subagent found the smallest safe WAL-floor step is non-deletion metadata wiring: return durable WAL end cursors from append, bind cursors to memtables, publish a manifest wal_replay_floor only after SST and manifest VersionEdit are durable, and initially keep startup replaying all WAL while exposing/verifying the floor. Do not treat current WalManager::truncate as a durable floor because it removes all segments and can race with active memtable commits.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,delegate,performance,cbo,sstable,topk,benchmark" date="2026-07-09" title="Next optimization backlog from 2026-07-09 delegates" description="Consolidated next backlog from parallel research delegates" source="main@8a12c0f">

### Next optimization backlog from 2026-07-09 delegates

Four read-only delegate agents completed on 2026-07-09. Join/CBO: next step is a unified join cost API shared by execution and EXPLAIN, then costed index nested-loop vs hash join while preserving final-step LIMIT row-goal. SSTable read path: P0 is point-get SSTable user-key min/max overlap skip before Bloom/find_ge, then explicit no-fill options for SQL large scans. Top-K/index: single-column ASC/DESC is already strong; P0 is composite DESC bounded scan via scan_range_reverse plus composite ORDER BY/LIMIT EXPLAIN and benchmark gate. Benchmark methodology: add claim-mode gates, cache-phase discipline, query-result cache counters, sample-quality checks for p99, and system counters for commercial performance claims.

</spec-entry>

<spec-entry category="arch" keywords="composite-index,topk,explain,benchmark,roadmap" date="2026-07-09" title="Composite Top-K follow-up after bounded DESC" description="Next gates after composite DESC execution path" source="main@8a12c0f">

### Composite Top-K follow-up after bounded DESC

After wiring composite DESC execution to bounded scan_range_reverse, the next observability slice should add EXPLAIN visibility and a benchmark gate. EXPLAIN should report ordered composite BTree ORDER BY/LIMIT only through the same capability/predicate coverage gate as execution. benchmark.py Part 20 or a new matrix should cover (host_id, ts) ASC/DESC, range DESC, residual fallback, and nullable/mixed-direction fallback with index-entry or storage range-scan counters. Direction-aware composite index metadata remains future work for mixed ASC/DESC key parts.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,topk,streaming,metrics,composite-index" date="2026-07-09" title="Next slice after composite Top-K benchmark" description="Roadmap from benchmark and delegates" source="main@8a12c0f">

### Next slice after composite Top-K benchmark

Delegate research and smoke benchmark results point to streaming bounded range/reverse visitor APIs plus ordered Top-K counters as the next high-value slice. Current composite DESC Top-K reports bounded ordered composite BTree in EXPLAIN and reads near LIMIT rows in smoke metadata, but latency is still far above ASC. Add Transaction scan_range_for_each / scan_range_reverse_for_each with early stop, plus metrics such as index_ordered_topk_entry_visit_count, index_ordered_topk_scan_count, index_ordered_topk_reverse_scan_count, and query_sort_fallback_count. Composite covering INCLUDE and nullable/mixed-direction fallback coverage remain follow-up work.

</spec-entry>

<spec-entry category="arch" keywords="topk,streaming,reverse,fusion,performance" date="2026-07-09" title="Next Top-K performance slice after metrics" description="Next performance task after ordered Top-K observability" source="delegate:cdx-105121-b1ff,delegate:cdx-105121-28e4">

### Next Top-K performance slice after metrics

The next non-blocking performance slice is to replace eager materialization in Fusion reverse ordered range scans with streaming bounded range/reverse visitor APIs. Delegate analysis found that composite DESC Top-K is correctly planned as bounded reverse and skips query sort, but latency remains much slower than ASC because Fusion reverse sources can still collect large reverse ranges before emitting LIMIT candidates, especially through memtable merge paths. Recommended sequence: add Transaction scan_range_for_each and scan_range_reverse_for_each with early stop, implement Memory/Fusion overrides before the default materializing fallback, then benchmark composite DESC/range/window Top-K again using the new ordered Top-K counters as the gate.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,topk,benchmark,visitor,metrics" date="2026-07-09" title="Next Top-K gates after lazy reverse" description="Next implementation gates after lazy reverse source" source="delegate:cdx-110950-78bc,delegate:cdx-110950-45de">

### Next Top-K gates after lazy reverse

After lazy Fusion memtable reverse sources, the next planned slices are: add Transaction scan_range_for_each and scan_range_reverse_for_each with Memory/Fusion streaming overrides; connect secondary and composite ordered Top-K execution to those visitors to remove bounded index-entry Vec materialization; add storage/source-level reverse counters such as raw internal entries decoded/yielded and reverse blocks read; and add BENCH_CLAIM_MODE gates for Part 20 that assert ordered scans, reverse scans, zero query-sort fallback, bounded visits, query-result cache misses, EXPLAIN access path, and latency ratios. Delegate sources referenced RocksDB/Pebble/WiredTiger iterator/cursor bounds and PostgreSQL/MySQL/SQLite ORDER BY LIMIT guidance.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,topk,metrics,benchmark,fusion" date="2026-07-09" title="Next gates after range visitor Top-K" description="Next proof and optimization gates after range visitor integration" source="main@2026-07-09">

### Next gates after range visitor Top-K

After connecting ordered Top-K to Transaction range visitors, the remaining performance proof gap is below the SQL visible-entry layer. Next slices: add storage/raw counters for Fusion reverse scans, including raw internal entries decoded, visible candidates yielded, reverse blocks read, and overlapping SSTable iterator opens; add BENCH_CLAIM_MODE gates for Part 20 using ordered Top-K counters, query-result cache counters, EXPLAIN metadata, and latency ratios; then attack Fusion reverse startup cost over many overlapping SSTables. External references checked this round: PostgreSQL B-tree ORDER BY/LIMIT and backward scans, RocksDB iterator bounds/snapshot/resource pinning, Pebble IterOptions bounds, and WiredTiger cursor bounds before visibility checks.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,topk,benchmark,claim-mode,metrics" date="2026-07-09" title="Next Top-K claim-mode gate after raw counters" description="Next gate after raw reverse metrics" source="main@8a12c0f">

### Next Top-K claim-mode gate after raw counters

After raw reverse counters, the next non-blocking slice is BENCH_CLAIM_MODE for Part 20. Positive ordered Top-K paths should assert ordered scan count per query equals 1, entry visits <= LIMIT, reverse scan count equals 1 for DESC and 0 for ASC, query sort fallback equals 0, and DESC raw storage work remains bounded using fusion_reverse_visible_puts <= LIMIT plus fusion_reverse_raw_entry_reads <= max(3*LIMIT,64). For SSTable-heavy runs, also assert sstable_reverse_iterator_opens and reverse block decodes/yields stay bounded relative to LIMIT and live SSTable count. Fallback paths should assert ordered scans are zero and sort fallback is present for expression or mixed-order cases.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,benchmark,claim-mode,cache,sstable,observability" date="2026-07-09" title="Next benchmark claim hardening after Part 20 gates" description="Next claim-mode hardening work" source="main@8a12c0f">

### Next benchmark claim hardening after Part 20 gates

After Part 20 claim-mode gates, next benchmark hardening should add query-result cache hit/miss counters or explicit cache-busting evidence for all claim queries, richer report environment disclosure including commit/build profile/CPU/RAM/disk/data-dir/cache flags, and a SSTable-heavy Top-K claim phase so sstable_reverse_iterator_open_count and reverse block decode/yield thresholds can be exercised in benchmark.py rather than only storage unit tests. Keep latency thresholds as warnings until sample counts and open/concurrent workload methodology are stronger.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,benchmark,disclosure,sstable,cache,claim-mode" date="2026-07-09" title="Next benchmark disclosure and SSTable-heavy claim phase" description="Next commercial-grade benchmark hardening" source="main@8a12c0f">

### Next benchmark disclosure and SSTable-heavy claim phase

After query-result cache counters, benchmark claim hardening should add report environment disclosure fields (git commit/dirty status, rust build profile, CPU/RAM, data-dir path/size, cache settings, selected env vars) and a SSTable-heavy Top-K claim phase that checkpoints or restarts after Part 20 load so sstable_reverse_iterator_open_count plus reverse block decode/yield thresholds are exercised by benchmark.py, not only storage unit tests. External references checked: ClickHouse Query Cache exposes QueryCacheHits/Misses, MySQL removed query cache in 8.0, PostgreSQL uses materialized views rather than implicit result cache, and TPC fair use emphasizes enough disclosure for fair comparison.

</spec-entry>

<spec-entry category="arch" keywords="benchmark,topk,sstable,reverse,claim-mode" date="2026-07-09" title="Next SSTable-heavy Top-K claim phase" description="Next claim-mode phase after disclosure" source="main@8a12c0f">

### Next SSTable-heavy Top-K claim phase

After benchmark disclosure fields are in place, the next non-blocking performance hardening task should make Part 20 exercise raw reverse SSTable work under BENCH_CLAIM_MODE. Add a variant that forces data out of memtable via checkpoint/restart or an explicit SSTable-heavy setup, then gate DESC ordered Top-K on sstable_reverse_iterator_open_count, reverse block read/decode/yield counters, and bounded fusion_reverse_raw_entry_read_count/fusion_reverse_visible_put_count. Keep query-result cache counters at zero for these claims and record data_dir disclosure so the report proves whether the workload hit persisted SSTables.

</spec-entry>

<spec-entry category="arch" keywords="benchmark,topk,sstable,restart,cold-cache,methodology" date="2026-07-09" title="Next Top-K claim methodology after SSTable checkpoint" description="Next benchmark methodology after checkpoint-based SSTable claim" source="main@8a12c0f">

### Next Top-K claim methodology after SSTable checkpoint

Part 20 claim-mode now proves persisted SSTable reverse path by checkpointing after load/index build, but it does not prove cold disk I/O because the process and OS page cache remain warm. Next commercial-grade benchmark methodology should add an optional restart phase for Part 20 or a dedicated matrix that measures first persisted pass versus warm pass, discloses OS page cache limitations, and records block cache hit/miss/read-byte counters separately from ordered Top-K and SSTable reverse counters. Keep latency claims as smoke-only until sample count and cache discipline are strengthened.

</spec-entry>

<spec-entry category="arch" keywords="benchmark,topk,restart,cold-cache,methodology,sstable" date="2026-07-09" title="Next benchmark restart phase after first-pass Top-K" description="Next restart-based benchmark methodology after first-pass rows" source="main@8a12c0f">

### Next benchmark restart phase after first-pass Top-K

Part 20 now distinguishes first persisted pass after checkpoint from warm persisted pass inside one server process. This still does not clear process caches or OS page cache. The next benchmark methodology step should be an optional controlled restart phase or dedicated matrix that starts FusionDB from the checkpointed data dir, measures first query after readiness, then warm repeats; disclose that OS page cache may remain warm unless an explicit privileged drop-cache or data-size-over-RAM method is used. Keep query-result cache counters at zero and carry forward SSTable reverse and block-cache hit/miss/read-byte counters.

</spec-entry>

<spec-entry category="arch" keywords="benchmark,topk,cold-cache,methodology,restart,os-page-cache" date="2026-07-09" title="Next Top-K restart methodology after owned-server matrix" description="Next methodology steps after Part 23 owned-server restart matrix" source="main@8a12c0f">

### Next Top-K restart methodology after owned-server matrix

The restart matrix now clears FusionDB process-local caches and proves persisted SSTable ordered DESC Top-K after process restart, but still does not prove device-cold disk I/O because OS page cache is uncontrolled. Next methodology options are: add an explicit privileged drop-cache hook disabled by default and clearly disclosed, generate a data set larger than RAM for cold-device evidence, or add block-cache/OS-cache disclosure fields and keep claims scoped to process-cold persisted SSTable path. A later implementation should parameterize SQL/metrics clients instead of temporarily switching global benchmark URLs.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,subagent,topk,lsm,stats,wal,zone-map" date="2026-07-09" title="Next parallel performance task package from subagents" description="Subagent-derived next optimization roadmap and parallelization boundaries" source="main@8a12c0f">

### Next parallel performance task package from subagents

Read-only subagents completed on 2026-07-09. Benchmark methodology recommendations: classify cache_phase, optional explicit drop_caches with meminfo disclosure, multi-trial restart-first-pass, cache pollution gates, schema v2 disclosure, device I/O evidence, and possible direct-IO experiment. LSM/Top-K recommendations by priority: P0 lazy SSTable reverse source activation, P0 index-prefix Bloom/block-prefix pruning, P0/P1 block-internal reverse seek with offset/restart table, P1 ordered Top-K no-fill cache policy, P1 composite covering INCLUDE, P1 residual-predicate exact streaming, P2 direction-aware composite indexes. Broader task package: Stats V2 MCV/histograms, unified join cost API, SQL no-fill plumbing, durable WAL replay floor, partitioned SSTable index, MVCC-safe SQL zone maps, cross-matrix claim hardening, composite INCLUDE. Treat Rust-side write delegation carefully while the worktree is dirty and avoid overlapping file scopes.

</spec-entry>

<spec-entry category="arch" keywords="topk,lazy-reverse,sstable,fusion,design" date="2026-07-09" title="Lazy SSTable reverse source activation design" description="P0 design for delaying SSTable activation in Fusion reverse merge" source="main@8a12c0f">

### Lazy SSTable reverse source activation design

Delegate cdx-125607-f7e0 completed the lazy reverse design. Current Fusion reverse merge eagerly activates every overlapping SSTable and add_reverse_source immediately pulls a first candidate, so small LIMIT DESC Top-K can pay O(overlapping SSTables) startup cost even with visitor early-stop. Recommended P0: keep write-buffer/memtables eager, but put eligible SSTables into a pending max-heap keyed by a safe frontier_user_key; activate a pending SSTable only when active heap is empty or pending.frontier_user_key >= current active top user key. Correctness constraints: preserve one scan snapshot, drain all versions for the same user key before visibility, activate equal-frontier pending SSTables before emitting candidate K to catch tombstones/newer versions, keep stable source_order independent of activation order, and fail open if frontier cannot be safely bounded. Initial frontier can use file-level last key, with later block-property frontier refinement. Tests should cover low-frontier SSTables not opened for LIMIT 1, equal-key pending activation for tombstone/newer-version correctness, and raw reverse counter gates. Metrics should interpret fusion_reverse_source_open_count as activated sources or add a dedicated activation counter.

</spec-entry>

<spec-entry category="arch" keywords="topk,index-prefix,bloom,block-prefix,sstable,design" date="2026-07-09" title="Index-prefix pruning design for Top-K SSTables" description="Design for secondary/composite index-prefix negative pruning" source="main@8a12c0f">

### Index-prefix pruning design for Top-K SSTables

Delegate cdx-125607-3f2e completed index-prefix pruning design. Current prefix Bloom/block-prefix pruning is table-prefix only and often misses secondary/composite Top-K ranges because Fusion probes only when prefix_end(start)==end. Recommended staged design: add a versioned SQL index scan prefix extractor over decoded MVCC user keys, supporting secondary column prefix index:<table>:<column>:, composite index family prefix, and composite leading equality prefix such as host_id for (host_id,ts). Use range-contained-in-supported-prefix rather than exact prefix_end(start)==end. All pruning must be negative-only and fail-open for old SSTables, unknown versions, extractor mismatch, decode failure, cross-prefix ranges, block property mismatch, incomplete prefix sets, or unsafe composite parsing. Add separate index-prefix and block-index-prefix check/positive/skip/fail-open counters, then extend Part 20/23 claim gates only for tagged prefix-prune cases. First implementation should avoid exact value prefixes for TEXT/DECIMAL/raw values until key encoding boundaries are proven.

</spec-entry>

<spec-entry category="arch" keywords="index-prefix,bloom,topk,sstable,composite" date="2026-07-09" title="Next P0 SQL index-prefix SSTable pruning" description="Next high-impact iteration: SQL index-prefix Bloom pruning for composite leading equality Top-K" source="main@8a12c0f">

### Next P0 SQL index-prefix SSTable pruning

Subagents cdx-131641-a051 and cdx-131641-718f converged on the next high-impact storage/index slice: add SSTable-level SQL index-prefix Bloom pruning for secondary/composite index ranges, especially composite leading equality Top-K such as (host_id, ts) with host_id = ?. Current Fusion table-prefix Bloom only probes when prefix_end(start) == end and cannot express index:<table>:<column-family> or composite leading component prefixes, so many SSTables may be opened for Top-K ranges that cannot contain the requested index prefix. P0 should be negative-only/fail-open, versioned for legacy SSTables, add separate index-prefix counters, and gate Part 20/23 with SSTable iterator opens/raw reverse reads decreasing while ordered Top-K and result correctness stay unchanged.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,lsm,topk,no-fill,prefix" date="2026-07-09" title="LSM SQL performance iteration shortlist" description="Ranked LSM-backed SQL performance roadmap from subagent research" source="main@8a12c0f">

### LSM SQL performance iteration shortlist

Subagent cdx-131641-718f ranked the next 2-3 iterations for FusionDB: 1) SQL index-prefix Bloom/block-prefix pruning for secondary and composite Top-K ranges, 2) SQL-level no-fill-cache read policy for bulk/fallback scans to reduce cache pollution, 3) block-internal reverse seek or bounded prefetch depending on whether counters show decode-heavy or IO-heavy DESC Top-K. Lower-priority ideas are MVCC-safe zone maps, partitioned indexes/filters, adaptive Bloom allocation, and learned indexes after isolated benchmarks. Validation should continue through benchmark.py claim gates with query cache disabled/disclosed, ordered Top-K counters, SSTable iterator opens, raw reverse reads, block reads, and block-cache pollution counters.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,reverse-block,no-fill,composite-covering,include" date="2026-07-09" title="Post index-prefix optimization shortlist" description="Next optimization candidates from delegate research after index-prefix pruning" source="main@8a12c0f">

### Post index-prefix optimization shortlist

After SQL index-prefix pruning, delegate outputs ranked the next non-blocking optimization candidates. Highest storage-side candidate is reverse block seek sidecar support, a versioned fail-open block-internal offset/restart sidecar so reverse SSTable iterators decode only needed suffix entries instead of full blocks. Highest SQL/cache candidate is no-fill cache policy plumbing: extend Fusion range visitors and SQL scan helpers with read options, then enable no-fill first for ANALYZE, CREATE INDEX backfill, DDL maintenance, and large no-LIMIT scans. Highest planner/index candidate is composite covering INCLUDE/index-only scans: allow composite INCLUDE metadata/payload, update DML maintenance on include-column changes, and produce CoveredIndexRows from try_composite_index_scan while failing open to base-row fetch for legacy or malformed payloads.

</spec-entry>

<spec-entry category="arch" keywords="no-fill,benchmark,cache-pollution,roadmap" date="2026-07-09" title="SQL no-fill benchmark follow-up" description="Follow-up for stronger SQL no-fill benchmark controls" source="main@8a12c0f">

### SQL no-fill benchmark follow-up

Part 25 now provides a SQL-level no-fill smoke gate that proves unbounded full scans produce SSTable block misses/read bytes and fill-skip counters without block-cache inserts. The next benchmark hardening step should add a controlled fill-cache comparison phase plus hot-block retention proof, ideally with a benchmark-owned server using tiny block cache, row_cache_capacity=0, a prewarmed hot row, and post-scan hot reread counters. Delegate review cited RocksDB fill_cache=false, LevelDB ReadOptions, PostgreSQL bulkread rings, Pebble iterator options, and InnoDB midpoint insertion as supporting cache-pollution control practices.

</spec-entry>

<spec-entry category="arch" keywords="reverse-block,sidecar,sstable,topk,roadmap" date="2026-07-09" title="Reverse block seek sidecar plan from delegate 2026-07-09" description="Fail-open SSTable reverse block seek sidecar design" source="main@8a12c0f">

### Reverse block seek sidecar plan from delegate 2026-07-09

Delegate cdx-reverse-block-sidecar-plan recommends a derived fail-open reverse block seek sidecar for SSTable reverse iterators. Store per-block decoded-payload entry offsets in a versioned sidecar such as .rseek, bind it to the SSTable fingerprint and per-block decoded CRC, lazy-load for reverse iterators, and fall back to the existing full block parse on missing/stale/invalid/mismatch. The sidecar should not alter read_block_at, CRC, decompression, block cache, or no-fill behavior; it only reduces block-internal reverse work from full-block decode to binary search plus emitted/probed entries. Add counters for hit/miss/stale/invalid/write/write_error/use/fail_open and use existing reverse decode/yield counters as benchmark evidence. Tests should cover encode/decode, stale/corrupt fallback, MVCC raw order, compressed blocks, no-fill preservation, and Part 20/23 DESC Top-K gates. Risk: it reduces parse/alloc cost but not block read/decompress cost, so benchmark must confirm whether reverse_block_entry_decode_count rather than I/O is the bottleneck.

</spec-entry>

<spec-entry category="arch" keywords="reverse-block,sidecar,benchmark,roadmap,lsm" date="2026-07-09" title="Next reverse seek sidecar gate after runtime bounded decode" description="Next gate for persisted reverse seek sidecar" source="main@8a12c0f">

### Next reverse seek sidecar gate after runtime bounded decode

Delegates cdx-rseek-impl-review, cdx-rseek-bench-gate, and cdx-lsm-practices-rseek agree that the next non-blocking storage slice is a persisted fail-open .rseek sidecar that stores decoded block entry offsets and per-block decoded CRC/len/count. It should be lazy-loaded by reverse iterators, never by SsTable::open, and must fall back to the current runtime append_reverse_block_entries_in_bounds helper on miss/stale/invalid/block mismatch. Add sidecar hit/miss/stale/invalid/write/write_error/use/fail_open counters and optionally probe_count; extend Part 20/23 gates to require sidecar use/hit and lower decode/yield ratio while treating latency and OS page cache as secondary evidence. External basis cited by delegates: RocksDB iterator bounds and PerfContext, LevelDB block restart offsets, Pebble bounds/block properties, PostgreSQL/InnoDB cache scan-resistance, and WiscKey/Monkey/RUM as later roadmap rather than immediate implementation.

</spec-entry>


<spec-entry category="arch" keywords="benchmark,claim-gate,block-index-prefix,topk,sstable" date="2026-07-09" title="Block SQL index-prefix next benchmark gate" description="Next benchmark gate for block SQL index-prefix pruning" source="main@8a12c0f">

### Block SQL index-prefix next benchmark gate

Read-only delegates on 2026-07-09 recommended making per-block exact SQL index-prefix sets the P0 implementation and keeping all pruning negative-only and fail-open. Next non-blocking benchmark work is a dedicated low-level or Part 24 gate where SSTable-level index-prefix Bloom is positive but block-level SQL index-prefix properties skip mixed decoy blocks. Required metrics: sstable_block_index_prefix_filter_check_count > 0, skip_count > 0, fail_open_count == 0 for the supported positive case, with result checksum unchanged and block reads/decodes below the disabled or fail-open baseline. Add explicit fallback cases for legacy/incomplete/corrupt metadata where skip_count remains 0 and fail_open_count increases.

</spec-entry>

<spec-entry category="arch" keywords="roadmap,block-pruning,benchmark,part24,part30" date="2026-07-09" title="Next block pruning benchmark work after Part 29" description="Next benchmark phase after Part 29" source="main@8a12c0f">

### Next block pruning benchmark work after Part 29

After Part 29, next benchmark work should move from low-level SSTable isolation to SQL/Fusion realism: add a Part 24 extension or Part 30 case where public SQL Top-K data has multiple overlapping SSTables, SSTable-level SQL index-prefix Bloom is MayMatch, and block-level SQL index-prefix skips decoy blocks without synthetic filter-only prefixes. Also vary granularity/layout (clustered, mixed, random), record metadata bytes/decode cost and cache pollution, and keep learned/advanced skipping as research-only until deterministic exact-prefix and min/max block properties are exhausted. This follows delegate research across RocksDB/Pebble block properties, Parquet page index, ClickHouse/DuckDB data skipping, and learned skipping papers.

</spec-entry>

<spec-entry category="arch" keywords="sorted-sstable,benchmark-feasibility,natural-false-positive,part30,block-pruning" date="2026-07-09" title="Sorted SSTable block-index prefix benchmark feasibility" description="Feasibility conclusion for natural block index-prefix benchmarks" source="main@8a12c0f">

### Sorted SSTable block-index prefix benchmark feasibility

A read-only delegate confirmed a strict sorted SSTable cannot contain a real target-prefix block while a separate block in the same file spans that target range but lacks the target prefix: composite SQL index-prefix ranges are contiguous in key order. The correct non-synthetic low-level proof is therefore natural Bloom false-positive: insert real neighboring prefixes, find a file-level Bloom MayMatch for an absent canonical target prefix, then use exact block SQL index-prefix metadata to skip the block. The next realism step remains Part 30 / SQL-Fusion: public SQL Top-K with overlapping SSTables, a real target SSTable for results, and decoy SSTables that produce natural file-level MayMatch then block-level skip; include layout/granularity and metadata/cache cost dimensions.

</spec-entry>

<spec-entry category="arch" keywords="part31,zone-map,mvcc,block-pruning,roadmap" date="2026-07-09" title="Part 31 MVCC-safe SQL zone-map pruning plan 2026-07-09" description="Part 31 zone-map pruning must be MVCC-isolated and fail-open before skipping data blocks." source="main@8a12c0f">

### Part 31 MVCC-safe SQL zone-map pruning plan 2026-07-09

Next high-impact optimization after Part 30 is Part 31 / BENCH_MATRIX=sql_block_zone_map_prune: block-level SQL zone-map pruning for data rows. Critical invariant from subagent research: because SQL predicates are applied after MVCC visible merge, an SSTable data block may be skipped only when skipping it cannot expose an older matching version hidden by a newer nonmatching value or tombstone. First implementation must fail open unless block metadata is complete/version-supported, predicate is in a safe typed subset (=, range, BETWEEN, positive IN over integer/date/timestamp/bool), schema fingerprint matches, and the block user-key interval is proven MVCC-isolated from write buffer, memtables, immutable memtables, and overlapping SSTables. Required fallback cases: newer nonmatching update over older match, tombstone over older match, legacy/malformed metadata, unsupported types/predicates. Suggested metrics: sstable_block_zone_map_filter_check/positive/skip/fail_open, metadata bytes, mvcc_overlap_fail_open, schema_fail_open. Sources searched: Parquet page index, DuckDB zonemaps, ClickHouse minmax skipping indexes, Pebble block properties, RocksDB prefix constraints, CockroachDB MVCC range tombstones.

</spec-entry>

<spec-entry category="arch" keywords="part31,zone-map,mvcc,subagent,roadmap" date="2026-07-09" title="Part 31 subagent consensus 2026-07-09" description="Subagent consensus for staged MVCC-safe Part 31 zone-map implementation and benchmark gates." source="main@8a12c0f">

### Part 31 subagent consensus 2026-07-09

Three read-only subagents reviewed Part 31. Consensus: implement SQL data block zone-map pruning in stages and do not enable any skip inside raw SSTable iterators without Fusion-level MVCC isolation proof. Metadata subagent recommends metadata-only/fail-open scaffold first: keep v4 writes until real zone maps exist, add explicit v5 fallback only when adding wire metadata, and avoid bincode compatibility regressions. MVCC subagent showed dangerous cases: newer nonmatching PUT or tombstone over an older matching row, plus same-user-key versions split across adjacent blocks; any uncertainty must fail open and increment mvcc_overlap/schema fail-open counters. Benchmark subagent recommends a future BENCH_MATRIX=sql_block_zone_map_prune / Part 31 with positive equality/range cases plus MVCC update/tombstone/unsupported fallback cases; counters, checksums, query-result-cache zero, and compaction=0 are hard gates, latency is secondary. External references searched include DuckDB zonemaps, ClickHouse skipping indexes, Parquet page index, Pebble block properties, RocksDB prefix seek constraints, and CockroachDB MVCC range tombstones/Pebble issue #1786.

</spec-entry>

<spec-entry category="arch" keywords="part31,sstable,v5,compatibility,bincode,wire-format" date="2026-07-09" title="Part 31 v5 metadata compatibility notes 2026-07-09" description="Compatibility cautions for SSTable v5 zone-map metadata" source="main@8a12c0f">

### Part 31 v5 metadata compatibility notes 2026-07-09

Subagent consensus: keep default writes on v4 until zone-map metadata exists; add explicit v4 fallback structs instead of relying on bincode shape accidents; reject trailing bytes during metadata fallback decoding because bincode 1.3 deserialize can otherwise accept prefix-compatible payloads. Current v5 scaffold uses the runtime SsTableMeta shape as the v5 wire shape for a narrow first step. Before production zone-map writing becomes broad, consider an explicit v5 wire struct or a small metadata header dispatch to decouple future runtime fields from on-disk format evolution.

</spec-entry>

<spec-entry category="arch" keywords="part31,zone-map,producer,pruning,benchmark,subagent,roadmap" date="2026-07-09" title="Part 31 zone-map next-step subagent synthesis 2026-07-09" description="Subagent synthesis for next Part 31 SQL zone-map work" source="main@8a12c0f">

### Part 31 zone-map next-step subagent synthesis 2026-07-09

Four read-only subagents completed after the metadata scaffold. Producer plan: next safe implementation is metadata producer only, computing per-block SQL zone maps during SSTable builder flush/compaction from data-row PUT payloads, with supported scalar i64-compatible types first and no reader skip. Pruning plan: query-time skip must be introduced later through a dedicated predicate plan and Fusion-level MVCC isolation proof; raw SSTable iterators must not independently skip blocks. Benchmark plan: future BENCH_MATRIX=sql_block_zone_map_prune should include clustered positive cases, random/striped controls, MVCC update/tombstone fallback, schema/legacy fallback, query cache zero checks, compaction=0, counter invariants, and checksum equality. Wire hardening plan recommended explicit v5 wire structs and header dispatch before real producer writes; this was implemented in src/storage/sstable.rs on 2026-07-09.

</spec-entry>

<spec-entry category="arch" keywords="part31,zone-map,producer,review,mvcc,roadmap" date="2026-07-09" title="Part 31 producer review follow-up 2026-07-09" description="Follow-up from producer implementation review subagent" source="main@8a12c0f">

### Part 31 producer review follow-up 2026-07-09

Read-only subagent part31-producer-impl-review completed after the producer implementation. It flagged: Fusion builder paths must enable collection; schema snapshot must include same-memtable schema+rows; empty maps must not force v5; schema fingerprint must avoid usize and Debug text; colon-delimited table names should fail open; future readers must treat tombstone_count/null/schema mismatch/MVCC uncertainty as fail-open. The implementation now addresses the first four directly: Fusion flush/compaction/shutdown use sstable_builder_with_zone_maps, empty maps keep v4, fingerprints use fixed-width ordinals and explicit IndexType tags, and schemas with ':' disable producer collection. Remaining future work before pruning: malformed/type-mismatch tests, compaction update/tombstone cases, and Fusion-level MVCC isolation gate.

</spec-entry>

<spec-entry category="arch" keywords="part31,mvcc,zone-map,gate,delegate" date="2026-07-09" title="Part 31 MVCC isolation gate plan 2026-07-09" description="Part 31 delegate consensus for MVCC-safe SQL zone-map pruning gate" source="main@8a12c0f">

### Part 31 MVCC isolation gate plan 2026-07-09

Delegate part31-mvcc-isolation-gate-plan completed. Consensus: keep SQL zone-map pruning gated at Fusion MVCC layer, never let raw SSTable iterators independently skip blocks. Safe first phase requires narrow predicate plan, full metadata/schema/count validation, block boundary user-key isolation, and fail-open on write-buffer, memtable, overlapping SSTable, tombstone/null/count mismatch, schema/type mismatch, unsupported predicates, legacy metadata, and reverse scans. Current implementation should remain producer/scaffold-only until Fusion can prove block isolation.

</spec-entry>

<spec-entry category="arch" keywords="part31,subagent,zone-map,mvcc,benchmark" date="2026-07-09" title="Part 31 follow-up subagent synthesis 2026-07-09" description="Subagent synthesis for Part 31 Fusion isolation, evaluator, and benchmark rollout" source="main@8a12c0f">

### Part 31 follow-up subagent synthesis 2026-07-09

Three read-only subagents completed for the next Part 31 steps. Fusion isolation design recommends: add validated SSTable block property helpers, keep reverse scans fail-open, evaluate zone-map blocks only after Fusion proves block user-key interval isolation against write buffer, memtables, adjacent same-user-key block boundaries, and overlapping SSTables, then pass approved skip offsets to the raw iterator. Decision-engine design confirmed the storage-layer three-state evaluator semantics: skip only on trusted no-match; read on possible match; fail-open on missing/mismatched metadata, schema/type/count/null/tombstone/MVCC uncertainty. Benchmark design recommends BENCH_MATRIX=sql_block_zone_map_prune with clustered positive, random control, absent predicate, MVCC fallback, schema/null/unsupported fallback, query-cache zero, compaction=0, checksum equality, and counters as hard gates.

</spec-entry>

<spec-entry category="arch" keywords="part31,roadmap,delegate,zone-map,performance" date="2026-07-09" title="Post Part 31 optimization roadmap from delegate synthesis 2026-07-09" description="Delegate synthesis for the next optimization wave after Part 31 owned claim" source="main@8a12c0f">

### Post Part 31 optimization roadmap from delegate synthesis 2026-07-09

Three read-only delegate agents reviewed Part 31 benchmark isolation, fail-open reduction, and commercial-database roadmap. Next highest priority tasks: add enabled-vs-disabled Part 31 control gate and block-read/read-byte deltas; split MVCC fail-open reason counters before changing pruning policy; consider safe reverse/DESC zone-map approved skip; then evaluate composite INCLUDE index-only Top-K, Stats V2 MCV/histograms, batch/vectorized scan pipeline, score/overlap-aware compaction scheduling, and partitioned SSTable metadata/index/filter cache. All pruning changes must preserve Fusion-level MVCC proof and raw SSTable iterators must consume only opaque approved offsets.

</spec-entry>

<spec-entry category="arch" keywords="composite,include,metadata,c4,c5,explain,benchmark" date="2026-07-09" title="Next P0 after Top-K row-source metrics" description="P0 risk and next steps after Top-K row-source metric hardening." source="main@8a12c0f">

### Next P0 after Top-K row-source metrics

Read-only delegate review on 2026-07-09 found the next P0 after ordered Top-K row-source metrics: composite INCLUDE metadata currently uses delimiter text c4:<table>:<key_columns_csv>:<include_columns_csv>. Quoted identifiers containing ':' or ',' can make metadata parse incorrectly. Before treating composite INCLUDE as release-grade, replace c4 with a structured c5/length-prefixed or serialized metadata format, or at minimum reject unsafe identifiers at DDL boundaries. Keep c4 read compatibility and fail-open behavior for legacy entries. Secondary follow-ups: EXPLAIN should report covering eligible rather than Index Only Scan because payload fail-open can force base-row fetch; Part 20 should later add a composite heap-fetch paired control with checksum matching.

</spec-entry>

<spec-entry category="arch" keywords="subagent,s3,include,identifier,keyspace,follow-up" date="2026-07-09" title="s3 subagent follow-up synthesis 2026-07-10" description="Subagent synthesis after s3 metadata hardening." source="main@8a12c0f">

### s3 subagent follow-up synthesis 2026-07-10

Three read-only subagents reviewed the single-column INCLUDE s3 work. Consensus: s3 length-prefixed metadata is the right replacement for delimiter s2 and parser ordering/fail-open behavior are sound. High-priority follow-ups identified: protect single-column INCLUDE key/include columns in ALTER TABLE DROP/RENAME COLUMN; add SQL-level legacy s2 and malformed s3/payload fail-open tests; address broader storage key delimiter risks for data/index/FTS namespaces; and unify quoted column identifier canonicalization because CREATE TABLE stores Ident::to_string() while CREATE INDEX resolves ident.value. The first two implementation risks partially addressed on 2026-07-10: ALTER TABLE dependency protection was implemented, and data-prefix scan filtering was added as a compatibility hardening layer. Remaining broader work: structured key components for index/FTS/count namespaces and canonical identifier handling.

</spec-entry>

<spec-entry category="arch" keywords="subagent,keyspace,identifier,canonicalization,structured-keys,roadmap" date="2026-07-09" title="Structured keyspace and identifier subagent synthesis 2026-07-10" description="Subagent synthesis for structured keyspace and identifier canonicalization roadmap." source="main@8a12c0f">

### Structured keyspace and identifier subagent synthesis 2026-07-10

Two read-only subagents completed after the s3 fail-open tests. Keyspace subagent recommends a vNext structured key codec with namespace tags and length-prefixed identifier components for data/index/FTS/count-summary keys, plus typed order-preserving value components for secondary indexes; migration should start with parsers/read compatibility, then dual-write/dual-delete, then backfill/metadata versioning. It warns that length-prefixing ordered value components would break range scans, and that SSTable prefix/Bloom extractors need versioned parsing. Identifier subagent recommends PostgreSQL-style canonicalization eventually: unquoted identifiers fold to lowercase while quoted identifiers store exact Ident.value; current TableSchema::get_column_index remains quote-unaware and case-insensitive. The implemented 2026-07-10 slice only fixes writer-side column identity by storing Ident.value in CREATE/ALTER TABLE and leaves table-name storage plus exact quote-aware lookup for future work.

</spec-entry>

<spec-entry category="arch" keywords="backlog,commercial-parity,unique,sentinel,side-index,raft,vectorized" date="2026-07-10" title="商业对标 backlog v1(2026-07-10 调研)" source="main@47449f4">

### 商业对标 backlog v1(2026-07-10 调研)

5 路带源调研(PG index-locking/GIN、CRDB/TiDB unique-key、Qdrant/Milvus/LanceDB、proposer-evaluated KV、CRDB vectorized)合成的分级清单。P0 正确性:①side-index(trigram/HNSW)提交耦合延迟应用——per-txn delta buffer,OCC 验证通过后在 commit lock 内应用,abort 即丢弃(InnoDB FTS 模式,零 undo);②UNIQUE sentinel key——复合唯一检查是未验证的 prefix 扫描(composite_index.rs:709-714),并发同值双提交;修复=向 write_buffer 额外写不含 row_id 的 sentinel key(值=row_id),现有 exact-key OCC 使败者确定性 abort(CRDB/TiDB 形态);NULL 跳过,DELETE 写墓碑,UPDATE 迁移;单列路径 insert.rs:481-503 同样审计;③Raft 复制 OCC 写集而非 SQL 文本(UUID/NOW 发散)+ pgwire DML 接入 Raft + vote/log 持久化。P1:读路径 candidate-then-verify(PG 核心不变式,残留索引垃圾无害化);向量索引=持久化列重建+delta map+tombstone 过滤(共识:不做 graph WAL,10x 放大且 hora 不支持;>1M 向量换 usearch);批量列式扫描 kernel(CRDB 同构证明 70x micro/4x TPC-H);fan-out 改为复制数据上的计算分区。P2:块级 PREWHERE、morsel 调度、自适应谓词重排、SSTable 侧车文本索引、HNSW 快照加速。冲突裁决:约束键必须进 OCC 写集,搜索索引 delta 必须不进——txn buffer 设计需显式编码此区分。全文:.workflow 调研 workflow wf_33111aa5-032 journal。

</spec-entry>

<spec-entry category="arch" keywords="xlarge,capability,regression,scan" date="2026-07-10" title="xlarge 能力快照与规模敏感回归(2026-07-10)" source="main@27935da">

### xlarge 能力快照与规模敏感回归(2026-07-10)

268 万行 xlarge 全量(afed057,468 修复后):索引/点查商业级(索引 7.2ms、点查 6ms、加速比 836×、单行事务写 10-24ms);LIKE 2.3×/DISTINCT 2.7× 优于 07-08 基线;但扫描/OLAP 出现基线不曾有的病态:Full scan 6.0s(基线 1.6s)、IN list 5.6s、Revenue 12.6s、Range id>N LIMIT 100 达 12s、Avg order value 37s、并发 read-heavy 60s 超时×6。已排除 SSTable 堆积(仅 3 个)与 bloom 饱和(468 已修)。BENCHPROD-469:用 8a12c0f 存档点 vs HEAD 同数据集受控 A/B,首刀 Range LIMIT 12s。教训:large(228k)全绿≠xlarge 无回归,规模敏感路径必须 xlarge 门禁。报告存档 .workflow/.csv-wave/20260710-xlarge-capability-snapshot.json

</spec-entry>

<spec-entry category="arch" keywords="xlarge,capability-report,对标,并发,limit-pushdown" date="2026-07-10" title="xlarge 能力报告解读 v1(2026-07-10,268 万行 103 项)" description="268万行xlarge全量基准的完整解读:二态系统格局、对标商业库位置评估、三层慢端问题与优先级" source="main@a1462d2">

### xlarge 能力报告解读 v1(2026-07-10,268 万行 103 项)

核心格局:二态系统——命中索引/摘要=商业级,跌落全表扫描=秒级(落后 1-2 数量级),无中间地带。【快端·商业级】GROUP BY 结果缓存 0.58ms;索引计数摘要 1.7-4.6ms(City distribution/Stock by category 等,不碰行);PK 点查 6.0-6.4ms、二级索引 7.2ms(50 万行表,HTTP 协议,pgwire 再快 ~10×)→ OLTP 点查/索引读与商业库同量级。【写入·合格】单行 8-23ms 真 fsync(Single UPDATE 8.2/INSERT 13.8/Record transfer 23.5),商业库 NVMe 1-5ms,差距 2-5×,主因 group commit 批次小+同步索引维护;装载 19.4k 行/s(468 修复后)。【慢端三层】①已修:bloom 定容装载塌方(468);②已定位未修(469):Range id>N LIMIT 100=12-29s——LIMIT 未下推到带谓词 PK 区间路径,25 万行物化后截断(对照:100 行小区间同查询 31ms,机制在、参数丢);val=42 LIMIT=6.2s 有索引不走;Avg order value 37s/Subquery IN 24s/Revenue 12.6s 为同类复合;③结构性:并发脆弱——Read-heavy 80:20 均值 8.6s+426 个 60s 超时错误,Balanced 247 错,机理=秒级扫描占满 32 线程→点查排队雪崩(队头阻塞),缺查询超时/准入控制/资源分组。【基线对比可信项】LIKE 2.3×、DISTINCT 2.7× 真实进步(7 月 trigram/块剪枝生效);Full scan 1591→6003ms 不完全可比(数据+17.5%、基线无 zone-map 层)但扣除后仍有真实回归(469 查)。【对标位置】OLTP 读=同量级✅;单行写=2-5×🟡;OLAP 扫描=1-2 数量级🔴(正解=批量列式 kernel,CRDB 同构证明 4× TPC-H);并发鲁棒=最弱🔴;正确性=463/464/465 修完后最接近商业标准(并发下不给错数据只会慢)。一句话:能当很快的 KV+索引库,不能当分析库,高并发需护栏。优先级:469 两缺陷(便宜收益大)→查询超时护栏→批量列式扫描。原始报告:.workflow/.csv-wave/20260710-xlarge-capability-snapshot.json(已入库 a1462d2)

</spec-entry>

<spec-entry category="arch" keywords="zone-map,audit,precompute,470" date="2026-07-10" title="zone-map 挂载点审计:预计算 vs 早停模式存量(2026-07-10)" source="main@087b503">

### zone-map 挂载点审计:预计算 vs 早停模式存量(2026-07-10)

469 修复后全量审计 6 个 sql_block_zone_map_scan_options 挂载点:scan/mod.rs:1813 PK区间=已修(仅无limit挂载);2284 Top-K visitor=合理(Top-K 天然全遍历,预计算摊销);2370=合理(有谓词时 scan_limit 必为 None 走 bulk,无谓词时无计划可挂);3136/3158=测试。唯一存量:2339 FilteredLimitScanVisitor(444)——visitor 自停但计划照挂,与 469 的区别:谓词 zone map 有真实剪枝收益,稀疏匹配(匹配少、块可大量跳)预计算是赚的,密集匹配(visitor 快速集满 limit)是亏的。决策需成本闸门:用 StatsEstimator 的选择率估计(已有 selectivity 基建)——估计匹配行数 >> limit 时挂计划,否则不挂;或先做 sparse/dense 两态 A/B 实测再定阈值。候选票 BENCHPROD-470,先测后动。

</spec-entry>