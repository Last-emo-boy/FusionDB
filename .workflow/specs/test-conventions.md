---
title: "Test Conventions"
readMode: required
priority: high
category: test
keywords:
  - test
  - coverage
  - mock
  - fixture
  - assertion
  - framework
---

# Test Conventions

## Framework

## Directory Structure

## Naming Conventions

## Patterns

## Entries



<spec-entry category="test" keywords="prefix-bloom,metrics,benchmark,sstable" date="2026-07-08" title="SSTable prefix Bloom metrics benchmark" description="Prefix Bloom counters and Part 16 benchmark expectations" source="main@8a12c0f">

### SSTable prefix Bloom metrics benchmark

Prefix Bloom observability uses four SSTable-probe counters: sstable_prefix_filter_check_count, positive_count, skip_count, and fail_open_count. FusionStorage increments them only after user-key range overlap and only when the scan range is prefix-safe. benchmark.py Part 16 is selected by BENCH_MATRIX=sstable_prefix_bloom, builds lexicographic a/m/z tables, expects absent-table prefix scans to increase skip counts, positive table scans to increase positive counts, and 2-row PK range controls to keep prefix checks at zero.

</spec-entry>

<spec-entry category="test" keywords="frontier,benchmark,fusion,part28,test" date="2026-07-09" title="Part 28 Fusion reverse frontier smoke 2026-07-09" description="Verification for Fusion public-API reverse frontier benchmark" source="working-tree">

### Part 28 Fusion reverse frontier smoke 2026-07-09

Verified src/bin/fusion-reverse-frontier-bench.rs and benchmark.py Part 28 with cargo check -q --bin fusion-reverse-frontier-bench --bin sstable-reverse-frontier-bench, python3 -m py_compile benchmark.py, rustfmt --check --edition 2021 src/bin/fusion-reverse-frontier-bench.rs src/bin/sstable-reverse-frontier-bench.rs, git diff --check on the touched benchmark/spec files, direct debug smoke BENCH_FUSION_REVERSE_FRONTIER_DECOYS=2 BENCH_FUSION_REVERSE_FRONTIER_ITERS=2 BENCH_FUSION_REVERSE_FRONTIER_PAYLOAD_BYTES=8192 cargo run -q --bin fusion-reverse-frontier-bench, benchmark debug smoke BENCH_MATRIX=fusion_reverse_frontier BENCH_FUSION_REVERSE_FRONTIER_DECOYS=2 BENCH_FUSION_REVERSE_FRONTIER_ITERS=2 BENCH_FUSION_REVERSE_FRONTIER_PAYLOAD_BYTES=8192 BENCH_FUSION_REVERSE_FRONTIER_RELEASE=0 python3 benchmark.py, and release smoke BENCH_MATRIX=fusion_reverse_frontier BENCH_FUSION_REVERSE_FRONTIER_DECOYS=2 BENCH_FUSION_REVERSE_FRONTIER_ITERS=1 BENCH_FUSION_REVERSE_FRONTIER_PAYLOAD_BYTES=8192 BENCH_FUSION_REVERSE_FRONTIER_RELEASE=1 BENCH_FUSION_REVERSE_FRONTIER_TIMEOUT_SEC=600 python3 benchmark.py. Representative debug counters: LIMIT 1 activations=2, deferred=4, visible_puts=2; full drain activations=6, deferred=0, rows=8; equal-frontier tombstone activations=4, equal-frontier activations=2, rows=0; compaction_run_count=0 in every phase. Release smoke wrote benchmark_report_medium_http_matrix_fusion_reverse_frontier.json and passed all Part 28 gates.

</spec-entry>

<spec-entry category="test" keywords="frontier,benchmark,sstable,part27,test" date="2026-07-09" title="Part 27 SSTable reverse frontier smoke 2026-07-09" description="Verification for deterministic reverse frontier microbench" source="working-tree">

### Part 27 SSTable reverse frontier smoke 2026-07-09

Verified src/bin/sstable-reverse-frontier-bench.rs and benchmark.py Part 27 with cargo check -q --bin sstable-reverse-frontier-bench, python3 -m py_compile benchmark.py, rustfmt --check --edition 2021 src/bin/sstable-reverse-frontier-bench.rs, git diff --check -- benchmark.py src/bin/sstable-reverse-frontier-bench.rs, direct smoke BENCH_SST_REVERSE_FRONTIER_DECOYS=4 BENCH_SST_REVERSE_FRONTIER_ITERS=2 BENCH_SST_REVERSE_FRONTIER_PAYLOAD_BYTES=64 cargo run -q --bin sstable-reverse-frontier-bench, and benchmark smoke BENCH_MATRIX=sstable_reverse_frontier BENCH_SST_REVERSE_FRONTIER_DECOYS=4 BENCH_SST_REVERSE_FRONTIER_ITERS=2 BENCH_SST_REVERSE_FRONTIER_PAYLOAD_BYTES=64 BENCH_SST_REVERSE_FRONTIER_RELEASE=0 python3 benchmark.py. The direct smoke reported optimized activations=2, deferred_unopened=8, reverse_iterator_open_count=2; file-level control activations=10, deferred_unopened=0, reverse_iterator_open_count=10; activation_reduction=8, ratio=5.0, same_results=true. The benchmark.py matrix completed without requiring a running FusionDB server and wrote benchmark_report_medium_http_matrix_sstable_reverse_frontier.json.

</spec-entry>

<spec-entry category="test" keywords="frontier,metrics,benchmark,topk,sstable" date="2026-07-09" title="Reverse frontier metrics and Part 26 verification 2026-07-09" description="Verification for reverse frontier counters and diagnostic benchmark" source="main@8a12c0f">

### Reverse frontier metrics and Part 26 verification 2026-07-09

Verified reverse frontier metrics with cargo fmt --check, cargo check -q, python3 -m py_compile benchmark.py, git diff --check on touched files, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_uses_in_range_block_frontier -- --nocapture --test-threads=1, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_activates_equal_frontier_sstables_before_emit -- --nocapture --test-threads=1, cargo test -q server::http_server::tests::http_metrics_include_pg_connection_pool_fields -- --nocapture --test-threads=1, cargo test -q storage::fusion::tests::fusion_scan_range_reverse -- --nocapture --test-threads=1, and cargo test -q storage::sstable::tests::reverse_frontier_user_key_for_range_uses_in_range_block_properties -- --nocapture --test-threads=1. Built target/debug/fusiondb and ran a temporary HTTP server from /tmp/fusiondb_frontier_smoke on port 18151. Small claim-mode smoke passed with BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk_frontier BENCH_INDEX_TOPK_LIMIT=8 BENCH_INDEX_TOPK_FRONTIER_DECOY_SSTABLES=2 FUSIONDB_URL=http://127.0.0.1:18151/query python3 benchmark.py. The report benchmark_report_small_http_matrix_index_topk_frontier.json records Part 26 as diagnostic evidence for frontier probe/tighten and Bloom-positive coverage; exact activation reduction is proven by the focused Fusion test that hand-builds SSTables and asserts pending activation/deferred counters.

</spec-entry>

<spec-entry category="test" keywords="frontier,lazy-reverse,sstable,fusion,test" date="2026-07-09" title="In-range reverse frontier verification 2026-07-09" description="Verification for range-local SSTable reverse activation frontier" source="main@8a12c0f">

### In-range reverse frontier verification 2026-07-09

Verified range-local SSTable reverse activation frontier with cargo fmt --check, cargo check -q, git diff --check -- src/storage/sstable.rs src/storage/fusion.rs, cargo test -q storage::sstable::tests::reverse_frontier_user_key_for_range_uses_in_range_block_properties -- --nocapture --test-threads=1, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_uses_in_range_block_frontier -- --nocapture --test-threads=1, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_lazily_activates_sstable_sources_by_frontier -- --nocapture --test-threads=1, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_activates_equal_frontier_sstables_before_emit -- --nocapture --test-threads=1, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_skips_sstable_by_sql_index_prefix_filter -- --nocapture --test-threads=1, cargo test -q storage::fusion::tests::fusion_scan_range_reverse -- --nocapture --test-threads=1, cargo test -q storage::sstable::tests -- --nocapture --test-threads=1, and cargo test -q storage::sstable::tests::user_key_range_reverse_iterator_bounds_skip_outside_blocks -- --nocapture --test-threads=1. The focused Fusion test builds one overlapping high-file-frontier SSTable with no in-range block and one matching SSTable, then asserts DESC LIMIT 1 returns the matching row while the SSTable activation hook reports exactly one activation. This proves block-property frontier pruning reduces unopened SSTables without changing visible reverse merge semantics.

</spec-entry>

<spec-entry category="test" keywords="prefix-bloom,benchmark,results,sstable" date="2026-07-08" title="SSTable prefix Bloom benchmark results 2026-07-08" description="Clean Part 16 medium/xlarge release benchmark results" source="main@8a12c0f">

### SSTable prefix Bloom benchmark results 2026-07-08

Part 16 release-server clean runs: medium BENCH_PREFIX_BLOOM_ROWS=4096 payload=1024 loaded 8192 rows in 308 ms; absent-prefix warm avg 0.553 ms with checks/query=2 skips/query=2 blocks/query=4; positive-prefix warm avg 4.915 ms with blocks/query=1031; 2-row PK range control warm avg 0.450 ms with prefix checks=0 and blocks/query=4. xlarge BENCH_PREFIX_BLOOM_ROWS=16384 payload=1024 loaded 32768 rows in 1517 ms; absent-prefix warm avg 0.648 ms with checks/query=6 skips/query=6 blocks/query=8; positive-prefix warm avg 11.639 ms with checks/query=3 skips/query=1 blocks/query=4129; PK range control warm avg 0.679 ms with checks=0 blocks/query=5. No fail-open observed. Reports: benchmark_report_medium_http_matrix_sstable_prefix_bloom.json and benchmark_report_xlarge_http_matrix_sstable_prefix_bloom.json.

</spec-entry>

<spec-entry category="test" keywords="read-amplification,metrics,benchmark,sstable" date="2026-07-08" title="SSTable read amplification metrics" description="Read amplification counters and derived benchmark metadata" source="main@8a12c0f">

### SSTable read amplification metrics

Storage benchmark reports now include derived read-amplification metadata from engine counters: sstable_point_probes_per_query, sstable_range_probes_per_query, sstable_range_overlap_skips_per_query, sstable_iterator_opens_per_query, block_cache_hit_ratio, cold_miss_ratio, blocks_per_returned_row, prefix_filter_skip_ratio, positive_ratio, and fail_open_ratio. A 2026-07-08 xlarge Part 16 rerun with 16384 rows/table and 1024-byte payload produced absent-prefix warm range_probes/query=6, iterator_opens/query=0, prefix skip_ratio=1.0, blocks/query=8; positive-prefix warm range_probes/query=27, iterator_opens/query=26, prefix skip_ratio=0.333333, blocks/query=4129. Total range probes include catalog/schema scans, so future benchmark work should separate data-prefix probes from total storage probes.

</spec-entry>

<spec-entry category="test" keywords="benchmark,sstable,block-prefix,read-amplification,metrics" date="2026-07-08" title="SSTable block-prefix microbenchmark" description="Low-level benchmark for SSTable block table-prefix property skip" source="main@8a12c0f">

### SSTable block-prefix microbenchmark

benchmark.py now registers non-default Part 17 / BENCH_MATRIX=sstable_block_prefix. It runs src/bin/sstable-block-prefix-bench instead of HTTP SQL setup, so it isolates per-block table-prefix property filtering from table-level prefix Bloom. The microbench builds paired optimized and fail-open SSTable sets; each SSTable has one mixed a/z block spanning absent data:m:. Optimized tables include block table_prefixes and should report sstable_block_prefix_filter_skip_count == sstable_count*iters with block_cache_miss_count == 0. Fail-open tables omit block prefix metadata and should report fail_open_count == block_cache_miss_count == sstable_count*iters. Smoke on 2026-07-09 with 8 SSTables, 2 iters, 128-byte payload produced optimized avg 0.023 ms, 16 skips, 0 misses; fail-open avg 0.738 ms, 16 fail-opens, 16 misses, about 32x speedup. Report: benchmark_report_small_http_matrix_sstable_block_prefix.json.

</spec-entry>

<spec-entry category="test" keywords="benchmark,sstable,block-prefix,release,read-amplification" date="2026-07-08" title="SSTable block-prefix release benchmark 2026-07-09" description="Release baseline for block table-prefix property skip" source="main@8a12c0f">

### SSTable block-prefix release benchmark 2026-07-09

Part 17 release run on local machine with BENCH_MATRIX=sstable_block_prefix default config: 512 SSTables, 5 iterations, 1024-byte payload. Optimized block-prefix metadata path averaged 0.119 ms with 2,560 block-prefix checks, 2,560 skips, 0 fail-opens, and 0 block cache misses. Fail-open control averaged 33.479 ms with 2,560 fail-open probes and 2,560 block cache misses. This isolates the v3 block table-prefix property filter from table-level prefix Bloom and shows about 282x speedup for an absent exact table-prefix range over many mixed blocks. Report: benchmark_report_small_http_matrix_sstable_block_prefix.json.

</spec-entry>

<spec-entry category="test" keywords="benchmark,sstable,user-key-bloom,part18,block-cache" date="2026-07-08" title="SSTable user-key Bloom microbenchmark 2026-07-09" description="Part 18 user-key Bloom read-amplification benchmark baseline" source="main@8a12c0f">

### SSTable user-key Bloom microbenchmark 2026-07-09

Part 18 benchmark matrix key is sstable_user_key_bloom. Smoke command: BENCH_SCALE=small BENCH_MATRIX=sstable_user_key_bloom BENCH_SST_USER_KEY_BLOOM_RELEASE=0 BENCH_SST_USER_KEY_BLOOM_SSTABLES=8 BENCH_SST_USER_KEY_BLOOM_ITERS=2 BENCH_SST_USER_KEY_BLOOM_PAYLOAD_BYTES=128 python3 benchmark.py. Current report benchmark_report_small_http_matrix_sstable_user_key_bloom.json shows optimized avg 0.003 ms with 16 user-key skips and 0 block cache misses; fail-open control avg 0.487 ms with 16 fail-opens and 16 block cache misses; speedup_vs_fail_open 177.4x. Required correctness checks: optimized skips must equal sstables*iters and fail-open misses must equal sstables*iters.

</spec-entry>

<spec-entry category="test" keywords="benchmark,sstable,no-fill,block-cache,part19" date="2026-07-08" title="SSTable no-fill cache benchmark 2026-07-09" description="Part 19 no-fill cache policy release benchmark baseline" source="main@8a12c0f">

### SSTable no-fill cache benchmark 2026-07-09

Part 19 benchmark matrix key is sstable_no_fill_cache. Default release run on local machine: BENCH_SCALE=small BENCH_MATRIX=sstable_no_fill_cache python3 benchmark.py. With 512 scan blocks, 5 iterations, 1024-byte payload, and 1-block cache, fill-cache scan averaged 20.099 ms with 2,562 misses, 2,562 inserts, 0 fill-skips, and hot reread hits/misses 3/2. no-fill scan averaged 18.259 ms with 2,560 misses, 0 inserts, 2,560 fill-skips, and hot reread hits/misses 5/0, about 1.10x faster while avoiding cache admission churn. Report: benchmark_report_small_http_matrix_sstable_no_fill_cache.json.

</spec-entry>

<spec-entry category="test" keywords="benchmark,sstable,file-open,no-fill,part19" date="2026-07-08" title="SSTable no-fill file-open benchmark 2026-07-09" description="Part 19 evidence for iterator file-handle reuse and no-fill cache policy" source="main@8a12c0f">

### SSTable no-fill file-open benchmark 2026-07-09

Part 19 now records sstable_block_file_open_count and sstable_block_read_bytes. Current default release run: BENCH_SCALE=small BENCH_MATRIX=sstable_no_fill_cache python3 benchmark.py. With 512 scan blocks, 5 iterations, and 1024-byte payload, fill-cache scan averaged 21.238 ms with 2,565 misses, 2,565 inserts, 10 file opens, hot reread hits/misses 0/5. no-fill scan averaged 16.156 ms with 2,560 misses, 0 inserts, 2,560 fill-skips, 5 file opens, hot reread hits/misses 5/0, speedup_vs_fill_cache 1.315x. Core acceptance: no-fill file opens must be <= iters despite scan_blocks*iters cold misses, proving iterator-level handle reuse. Report: benchmark_report_small_http_matrix_sstable_no_fill_cache.json.

</spec-entry>

<spec-entry category="test" keywords="benchmark,index-topk,topk,part20,order-by" date="2026-07-08" title="Indexed Top-K benchmark 2026-07-09" description="Part 20 indexed ORDER BY Top-K smoke benchmark" source="codex:index-topk-bench-2026-07-09">

### Indexed Top-K benchmark 2026-07-09

benchmark.py now includes non-default Part 20 / BENCH_MATRIX=index_topk. Setup creates bench_topk_scan, bench_topk_idx, and bench_topk_cover with deterministic NOT NULL integer scores, plus a normal score index and a score INCLUDE(payload) index. Small HTTP smoke with BENCH_INDEX_TOPK_ROWS=2000 and BENCH_INDEX_TOPK_LIMIT=20 loaded 6000 rows in 867 ms. Results: full scan ASC avg 10.8 ms, existing range ceiling avg 3.0 ms, pure indexed ORDER BY ASC avg 2.5 ms, indexed heap payload avg 3.6 ms, covering payload avg 2.6 ms, expression fallback avg 14.4 ms. Report: benchmark_report_small_http_matrix_index_topk.json.

</spec-entry>

<spec-entry category="test" keywords="topk,explain,test,sql_ddl" date="2026-07-09" title="EXPLAIN ordered Top-K verification 2026-07-09" description="Verification set for EXPLAIN secondary BTree ordered Top-K visibility" source="main@8a12c0f">

### EXPLAIN ordered Top-K verification 2026-07-09

Validated the EXPLAIN ordered Top-K visibility patch with: cargo test -q --test sql_ddl explain_order_by_secondary -- --nocapture; cargo check -q; cargo test -q --test sql_index_cache secondary_btree_order_by_limit -- --nocapture; cargo test -q --test sql_stream_scan test_streaming_order_by_alias_shadow_falls_back_to_query_sort -- --nocapture; cargo test -q --test sql_ddl -- --nocapture; git diff --check. Added tests for NOT NULL indexed ASC hit, nullable fallback, and projection alias fallback.

</spec-entry>

<spec-entry category="test" keywords="topk,test,benchmark,types,index_topk" date="2026-07-09" title="Top-K type expansion verification 2026-07-09" description="Verification and benchmark smoke for ordered Top-K type expansion" source="main@8a12c0f">

### Top-K type expansion verification 2026-07-09

Validated safe type expansion with: cargo test -q execution::scan::index_plan::tests::secondary_index_ -- --nocapture; cargo test -q --test sql_index_cache secondary_btree_order_by_limit_ -- --nocapture; cargo test -q --test sql_ddl explain_order_by_secondary -- --nocapture; cargo check -q; cargo test -q --test sql_index_cache -- --nocapture; cargo test -q --test sql_ddl -- --nocapture; python3 -m py_compile benchmark.py; BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 python3 benchmark.py; cargo test -q; git diff --check. Smoke benchmark loaded 2000 rows in 395 ms and new type cases averaged: BOOLEAN 1.9 ms, DATE32 1.9 ms, TIMESTAMPTZ 2.2 ms, INTERVAL 1.9 ms, TIMESTAMPTZ covering payload 1.9 ms; DESC fallback remained slower at 9.2 ms.

</spec-entry>

<spec-entry category="test" keywords="reverse,storage,memory,test" date="2026-07-09" title="Memory reverse range verification 2026-07-09" description="Regression coverage for Memory scan_range_reverse" source="main@8a12c0f">

### Memory reverse range verification 2026-07-09

Verified the first storage reverse-range slice with cargo test -q storage::memory::tests::test_scan_range_reverse -- --nocapture, cargo test -q, and git diff --check. Coverage includes descending order, write-buffer overwrite, committed tombstone shadowing, write-buffer-only tombstone not consuming limit, last() using reverse limit one, limit zero, start greater than end, and start equal end.

</spec-entry>

<spec-entry category="test" keywords="reverse,sstable,test,mvcc" date="2026-07-09" title="SSTable reverse iterator verification 2026-07-09" description="Regression coverage for SSTable reverse iterator" source="main@8a12c0f">

### SSTable reverse iterator verification 2026-07-09

Verified with cargo test -q storage::sstable::tests -- --nocapture, cargo test -q, and git diff --check. Coverage includes reverse no-fill cache behavior, upper-bound block skip, lower-bound block stop, lower inclusive semantics, start equal end empty range, block-prefix property negative skip, user-key upper bound ignoring MVCC timestamp suffix, and same-user internal versions returning oldest-to-newest under reverse internal ordering.

</spec-entry>

<spec-entry category="test" keywords="fusion,reverse,test,mvcc" date="2026-07-09" title="Fusion reverse range verification 2026-07-09" description="Regression coverage for Fusion scan_range_reverse" source="main@8a12c0f">

### Fusion reverse range verification 2026-07-09

Verified with cargo test -q storage::fusion::tests::fusion_scan_range_reverse -- --nocapture, cargo test -q storage::fusion::tests::scan_range_user_key_prefix_boundary_includes_shorter_key_from_sstable -- --nocapture, cargo test -q storage::fusion::tests -- --nocapture, cargo check -q, cargo test -q, and git diff --check. Coverage includes forward/reverse equivalence after MVCC merge, write-buffer put/delete priority, immutable and active memtable versions, SSTable versions, future versions ignored by stale read_ts, visible tombstones suppressing rows, limit counting only visible PUT rows, last() returning the next live high key, and [a,a\0) shorter-key bounds for memtable and SSTable paths.

</spec-entry>

<spec-entry category="test" keywords="topk,desc,test,explain,index" date="2026-07-09" title="DESC secondary Top-K verification 2026-07-09" description="Regression coverage for DESC secondary ordered Top-K" source="main@8a12c0f">

### DESC secondary Top-K verification 2026-07-09

Verified DESC secondary BTree Top-K with cargo test -q --test sql_index_cache secondary_btree_order_by_limit -- --nocapture, cargo test -q --test sql_index_cache secondary_btree_range_order -- --nocapture, cargo test -q --test sql_ddl explain_order_by_secondary -- --nocapture, cargo check -q, python3 -m py_compile benchmark.py, cargo test -q, and git diff --check. Coverage includes pure ORDER BY score DESC LIMIT/OFFSET, WHERE range ORDER BY score DESC LIMIT, covering INCLUDE payload under DESC, EXPLAIN ASC/DESC hit, nullable DESC fallback, projection alias DESC fallback, and the reverse capability gate.

</spec-entry>

<spec-entry category="test" keywords="benchmark,index-topk,desc,topk" date="2026-07-09" title="Index Top-K DESC benchmark smoke 2026-07-09" description="Small HTTP smoke for DESC indexed Top-K" source="main@8a12c0f">

### Index Top-K DESC benchmark smoke 2026-07-09

Ran BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 python3 benchmark.py against local FusionDB HTTP server. Report benchmark_report_small_http_matrix_index_topk.json: loaded 2000 rows in 451 ms; TopK DESC index order averaged 57.5 ms for 20 rows; TopK DESC covering payload averaged 63.2 ms for 20 rows. Server was shut down cleanly after the smoke run. These are smoke numbers on an existing dirty data directory, not final performance baselines.

</spec-entry>

<spec-entry category="test" keywords="benchmark,index-topk,desc,topk,current" date="2026-07-09" title="Index Top-K DESC benchmark smoke current code 2026-07-09" description="Current-code small HTTP smoke for DESC indexed Top-K" source="main@8a12c0f">

### Index Top-K DESC benchmark smoke current code 2026-07-09

After adding the reverse capability guard, reran BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 python3 benchmark.py against a local FusionDB HTTP server. Report benchmark_report_small_http_matrix_index_topk.json: loaded 2000 rows in 681 ms; TopK DESC index order averaged 72.7 ms for 20 rows; TopK DESC covering payload averaged 72.6 ms for 20 rows; TopK index order ASC averaged 59.1 ms; TopK covering payload averaged 58.0 ms. The local server was shut down cleanly. This supersedes earlier smoke numbers from before the capability guard and is still not a final clean-data baseline.

</spec-entry>

<spec-entry category="test" keywords="desc,topk,fusion,mvcc,secondary-index,include" date="2026-07-09" title="Fusion SQL DESC Top-K MVCC verification 2026-07-09" description="FusionStorage SQL DESC Top-K MVCC regression coverage" source="main@8a12c0f">

### Fusion SQL DESC Top-K MVCC verification 2026-07-09

Added FusionStorage-backed SQL regressions for secondary BTree ORDER BY score DESC LIMIT over MVCC index versions. Non-covering test flushes base index entries, then DELETEs the highest score, UPDATEs an indexed score, INSERTs replacement rows, flushes again, and asserts EXPLAIN uses ordered secondary BTree DESC while LIMIT counts only visible rows. Covering INCLUDE test updates indexed score and included payload, deletes a high-score row, corrupts base payload columns, and asserts DESC Top-K returns visible payload from the index. Verified with cargo test -q --test sql_index_cache fusion_secondary_btree_desc_topk -- --nocapture, cargo test -q --test sql_index_cache secondary_btree -- --nocapture, cargo check -q, cargo test -q, and git diff --check.

</spec-entry>

<spec-entry category="test" keywords="topk,order-by,alias,residual,secondary-index,composite" date="2026-07-09" title="Ordered index Top-K alias and residual verification 2026-07-09" description="Alias-safe ordered index Top-K and residual predicate regression coverage" source="main@8a12c0f">

### Ordered index Top-K alias and residual verification 2026-07-09

Fixed a SQL correctness bug where range/BETWEEN ordered secondary-index scans could trust raw ORDER BY identifiers even when the identifier resolved to a projection alias. The query layer now passes only alias-safe streaming_order_limit as the schema/index ordered_limit, and scan_single_table derives schema_order_by from that gate before primary-key, secondary BTree, and composite ordered index paths can mark rows_satisfy_order_by. Added regressions for projection alias range ORDER BY/LIMIT, single-column residual WHERE filters, and composite ordered candidates with residual filters. Verified with cargo test -q --test sql_index_cache projection_alias -- --nocapture, cargo test -q --test sql_index_cache residual -- --nocapture, cargo test -q --test sql_index_cache secondary_btree -- --nocapture, cargo test -q --test sql_ddl explain_order_by_secondary -- --nocapture, cargo check -q, cargo test -q, and git diff --check.

</spec-entry>

<spec-entry category="test" keywords="distinct,count-distinct,index,test,explain,benchmark" date="2026-07-09" title="Secondary BTree DISTINCT key stream verification 2026-07-09" description="Verification for index-key DISTINCT and COUNT DISTINCT slice" source="main@8a12c0f">

### Secondary BTree DISTINCT key stream verification 2026-07-09

Verified the DISTINCT key-stream slice with cargo test -q --test sql_distinct_stream -- --nocapture, cargo test -q --test sql_ddl explain_distinct_secondary -- --nocapture, cargo test -q --test sql_ddl explain_order_by_secondary -- --nocapture, cargo check -q, python3 -m py_compile benchmark.py, cargo test -q, and git diff --check. Coverage includes COUNT(DISTINCT bucket) over a nullable indexed column with corrupted base-row bucket bytes, SELECT DISTINCT bucket over NOT NULL indexed INTEGER with corrupted base-row bucket bytes, EXPLAIN positive paths for SELECT DISTINCT and COUNT(DISTINCT), and fallback EXPLAIN for unindexed payload and WHERE filtering. The implemented benchmark hook is syntax-checked but not performance-run in this verification pass.

</spec-entry>

<spec-entry category="test" keywords="groupby,count,index,test,explain,benchmark" date="2026-07-09" title="Secondary BTree GROUP BY COUNT key stream verification 2026-07-09" description="Verification for index-key GROUP BY COUNT slice" source="main@8a12c0f">

### Secondary BTree GROUP BY COUNT key stream verification 2026-07-09

Verified GROUP BY COUNT key-stream with cargo test -q --test sql_group_aggregate group_by_count_index -- --nocapture, cargo test -q --test sql_group_aggregate nullable_index_fallback -- --nocapture, cargo test -q --test sql_group_aggregate -- --nocapture, cargo test -q --test sql_ddl explain_group_by_secondary -- --nocapture, cargo check -q, python3 -m py_compile benchmark.py, git diff --check, and cargo test -q. Coverage includes NOT NULL INTEGER indexed group key with corrupted base-row group column proving index-only execution, TEXT indexed group key containing ':' to verify value_key parsing, nullable indexed fallback preserving the NULL group, UPDATE/DELETE index maintenance visibility for key-stream grouping, and EXPLAIN positive/fallback assertions. Delegate cdx-035258-360e reviewed the implementation and noted the remaining O(N index entries) materialization via scan_prefix_parallel plus future need for streaming prefix visitors and NDV-aware EXPLAIN estimates.

</spec-entry>

<spec-entry category="test" keywords="test,distinct,count-distinct,groupby,index,visitor,fallback" date="2026-07-09" title="Streaming index key path verification 2026-07-09" description="Verification record for streaming secondary index key paths and malformed-key fallback tests" source="main@streaming-prefix-2026-07-09">

### Streaming index key path verification 2026-07-09

Verified streaming index key paths with cargo test -q --test sql_distinct_stream -- --nocapture, cargo test -q --test sql_group_aggregate -- --nocapture, cargo test -q --test sql_ddl explain_group_by_secondary -- --nocapture, cargo check -q, python3 -m py_compile benchmark.py, git diff --check, and cargo test -q. Added malformed secondary index key fallback coverage for COUNT(DISTINCT) and GROUP BY COUNT to ensure visitor early-stop state returns Ok(None) and does not expose partial count/group rows. Delegate cdx-040200-b931 reviewed visitor semantics and noted the remaining limits: tight O(N index-entry) scan, no SSTable block I/O reduction, and future need for loose/skip-to-next-distinct-key support plus allocation/RSS benchmarking.

</spec-entry>

<spec-entry category="test" keywords="test,distinct,count-distinct,index,loose-scan,benchmark,colon" date="2026-07-09" title="Secondary BTree DISTINCT loose seek verification 2026-07-09" description="Verification record for first()-based DISTINCT loose key seek and TEXT fallback guard" source="main@loose-distinct-2026-07-09">

### Secondary BTree DISTINCT loose seek verification 2026-07-09

Verified loose DISTINCT/COUNT DISTINCT with cargo test -q --test sql_distinct_stream count_distinct -- --nocapture, cargo test -q --test sql_distinct_stream -- --nocapture, cargo test -q --test sql_ddl explain_distinct_secondary -- --nocapture, cargo test -q --test sql_group_aggregate group_by_count_index -- --nocapture, cargo check -q, python3 -m py_compile benchmark.py, git diff --check, and cargo test -q. Added TEXT colon-prefix regression for COUNT(DISTINCT label) over a secondary index with values a:b and a:b:c plus corrupted base-row label bytes, proving TEXT remains on safe tight key-stream fallback and still avoids base-row decode. Ran small HTTP BENCH_MATRIX=index_distinct smoke on a temporary server: 60,000 rows loaded in 7,901 ms; Distinct full scan baseline avg 39.2 ms vs Distinct loose key seek 2.2 ms; Count distinct full scan baseline 39.2 ms vs Count distinct loose key seek 2.7 ms; nullable count distinct loose key seek 1.9 ms. GROUP BY rows in the standard benchmark remain cache-contaminated by grouped aggregate query-result cache and should be measured with cache-busting for scan-performance claims.

</spec-entry>

<spec-entry category="test" keywords="metrics,index,distinct,groupby,benchmark" date="2026-07-09" title="Secondary index key scan metrics verification 2026-07-09" description="Validated secondary-index scan counters and benchmark guardrails" source="main@8a12c0f">

### Secondary index key scan metrics verification 2026-07-09

Verification for secondary-index scan counters: cargo fmt, python3 -m py_compile benchmark.py, cargo test -q server::http_server::tests::http_metrics_include_pg_connection_pool_fields -- --nocapture, cargo test -q --test sql_distinct_stream -- --nocapture, cargo test -q --test sql_group_aggregate group_by_count_index -- --nocapture, cargo test -q, and git diff --check all passed. Small HTTP benchmark BENCH_SCALE=small BENCH_MATRIX=index_distinct on port 18291 produced report metadata showing loose DISTINCT/COUNT paths at 505 seeks and 500 values across 5 measured queries, with zero key-stream visits; GROUP BY COUNT index key-stream showed 100000 index_key_stream_entry_visits across 5 measured queries and zero loose counters. Part 21 GROUP BY query text varies per iteration to avoid warmup result-cache contamination; DISTINCT ORDER LIMIT metadata marks loose_scan_bounded=false because LIMIT trims after the loose scan.

</spec-entry>

<spec-entry category="test" keywords="groupby,index,stats,test,metrics" date="2026-07-09" title="Stats-aware GROUP BY COUNT gate verification 2026-07-09" description="Validated stats-aware GROUP BY COUNT index gate and fallback behavior" source="main@8a12c0f">

### Stats-aware GROUP BY COUNT gate verification 2026-07-09

Verified the analyzed-table GROUP BY COUNT cost gate with cargo test -q execution::query::column_scan::tests::group_by_count_index_key_scan_stats_gate -- --nocapture, cargo test -q --test sql_group_aggregate analyzed_small_table_prefers_full_scan -- --nocapture, cargo test -q --test sql_ddl explain_group_by_secondary -- --nocapture, cargo test -q --test sql_group_aggregate group_by_count_index -- --nocapture, cargo check -q, python3 -m py_compile benchmark.py, git diff --check, and cargo test -q. Coverage includes pure stats threshold/fail-open logic, analyzed small table fallback with index_key_stream_entry_visit_count == 0, EXPLAIN fallback after ANALYZE on a small table, and existing no-stats corrupted-base-row index-only GROUP BY COUNT behavior.

</spec-entry>

<spec-entry category="test" keywords="groupby,count,summary,tests,metrics" date="2026-07-09" title="GROUP BY COUNT summary tests" description="Stable test coverage for maintained GROUP BY COUNT summary index" source="main@8a12c0f">

### GROUP BY COUNT summary tests

Tests for GROUP BY COUNT summary should avoid exact assertions on GLOBAL_METRICS in parallel Rust tests; use storage-level summary key assertions and query results instead. Cover backfill with corrupted base group columns, analyzed small tables using summary, missing marker legacy fallback, malformed count fallback, incomplete summary meta mismatch fallback, TEXT value keys containing ':', nullable column full-row fallback, DML insert/update/delete maintenance, DROP INDEX cleanup, and TRUNCATE reseeding empty summary metadata.

</spec-entry>

<spec-entry category="test" keywords="groupby,count,summary,benchmark,part21" date="2026-07-09" title="GROUP BY COUNT summary Part 21 smoke 2026-07-09" description="Small HTTP smoke for maintained GROUP BY COUNT summary index" source="main@8a12c0f">

### GROUP BY COUNT summary Part 21 smoke 2026-07-09

Run FusionDB from a clean temporary cwd to avoid recovering the repo-root 698 MB data directory: rm -rf /tmp/fusiondb_part21_smoke && mkdir -p /tmp/fusiondb_part21_smoke && (cd /tmp/fusiondb_part21_smoke && /root/FusionDB/target/debug/fusiondb). Then run from repo root: BENCH_PARTS=21 BENCH_SCALE=SMALL BENCH_INDEX_DISTINCT_ROWS=2000 BENCH_INDEX_DISTINCT_NDV=100 BENCH_INDEX_DISTINCT_LIMIT=10 python3 benchmark.py. Current smoke report benchmark_report_small_http_parts_21.json showed Group by count summary index avg 3.62 ms vs full scan baseline 11.058 ms, summary visits per query 100.0 for NDV=100, and key-stream visits per query 0.0.

</spec-entry>

<spec-entry category="test" keywords="startup,recovery,benchmark,sstable,cache" date="2026-07-09" title="Startup recovery cache benchmark 2026-07-09" description="Cold/warm startup measurements after timestamp recovery cache" source="main@8a12c0f">

### Startup recovery cache benchmark 2026-07-09

Repo-root data/sstables contained two 347MB SSTables plus smaller files. Before cache, target/debug/fusiondb from repo root timed out at 20s without listening. After implementing the cache, the first migration startup took 101s and logged Restored SSTable max timestamp 3732 (0 cached, 4 scanned). Subsequent warm startup after cache completion logged (6 cached, 0 scanned), but time-to-HTTP-listen remained about 9s. Marker timing showed start at 118ms and restored_ts at 8822ms, so the remaining warm-start bottleneck is SsTable::open metadata/index/filter deserialization, not timestamp data-block scan, server bind, or vector rebuild. Targeted tests passed: cargo test -q fusion_reopen; cargo test -q fusion_shutdown_does_not_create_empty_sstable; cargo check -q; git diff --check.

</spec-entry>

<spec-entry category="test" keywords="startup,sstable,open,benchmark,parallel" date="2026-07-09" title="Parallel SSTable open startup benchmark 2026-07-09" description="Warm startup improvement from parallel SSTable open" source="main@8a12c0f">

### Parallel SSTable open startup benchmark 2026-07-09

After timestamp recovery cache, repo-root warm startup still spent about 8.8s before Restored SSTable max timestamp because SsTable::open eagerly read/deserialized large metadata: 169.sst and 171.sst each had about 26.9MB index and 57.0MB meta. After parallel startup open plus moving block_properties out of meta instead of cloning, marker startup from target/debug/fusiondb improved: restored_ts/http_listen 4421ms on first measured run and http_listen 4148ms on second run, both with Restored SSTable max timestamp 3732 (6 cached, 0 scanned). Verification: cargo check -q, cargo build -q --bin fusiondb, cargo test -q fusion_reopen, cargo test -q fusion_shutdown_does_not_create_empty_sstable, cargo test -q range_iterator_lower_bound_skips_previous_block_with_block_properties, cargo test -q user_key_range_iterator_skips_block_without_target_table_prefix_property, git diff --check.

</spec-entry>

<spec-entry category="test" keywords="startup,sstable,open,metrics,benchmark" date="2026-07-09" title="SSTable open phase metrics benchmark 2026-07-09" description="Warm startup phase metrics show meta decode dominates" source="main@8a12c0f">

### SSTable open phase metrics benchmark 2026-07-09

Repo-root warm startup after parallel open and timestamp cache listened in 4356ms. /metrics reported sstable_open_count=6, sstable_open_total_us=8535828, index_bytes=53978138, index_read_us=70601, index_decode_us=2923398, filter_bytes=2158332, filter_read_us=14562, filter_decode_us=19695, meta_bytes=114262936, meta_read_us=109458, meta_decode_us=5233266, index_entries=917914, block_property_count=917914, live_sstable_count=6. This proves remaining warm startup is dominated by metadata decode CPU, especially block_properties/meta, not file I/O. Validation commands: cargo check -q; cargo test -q http_metrics_include_pg_connection_pool_fields; cargo test -q fusion_reopen; cargo test -q range_iterator_lower_bound_skips_previous_block_with_block_properties; cargo build -q --bin fusiondb.

</spec-entry>

<spec-entry category="test" keywords="benchmark,startup,sstable,descriptor-cache,index-decode" date="2026-07-09" title="Warm startup descriptor cache benchmark" description="Descriptor cache reduces warm startup to about 1.7-1.9 seconds" source="main@8a12c0f">

### Warm startup descriptor cache benchmark

On the repo-root data set after descriptor cache exists, two controlled starts measured HTTP readiness at 1850 ms and 1689 ms. /metrics reported sstable_open_count=6, sstable_open_meta_bytes=0, sstable_open_meta_read_us=0, sstable_open_meta_decode_us=0, sstable_open_index_bytes=53978138, and sstable_open_index_decode_us about 2.9-3.2 s cumulative. Previous parallel-open warm startup was about 4.1-4.4 s with meta_decode_us about 5.2 s cumulative, so the remaining startup bottleneck is index decode.

</spec-entry>

<spec-entry category="test" keywords="benchmark,startup,sstable,index,decode" date="2026-07-09" title="SSTable direct vector index startup benchmark" description="Direct vector decode reduces warm startup to about 1.3-1.4 seconds" source="main@8a12c0f">

### SSTable direct vector index startup benchmark

After replacing runtime BTreeMap index construction with direct vector decode, repo-root warm startup on the existing legacy SSTables measured HTTP readiness at 1317 ms, 1328 ms, and 1389 ms. /metrics reported sstable_open_index_bytes=53978138, sstable_open_index_entries=917914, sstable_open_index_decode_us=2284005, 2312154, and 2207887 respectively, with sstable_open_meta_decode_us=0 from descriptor cache. Previous descriptor-cache baseline was 1689-1850 ms readiness and about 2.9-3.2 s cumulative index decode.

</spec-entry>

<spec-entry category="test" keywords="benchmark,startup,sstable,index,sidecar" date="2026-07-09" title="SSTable index sidecar startup benchmark" description="Index sidecar cache reduces warm startup to about 0.26-0.30 seconds" source="main@8a12c0f">

### SSTable index sidecar startup benchmark

Repo-root legacy SSTable warm startup before sidecar, after direct vector decode, measured about 1.3-1.4s HTTP readiness with sstable_open_index_decode_us about 2.2s cumulative. First sidecar migration run generated six *.idxcache files, including two 25MB files for the large SSTables, and listened in 1298ms. Subsequent cache-hit runs listened in 267ms, 261ms, and 303ms. Final /metrics showed sstable_index_cache_hit_count=6, miss/stale/invalid/write_error=0, sstable_open_total_us=357759, sstable_open_index_decode_us=227998, sstable_open_meta_decode_us=0, and live_sstable_count=6.

</spec-entry>

<spec-entry category="test" keywords="benchmark,startup,sstable,index,sidecar,ficx" date="2026-07-09" title="SSTable FICX v3 startup benchmark" description="FICX v3 migration and warm-start benchmark evidence" source="main@8a12c0f">

### SSTable FICX v3 startup benchmark

After upgrading index sidecar cache from FICX v2 to v3, the first repo-root startup treated six existing sidecars as stale, rewrote six v3 sidecars, and listened in 1442ms with sstable_open_index_decode_us=2125203, stale_count=6, write_count=6, invalid=0, write_error=0. The next warm startup hit all six v3 sidecars and listened in 258ms with sstable_open_total_us=397450, sstable_open_index_decode_us=300466, hit_count=6, miss/stale/invalid/write/write_error=0, live_sstable_count=6. Verified with cargo check, sidecar unit tests, fusion_reopen, HTTP metrics test, cargo build --bin fusiondb, python3 -m py_compile benchmark.py, and git diff --check.

</spec-entry>

<spec-entry category="test" keywords="benchmark,startup,sstable,index,sidecar,rss" date="2026-07-09" title="SSTable startup index benchmark matrix" description="Part 22 startup/index benchmark matrix" source="main@8a12c0f">

### SSTable startup index benchmark matrix

benchmark.py now registers non-default Part 22 / BENCH_MATRIX=sstable_startup_index. The matrix starts an isolated FusionDB server from a generated fusiondb.toml in a temporary cwd, copies the target BENCH_SST_STARTUP_DATA_DIR into a scenario data dir with SST files hardlinked and cache/sidecar files copied, manipulates only *.idxcache for warm_sidecar/no_sidecar/stale_sidecar/corrupt_sidecar scenarios, then records ready_ms, RSS at readiness, first point/range query latency, live_sstable_count, SSTable open counters, compaction counters, and sidecar hit/miss/stale/invalid/write/write_error counters into the normal benchmark report. It skips the external-server setup path when this is the only selected part.

</spec-entry>

<spec-entry category="test" keywords="benchmark,startup,sstable,index,sidecar,smoke" date="2026-07-09" title="SSTable startup index matrix smoke 2026-07-09" description="Smoke evidence for Part 22 startup/index matrix" source="main@8a12c0f">

### SSTable startup index matrix smoke 2026-07-09

Smoke command: BENCH_SCALE=small BENCH_MATRIX=sstable_startup_index BENCH_SST_STARTUP_TIMEOUT_SEC=45 BENCH_SST_STARTUP_KEEP_WORKDIR=0 python3 benchmark.py. Source repo data was copied into per-scenario temporary dirs. Results: warm_sidecar ready 314.45ms, RSS 127156KiB, live_sstable_count=6, hit/miss/stale/invalid/write=6/0/0/0/0, first point 8.496ms, first range 8.712ms; no_sidecar ready 1550.597ms, RSS 132804KiB, counters 0/6/0/0/6; stale_sidecar ready 1408.464ms, RSS 133100KiB, counters 0/0/6/0/6; corrupt_sidecar ready 1661.791ms, RSS 132420KiB, counters 0/0/0/6/6. All four scenarios had compaction_run_count/input/output=0 and sstable_open_meta_decode_us=0. Report: benchmark_report_small_http_matrix_sstable_startup_index.json. Verified python3 -m py_compile benchmark.py and git diff --check.

</spec-entry>

<spec-entry category="test" keywords="manifest,startup,benchmark,test" date="2026-07-09" title="SSTable manifest smoke verification 2026-07-09" description="Manifest tests and Part 22 startup smoke results" source="main@8a12c0f">

### SSTable manifest smoke verification 2026-07-09

Verified the MANIFEST/CURRENT skeleton with cargo fmt --check, cargo check -q, targeted tests fusion_reopen_uses_manifest_live_sstable_list, fusion_compaction_updates_manifest_live_sstable_list, fusion_reopen_fails_when_manifest_references_missing_sstable, fusion_reopen_persists_sstable_timestamp_cache, fusion_shutdown_does_not_create_empty_sstable, and fusion_compaction_defers_obsolete_sstable_delete_until_readers_drop. Part 22 startup smoke after the change: warm_sidecar ready 372.3ms RSS 125808KiB live=6 hit/miss/stale/invalid/write=6/0/0/0/0 first point 12.0ms range 5.6ms; no_sidecar 1291.9ms; stale_sidecar 1348.8ms; corrupt_sidecar 1396.2ms.

</spec-entry>

<spec-entry category="test" keywords="manifest,benchmark,orphan,startup,test" date="2026-07-09" title="SSTable manifest benchmark gate 2026-07-09" description="Part 22 manifest/orphan startup benchmark gate" source="main@8a12c0f">

### SSTable manifest benchmark gate 2026-07-09

Part 22 sstable_startup_index now includes warm_manifest and orphan_manifest scenarios by default. benchmark.py can write a valid MANIFEST-000001/CURRENT into copied startup workdirs, seeding live files from descriptor cache when present, then create orphan numeric SSTables outside the manifest. Gates assert manifest scenarios load manifest, do not use legacy directory scan, live_sstable_count equals manifest live file count, and opened SSTables equal manifest live file count. Smoke on 2026-07-09: warm_manifest ready 260.2ms, manifest load/live legacy/candidates 1/6 0/0; orphan_manifest with 64 orphan SSTables ready 261.3ms, same 1/6 0/0; full default Part 22 still passed sidecar scenarios.

</spec-entry>

<spec-entry category="test" keywords="dirty_wal,wal,replay,benchmark,startup" date="2026-07-09" title="Dirty WAL startup benchmark gate 2026-07-09" description="Dirty WAL manifest startup benchmark gate and smoke result" source="main@8a12c0f">

### Dirty WAL startup benchmark gate 2026-07-09

Part 22 startup/index matrix includes dirty_wal_manifest, a deterministic low-level WAL fixture that writes raw Fusion WAL Put records before startup. The gate requires manifest load/live counts to match the seeded manifest, legacy scan counters to stay zero, wal_replay_entry_count to cover dirty_wal_entries_written, and wal_replay_apply_count to be positive. Current smoke evidence: BENCH_MATRIX=sstable_startup_index BENCH_SST_STARTUP_SCENARIOS=dirty_wal_manifest BENCH_SST_STARTUP_DIRTY_WAL_ENTRIES=1000 python3 benchmark.py completed with ready_ms 263.526, wal_replay_entry_count 1000, wal_replay_bytes 55890, wal_replay_apply_count 1, wal_replay_max_ts 9000000999, and no WAL replay errors.

</spec-entry>

<spec-entry category="test" keywords="manifest,wal,crash,recovery,test" date="2026-07-09" title="Manifest WAL crash consistency matrix" description="Deterministic crash consistency matrix for manifest and WAL recovery" source="main@8a12c0f">

### Manifest WAL crash consistency matrix

Research agent recommends deterministic recovery fixtures before random crash tests. P0 matrix: CURRENT must choose only its MANIFEST live set even with orphan MANIFEST/SST files; missing manifest-referenced SST must fail; orphan SST outside MANIFEST must be ignored; append-only MANIFEST may recover only complete tail records and must fail on middle corruption; multi-WAL replay must be ordered and idempotent; latest WAL partial tail may truncate to last complete record; older WAL corrupt with later WAL must fail or stop at prefix, never skip over a hole. P1: missing required WAL and WAL checksum/middle corruption must not silently lose data; CURRENT rename/fsync windows must allow only old or new consistent state; parent directory fsync windows must be modeled for MANIFEST, CURRENT, WAL, and SST creation.

</spec-entry>

<spec-entry category="test" keywords="manifest,record,crc32c,test,recovery" date="2026-07-09" title="Manifest record framing verification 2026-07-09" description="Manifest record physical log targeted verification" source="main@8a12c0f">

### Manifest record framing verification 2026-07-09

Validated manifest_record with cargo fmt --check, cargo check -q, and cargo test -q manifest_record. Coverage includes CRC32C Castagnoli standard vector, small and empty FULL round trips, large record fragmentation across multiple blocks, partial tail header prefix recovery, partial tail payload prefix recovery, Strict-mode torn payload failure, middle checksum corruption failure, invalid MIDDLE without FIRST failure, 1..6 byte trailer zero padding, exact 7-byte remaining zero-length FIRST behavior, and append-mode existing_len block offset preservation.

</spec-entry>

<spec-entry category="test" keywords="manifest,edit,test,replay,recovery" date="2026-07-09" title="Manifest edit schema verification 2026-07-09" description="Manifest v2 logical edit schema targeted verification" source="main@8a12c0f">

### Manifest edit schema verification 2026-07-09

Validated manifest_edit with cargo fmt --check, cargo check -q, cargo test -q manifest_edit, and cargo test -q manifest_record. manifest_edit coverage includes round-trip encoding for Snapshot/AddSstable/DeleteSstable/Compact/SetNextFileNumber/SetHighWatermark/SetWalReplayFloor, record-layer replay into ManifestVersionState, prefix recovery on torn tail through manifest_record, rejection of unsupported version and unknown tag, truncated and trailing payload rejection, duplicate add and missing delete rejection, next_file_number/high_watermark/WAL floor regression rejection, snapshot duplicate id and low next_file_number rejection, and high_watermark below live SSTable max_ts rejection.

</spec-entry>

<spec-entry category="test" keywords="manifest,current,test,recovery" date="2026-07-09" title="Manifest log CURRENT verification 2026-07-09" description="Manifest log targeted verification results" source="main@8a12c0f">

### Manifest log CURRENT verification 2026-07-09

Verified manifest_log with cargo test -q manifest_log: file-name parsing and path rejection, manifest creation plus CURRENT replay, append replay and offset accounting, torn-tail prefix recovery, missing/bad CURRENT target rejection, non-Snapshot manifest creation rejection, and CRLF CURRENT compatibility. Also reran cargo test -q manifest_edit, cargo test -q manifest_record, cargo fmt --check, and cargo check -q.

</spec-entry>

<spec-entry category="test" keywords="manifest,edit,test,validation" date="2026-07-09" title="Manifest edit canonical entry verification 2026-07-09" description="Targeted verification for manifest v2 SSTable entry validation" source="main@8a12c0f">

### Manifest edit canonical entry verification 2026-07-09

Verified canonical SSTable entry hardening with cargo test -q manifest_edit, cargo test -q manifest_log, cargo test -q manifest_record, cargo check -q, and cargo fmt --check. New manifest_edit coverage rejects non-canonical SSTable file names during encode/apply, rejects invalid raw disk payloads during decode, and rejects invalid records during replay.

</spec-entry>

<spec-entry category="test" keywords="manifest,current,torn-tail,test,crash" date="2026-07-09" title="Manifest crash-window verification 2026-07-09" description="P0 manifest crash-window tests and verification" source="main@8a12c0f">

### Manifest crash-window verification 2026-07-09

Verified manifest crash-window hardening with cargo test -q manifest_log, cargo test -q manifest_record, cargo test -q manifest_edit, cargo check -q, and cargo fmt --check. New coverage includes CURRENT preferring the pointed manifest over a newer orphan MANIFEST, ignoring CURRENT.tmp/unique CURRENT temp remnants, replay_current_manifest failing on middle checksum corruption, append after torn-tail recovery truncating to valid_bytes before appending, canonical MANIFEST name rejection, and record-layer failure when a dangling FIRST fragment is followed by a later FULL record.

</spec-entry>

<spec-entry category="test" keywords="manifest,edit,test,prefix,crash" date="2026-07-09" title="Manifest edit prefix legality verification 2026-07-09" description="Targeted verification for manifest edit prefix legality" source="main@8a12c0f">

### Manifest edit prefix legality verification 2026-07-09

Verified prefix-legality hardening with cargo test -q manifest_edit, cargo test -q manifest_log, cargo test -q manifest_record, cargo check -q, and cargo fmt --check. manifest_edit now has 12 tests, including replay rejection for the crash-sensitive sequence Snapshot(empty, high_watermark=0), AddSstable(max_ts=10), SetHighWatermark(10); the Add prefix fails immediately instead of relying on a later edit to repair state. Tests also verify failed AddSstable below high_watermark rolls back state.

</spec-entry>

<spec-entry category="test" keywords="manifest,versionedit,test,crash" date="2026-07-09" title="Manifest composite VersionEdit verification 2026-07-09" description="Targeted verification for composite manifest VersionEdit" source="main@8a12c0f">

### Manifest composite VersionEdit verification 2026-07-09

Verified composite VersionEdit with cargo test -q manifest_edit, cargo test -q manifest_log, cargo test -q manifest_record, cargo check -q, and cargo fmt --check. manifest_edit now has 14 tests. New coverage round-trips VersionEdit, replays a VersionEdit that atomically deletes files, adds a file, advances next_file_number, high_watermark, and wal_replay_floor, and verifies failed VersionEdit application rolls back state on invariant failure.

</spec-entry>

<spec-entry category="test" keywords="manifest,current,rollover,test,recovery" date="2026-07-09" title="Manifest current rollover verification 2026-07-09" description="Targeted verification for CURRENT manifest rollover" source="main@8a12c0f">

### Manifest current rollover verification 2026-07-09

Verified recovered-tail rollover with cargo test -q manifest_log, cargo test -q manifest_edit, cargo test -q manifest_record, cargo check -q, and cargo fmt --check. manifest_log now has 14 tests. New coverage checks that clean CURRENT replay does not roll over, torn-tail CURRENT manifest rolls over to a new snapshot and installs CURRENT, and rollover skips an existing orphan MANIFEST-N while preserving the live set from the old CURRENT valid prefix.

</spec-entry>

<spec-entry category="test" keywords="manifest,replay,test,snapshot,current" date="2026-07-09" title="Manifest log snapshot-first replay regression" description="Regression tests for empty and non-Snapshot MANIFEST startup replay" source="main@8a12c0f">

### Manifest log snapshot-first replay regression

Coverage should include replay_current_manifest rejecting CURRENT targets whose MANIFEST is empty, plus MANIFEST files whose first logical edit is not Snapshot. Keep existing recovery tests for torn tail rollover, append tail repair, and orphan manifest skipping green after this constraint.

</spec-entry>

<spec-entry category="test" keywords="manifest,fusionstorage,test,startup,legacy" date="2026-07-09" title="FusionStorage manifest v2 integration verification" description="Validation for v2 Snapshot/CURRENT integration plus legacy JSON fallback" source="main@8a12c0f">

### FusionStorage manifest v2 integration verification

Verified manifest v2 FusionStorage integration with cargo fmt --check, cargo check -q, cargo test -q --lib, cargo test -q manifest_log, cargo test -q manifest_edit, cargo test -q manifest_record, and targeted tests fusion_reopen_uses_manifest_live_sstable_list, fusion_reopen_accepts_legacy_json_sstable_manifest, fusion_compaction_updates_manifest_live_sstable_list, and fusion_reopen_fails_when_manifest_references_missing_sstable.

</spec-entry>

<spec-entry category="test" keywords="manifest,startup,descriptor,test,performance" date="2026-07-09" title="Manifest v2 descriptor startup verification" description="Regression coverage for descriptor-cache-independent v2 manifest startup" source="main@8a12c0f">

### Manifest v2 descriptor startup verification

fusion_reopen_uses_manifest_live_sstable_list now removes the derived descriptor cache before reopen and asserts it is not recreated, proving startup can open v2-manifest live SSTables from descriptors embedded in MANIFEST. Revalidated with cargo fmt --check, cargo check -q, targeted manifest/FusionStorage tests, and cargo test -q --lib (467 passed).

</spec-entry>

<spec-entry category="test" keywords="manifest,v2,benchmark,startup,gate" date="2026-07-09" title="Manifest v2 startup benchmark gate roadmap" description="Benchmark roadmap for real v2 MANIFEST startup fixtures and metrics" source="main@8a12c0f">

### Manifest v2 startup benchmark gate roadmap

Part 22 benchmark manifest scenarios should gain true v2 fixtures instead of hand-written legacy JSON: v2_manifest, v2_orphan_manifest, v2_torn_tail_rollover, v2_many_edits, and v2_dirty_wal_floor. Gates should assert manifest load count 1, legacy scan 0, load/open errors 0, live/open counts match, compaction noise 0, and track v2 replay records/valid bytes/recovered tail/rollover plus ready_ms, RSS, and first-query penalty.

</spec-entry>

<spec-entry category="test" keywords="manifest,versionedit,test,fusionstorage" date="2026-07-09" title="FusionStorage append-only manifest verification" description="Test evidence for FusionStorage append-only manifest integration" source="main@8a12c0f">

### FusionStorage append-only manifest verification

Verified append-only VersionEdit integration with cargo fmt --check, cargo check -q, cargo test -q manifest_record, cargo test -q manifest_edit, cargo test -q manifest_log, cargo test -q fusion_flush_appends_manifest_version_edits_to_existing_manifest, cargo test -q fusion_reopen_accepts_legacy_json_sstable_manifest, cargo test -q fusion_compaction_updates_manifest_live_sstable_list, cargo test -q fusion_flush_candidate_remains_visible_until_sstable_registration, and cargo test -q --lib -- --test-threads=1. Final full library result was 468 passed, 0 failed.

</spec-entry>

<spec-entry category="test" keywords="manifest,v2,benchmark,startup,versionedit" date="2026-07-09" title="Manifest v2 startup benchmark gate" description="Part 22 now covers real v2 MANIFEST startup fixtures" source="main@8a12c0f">

### Manifest v2 startup benchmark gate

Part 22 sstable_startup_index now has true v2 MANIFEST fixture scenarios in benchmark.py: v2_manifest, v2_orphan_manifest, v2_many_edits, and explicit v2_torn_tail_rollover. benchmark.py writes FMED v2 manifest edit payloads using LevelDB-style 32KiB physical records with CRC32C masking, sourcing first_key/last_key/format_version from the SSTable descriptor cache and max_ts from the timestamp cache. Gates assert manifest_load_count == 1, legacy_scan_count == 0, load/open errors == 0, live/open counts match, compaction_run_count == 0, v2_many_edits writes VersionEdit records, and torn-tail rollover installs MANIFEST-000002. Verified with python3 -m py_compile benchmark.py, cargo check -q, git diff --check -- benchmark.py, BENCH_SCALE=small BENCH_MATRIX=sstable_startup_index BENCH_SST_STARTUP_SCENARIOS=v2_manifest BENCH_SST_STARTUP_TIMEOUT_SEC=45 python3 benchmark.py, BENCH_SCALE=small BENCH_MATRIX=sstable_startup_index BENCH_SST_STARTUP_SCENARIOS=v2_orphan_manifest,v2_many_edits,v2_torn_tail_rollover BENCH_SST_STARTUP_TIMEOUT_SEC=45 python3 benchmark.py, and default Part 22 small startup matrix.

</spec-entry>

<spec-entry category="test" keywords="sstable,point-get,overlap,test,metrics" date="2026-07-09" title="Point-get SSTable overlap skip verification" description="Verification evidence for point-get SSTable overlap skip" source="main@8a12c0f">

### Point-get SSTable overlap skip verification

Verified point-get user-key overlap skip with cargo fmt --check, cargo check -q, python3 -m py_compile benchmark.py, git diff --check -- src/storage/fusion.rs src/monitor.rs src/server/http_server.rs benchmark.py, cargo test -q point_get_skips_sstable_before_bloom_when_user_key_outside_file_range, cargo test -q point_get_uses_sstable_user_key_bloom_to_skip_absent_key, cargo test -q http_metrics_include_pg_connection_pool_fields, cargo test -q fusion_reopen_uses_manifest_live_sstable_list, and cargo test -q --lib -- --test-threads=1. Final library result: 469 passed, 0 failed. The overlap test asserts skipped SSTables do not count as point probes, do not reach user-key Bloom, and do not read cached blocks; the Bloom test keeps an in-range absent key to verify Bloom still handles non-overlap cases.

</spec-entry>

<spec-entry category="test" keywords="composite-index,topk,desc,test,reverse" date="2026-07-09" title="Composite DESC bounded reverse verification" description="Test evidence for composite DESC bounded reverse scan" source="main@8a12c0f">

### Composite DESC bounded reverse verification

Verified composite DESC bounded reverse scan with cargo fmt --check, cargo check -q, cargo test -q composite_desc_ordered_scan_uses_bounded_reverse_range, cargo test -q composite_desc_ordered_scan_requires_bounded_reverse_capability, cargo test -q --test sql_index_cache composite_index_ -- --nocapture, cargo test -q --test sql_index_cache -- --nocapture, and cargo test -q --lib -- --test-threads=1. Final results: sql_index_cache 57 passed; lib 471 passed. New unit coverage uses a recording Transaction to assert DESC composite ORDER BY ts LIMIT 2 calls scan_range_reverse with limit Some(2), returns ordered row ids from the high end, and refuses ordered_row_ids when bounded reverse capability is absent.

</spec-entry>

<spec-entry category="test" keywords="composite-index,topk,explain,test,desc" date="2026-07-09" title="Composite ordered Top-K EXPLAIN verification" description="Tests for composite ordered Top-K EXPLAIN" source="main@8a12c0f">

### Composite ordered Top-K EXPLAIN verification

Verified composite ordered BTree EXPLAIN visibility with cargo fmt --check, cargo check -q, cargo test -q --test sql_ddl explain_order_by_composite -- --nocapture, cargo test -q --test sql_ddl explain_order_by_secondary -- --nocapture, cargo test -q composite_desc_ordered_scan -- --nocapture, cargo test -q --test sql_ddl -- --nocapture, cargo test -q --lib -- --test-threads=1, and git diff --check. Coverage includes ASC and DESC range Top-K on (host_id, ts) plus a residual predicate fallback that must not report ordered composite BTree.

</spec-entry>

<spec-entry category="test" keywords="benchmark,topk,composite-index,smoke,performance" date="2026-07-09" title="Composite Top-K benchmark smoke 2026-07-09" description="Small smoke evidence for composite Top-K benchmark" source="main@8a12c0f">

### Composite Top-K benchmark smoke 2026-07-09

Verified benchmark.py composite Top-K matrix with python3 -m py_compile benchmark.py, git diff --check -- benchmark.py, and BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 python3 benchmark.py against a temporary /tmp FusionDB server. Report benchmark_report_small_http_matrix_index_topk.json loaded 3000 rows and ran 21 Part 20 cases. Composite EXPLAIN metadata showed ordered=true for composite ASC, DESC, range DESC, and window DESC; ordered=false for full scan, residual fallback, no-prefix fallback, and mixed-order fallback. Smoke result exposed the next optimization target: composite ASC averaged 2.4ms while composite DESC/range/window/residual were about 121-127ms on this debug small run despite bounded EXPLAIN paths.

</spec-entry>

<spec-entry category="test" keywords="topk,metrics,test,benchmark,verification" date="2026-07-09" title="Ordered Top-K metrics verification 2026-07-09" description="Validation coverage for ordered Top-K metrics" source="main@2026-07-09">

### Ordered Top-K metrics verification 2026-07-09

Verified ordered Top-K metrics with cargo fmt --check, cargo build -q --bin fusiondb, cargo check -q, cargo test -q --test sql_index_cache ordered_topk_metrics -- --nocapture, cargo test -q --test sql_index_cache secondary_btree_order_by_limit -- --nocapture, cargo test -q composite_desc_ordered_scan -- --nocapture, cargo test -q --test sql_index_cache -- --nocapture, cargo test -q --lib -- --test-threads=1, cargo test -q fusion_shutdown_does_not_create_empty_sstable -- --nocapture, python3 -m py_compile benchmark.py, and git diff --check. Small HTTP index_topk smoke after the counters showed per-query counter deltas as expected: ASC/DESC index paths had one ordered Top-K scan and about LIMIT entry visits per query, DESC also had one reverse scan per query, and expression or mixed-order fallback had one query sort fallback per query.

</spec-entry>

<spec-entry category="test" keywords="test,benchmark,fusion,topk,reverse" date="2026-07-09" title="Fusion lazy reverse and flush race verification 2026-07-09" description="Validation for lazy reverse source and flush race fix" source="main@2026-07-09">

### Fusion lazy reverse and flush race verification 2026-07-09

Verified the lazy Fusion reverse source and flush_lock race fix with cargo check -q, cargo fmt --check, git diff --check, cargo build -q --bin fusiondb, cargo test -q storage::fusion::tests::fusion_scan_range_reverse -- --nocapture, cargo test -q storage::memory::tests::test_scan_range_reverse -- --nocapture, cargo test -q --test sql_index_cache ordered_topk_metrics -- --nocapture, cargo test -q --test sql_index_cache secondary_btree_order_by_limit -- --nocapture, cargo test -q composite_desc_ordered_scan -- --nocapture, concurrent cargo test -q storage::fusion::tests -- --nocapture plus cargo test -q --test sql_index_cache -- --nocapture, and cargo test -q --lib -- --test-threads=1. Current small HTTP index_topk smoke with BENCH_INDEX_TOPK_ROWS=500 and LIMIT=20 loaded 3000 rows in 554ms; TopK DESC index order averaged 11.292ms with scans/q=1, visits/q=20, reverse/q=1, sort/q=0; composite DESC/range/window averaged about 18.4-18.9ms with the same bounded counters.

</spec-entry>

<spec-entry category="test" keywords="test,benchmark,topk,visitor,range" date="2026-07-09" title="Range visitor Top-K verification 2026-07-09" description="Validation for range visitor Top-K integration" source="main@2026-07-09">

### Range visitor Top-K verification 2026-07-09

Verified Transaction range visitor integration with cargo check -q, cargo fmt --check, git diff --check, cargo build -q --bin fusiondb, cargo test -q storage::memory::tests::test_scan_range_reverse -- --nocapture, cargo test -q storage::fusion::tests::fusion_scan_range_for_each -- --nocapture, cargo test -q --test sql_index_cache ordered_topk_metrics -- --nocapture, cargo test -q --test sql_index_cache secondary_btree_order_by_limit -- --nocapture, cargo test -q composite_desc_ordered_scan -- --nocapture, cargo test -q --test sql_index_cache composite_index_ -- --nocapture, cargo test -q --test sql_index_cache -- --nocapture, cargo test -q storage::fusion::tests -- --nocapture, and cargo test -q --lib -- --test-threads=1. Small HTTP BENCH_MATRIX=index_topk smoke loaded 3000 rows in 558ms; TopK index order ASC averaged 1.969ms with scans/q=1 visits/q=20 sort/q=0; DESC averaged 12.441ms with reverse/q=1; composite DESC/range/window averaged 18.959/20.911/23.007ms with visits/q=20 and sort/q=0.

</spec-entry>

<spec-entry category="test" keywords="test,benchmark,topk,reverse,metrics" date="2026-07-09" title="Fusion reverse raw metrics verification 2026-07-09" description="Verification for raw reverse Top-K counters" source="main@8a12c0f">

### Fusion reverse raw metrics verification 2026-07-09

Verified raw reverse metrics with cargo fmt --check, python3 -m py_compile benchmark.py, git diff --check, cargo check -q, cargo build -q --bin fusiondb, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_records_raw_sstable_work_counters -- --nocapture, cargo test -q server::http_server::tests::http_metrics_include_pg_connection_pool_fields -- --nocapture, cargo test -q storage::fusion::tests -- --nocapture, cargo test -q --test sql_index_cache -- --nocapture, and cargo test -q --lib -- --test-threads=1. Small HTTP BENCH_MATRIX=index_topk smoke loaded 3000 rows; DESC positive paths showed scans/q=1 visits/q=20 reverse/q=1 sort/q=0 and Fusion raw/visible counters around raw=22 candidates=21 puts=20 per query. SSTable reverse iterator counters are covered by the storage regression; the smoke data remained memtable-hot so sstable reverse iterator/block counters were zero there.

</spec-entry>

<spec-entry category="test" keywords="test,benchmark,claim-mode,topk" date="2026-07-09" title="Part 20 claim-mode verification 2026-07-09" description="Verification for Part 20 claim mode" source="main@8a12c0f">

### Part 20 claim-mode verification 2026-07-09

Verified BENCH_CLAIM_MODE with python3 -m py_compile benchmark.py, git diff --check -- benchmark.py, and BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 python3 benchmark.py against a clean temporary FusionDB server. The run loaded 3000 rows, returned exit code 0, printed BENCH_CLAIM_MODE passed, and report benchmark_report_small_http_matrix_index_topk.json recorded claim_mode=true with 21/21 Part 20 cases claim_status=passed. Representative counters: ASC scans/q=1 visits/q=20 reverse/q=0 sort/q=0; DESC scans/q=1 visits/q=20 reverse/q=1 sort/q=0 raw/q=22 puts/q=20; mixed-order fallback scans/q=0 visits/q=0 reverse/q=0 sort/q=1.

</spec-entry>

<spec-entry category="test" keywords="test,cache,metrics,claim-mode,benchmark" date="2026-07-09" title="Query-result cache metrics verification 2026-07-09" description="Verification for query-result cache counters and Part 20 gate" source="main@8a12c0f">

### Query-result cache metrics verification 2026-07-09

Verified query-result cache observability with cargo fmt --check, python3 -m py_compile benchmark.py, git diff --check on touched files, cargo check -q, cargo test -q server::http_server::tests::http_metrics_include_pg_connection_pool_fields -- --nocapture, cargo test -q --test sql_join test_join_group_by_aggregate_result_cache_invalidates_after_insert -- --nocapture, cargo test -q --test pg_integration test_pg_grouped_aggregate_cache_consistent_and_invalidated_by_writes -- --nocapture, cargo test -q --test sql_join -- --nocapture, cargo test -q --lib -- --test-threads=1, cargo build -q --bin fusiondb, and BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 python3 benchmark.py. Final report had claim_mode=true, 21/21 Part 20 cases claim_status=passed, and cache_nonzero=[] for eligible/hit/miss/stale/insert/invalidation metadata.

</spec-entry>

<spec-entry category="test" keywords="benchmark,disclosure,claim-mode,verification,topk" date="2026-07-09" title="Benchmark disclosure verification 2026-07-09" description="Verification results for disclosure report fields" source="main@8a12c0f">

### Benchmark disclosure verification 2026-07-09

Verified benchmark.py disclosure changes with python3 -m py_compile benchmark.py and git diff --check -- benchmark.py. Ran BENCH_DISCLOSURE_DATA_DIR=/tmp/fusiondb_disclosure_claim_smoke/data BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 python3 benchmark.py against an isolated temporary FusionDB server. The run exited 0, printed BENCH_CLAIM_MODE passed, wrote benchmark_report_small_http_matrix_index_topk.json, and the report contained disclosure.schema_version=1, status=unaudited_non_official, Cargo package fusiondb 0.1.0, git dirty status, Rust debug binary metadata, x86_64 system CPU/RAM, data_dir source BENCH_DISCLOSURE_DATA_DIR with size_bytes, no FUSIONDB_PG_PASSWORD leak, and all Part 20 claim_status values passed.

</spec-entry>

<spec-entry category="test" keywords="benchmark,topk,sstable,claim-mode,verification,reverse" date="2026-07-09" title="SSTable-heavy Top-K claim smoke 2026-07-09" description="Verification for persisted SSTable reverse Top-K claim" source="main@8a12c0f">

### SSTable-heavy Top-K claim smoke 2026-07-09

Verified Part 20 SSTable-heavy claim with python3 -m py_compile benchmark.py and git diff --check -- benchmark.py. Ran an isolated temporary FusionDB server from /tmp/fusiondb_topk_sstable_claim, then BENCH_DISCLOSURE_DATA_DIR=/tmp/fusiondb_topk_sstable_claim/data BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 python3 benchmark.py. The run exited 0, printed checkpoint index_topk_sstable_claim: ok, BENCH_CLAIM_MODE passed, and wrote benchmark_report_small_http_matrix_index_topk.json. Report load metadata had index_topk_sstable_claim_checkpoint_ok=1 and checkpoint_ms about 240ms. All five ordered DESC paths had sstable_heavy_required=true, claim_sstable_reverse_observed=true, claim_status=passed, ordered reverse scans/query=1, sort fallbacks/query=0, query-result cache eligible/hit/miss per query=0, Fusion raw/puts about 22/20 per query, and SSTable reverse iterator/block/decode/yield counters nonzero.

</spec-entry>

<spec-entry category="test" keywords="benchmark,topk,first-pass,verification,sstable,claim-mode" date="2026-07-09" title="First persisted Top-K pass smoke 2026-07-09" description="Smoke verification for Part 20 first persisted pass" source="main@8a12c0f">

### First persisted Top-K pass smoke 2026-07-09

Verified first persisted Part 20 rows with python3 -m py_compile benchmark.py and git diff --check -- benchmark.py. Ran an isolated temporary FusionDB server from /tmp/fusiondb_topk_first_pass, then BENCH_DISCLOSURE_DATA_DIR=/tmp/fusiondb_topk_first_pass/data BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 python3 benchmark.py. The run exited 0, printed checkpoint index_topk_sstable_claim: ok and BENCH_CLAIM_MODE passed. The report had 26 Part 20 rows: 5 phase=first-pass and 21 phase=warm. Every first-pass row had planned_iters=1, warmup_iters=0, query_count=1, row_count=20, claim_status=passed, ordered reverse scans/query=1, query sort fallbacks/query=0, query-result cache hits/query=0, and nonzero SSTable reverse iterator/block/decode/yield counters.

</spec-entry>

<spec-entry category="test" keywords="benchmark,topk,restart,smoke,verification,claim-mode" date="2026-07-09" title="Top-K restart matrix smoke 2026-07-09" description="Verification for Part 23 restart matrix and original Part 20 regression" source="main@8a12c0f">

### Top-K restart matrix smoke 2026-07-09

Verified BENCH_MATRIX=index_topk_restart with python3 -m py_compile benchmark.py and git diff --check -- benchmark.py. Main smoke: BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk_restart BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 BENCH_INDEX_TOPK_RESTART_BINARY=target/debug/fusiondb BENCH_INDEX_TOPK_RESTART_PORT=18101 python3 benchmark.py exited 0 and printed BENCH_CLAIM_MODE passed. Report benchmark_report_small_http_matrix_index_topk_restart.json had 10 rows: 5 restart-first-pass and 5 restart-warm, all claim_status=passed with ordered reverse scans/query=1, sort fallbacks/query=0, query-result cache hits/query=0, and nonzero SSTable reverse iterator/block/decode/yield counters. Isolation smoke with FUSIONDB_URL=http://127.0.0.1:1/query and BENCH_INDEX_TOPK_ROWS=200 also passed, proving the matrix does not depend on the external server endpoint. Regression smoke for original BENCH_MATRIX=index_topk with rows=200 also passed after the new part.

</spec-entry>

<spec-entry category="test" keywords="benchmark,disclosure,restart,smoke,claim-mode" date="2026-07-09" title="Benchmark-owned restart disclosure smoke 2026-07-09" description="Restart matrix disclosure smoke with invalid external URL and owned server evidence" source="main@8a12c0f">

### Benchmark-owned restart disclosure smoke 2026-07-09

Verified benchmark.py owned-server disclosure with python3 -m py_compile benchmark.py and git diff --check -- benchmark.py. Ran FUSIONDB_URL=http://127.0.0.1:1/query BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk_restart BENCH_INDEX_TOPK_ROWS=200 BENCH_INDEX_TOPK_LIMIT=20 BENCH_INDEX_TOPK_RESTART_BINARY=target/debug/fusiondb BENCH_INDEX_TOPK_RESTART_PORT=18101 python3 benchmark.py. The run exited 0 and printed BENCH_CLAIM_MODE passed. benchmark_report_small_http_matrix_index_topk_restart.json contains disclosure.server.base_url=http://127.0.0.1:1/query, base_url_role=restored_external_configuration_at_report_time, disclosure.server.benchmark_owned.active=true, owned query/metrics/checkpoint URLs on 127.0.0.1:18101, process_cache_state=new after restart, os_page_cache_state=not controlled, 10 benchmark rows, phases restart-first-pass=5 and restart-warm=5, and all claim_status values passed. A direct non-owned disclosure call returned benchmark_owned.active=false.

</spec-entry>

<spec-entry category="test" keywords="benchmark,test,cold-cache,restart,claim-mode" date="2026-07-09" title="OS cache control default smoke 2026-07-09" description="Default no-drop path remains passing and records cache phase" source="main@8a12c0f">

### OS cache control default smoke 2026-07-09

Verified optional OS cache control changes with python3 -m py_compile benchmark.py and git diff --check -- benchmark.py. Ran default disabled path: FUSIONDB_URL=http://127.0.0.1:1/query BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk_restart BENCH_INDEX_TOPK_ROWS=200 BENCH_INDEX_TOPK_LIMIT=20 BENCH_INDEX_TOPK_RESTART_BINARY=target/debug/fusiondb BENCH_INDEX_TOPK_RESTART_PORT=18101 python3 benchmark.py. Run exited 0 and printed BENCH_CLAIM_MODE passed. Report benchmark_report_small_http_matrix_index_topk_restart.json contains disclosure.server.benchmark_owned.os_cache_control={mode:none, requested:false, executed:false, success:true, os_page_cache_controlled:false}; cache phases were restart_first_pass_process_cold_os_cache_uncontrolled=5 and restart_warm_after_first_pass=5; all claim_status values passed. Direct helper checks confirmed none returns success without execution and unsupported mode returns success=false without execution.

</spec-entry>

<spec-entry category="test" keywords="benchmark,test,restart,topk,multi-trial" date="2026-07-09" title="Top-K restart multi-trial smoke 2026-07-09" description="Part 23 two-trial claim-mode smoke passed" source="main@8a12c0f">

### Top-K restart multi-trial smoke 2026-07-09

Verified BENCH_INDEX_TOPK_RESTART_TRIALS with python3 -m py_compile benchmark.py and git diff --check -- benchmark.py. Compatibility smoke: FUSIONDB_URL=http://127.0.0.1:1/query BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk_restart BENCH_INDEX_TOPK_ROWS=120 BENCH_INDEX_TOPK_LIMIT=20 BENCH_INDEX_TOPK_RESTART_BINARY=target/debug/fusiondb BENCH_INDEX_TOPK_RESTART_PORT=18101 python3 benchmark.py exited 0 with BENCH_CLAIM_MODE passed. Multi-trial smoke added BENCH_INDEX_TOPK_RESTART_TRIALS=2 and also exited 0. Report benchmark_report_small_http_matrix_index_topk_restart.json had 20 rows, trial_number values [1,2], 10 rows per trial, phases restart-first-pass=10 and restart-warm=10, cache phases restart_first_pass_process_cold_os_cache_uncontrolled=10 and restart_warm_after_first_pass=10, all claim_status values passed, and disclosure.server.benchmark_owned.restart_trials_requested=2. Ports 18101/18102/8091/8092 had no listeners afterward.

</spec-entry>

<spec-entry category="test" keywords="benchmark,test,restart,trial-summary,claim-mode" date="2026-07-09" title="Top-K restart trial summary smoke 2026-07-09" description="Two-trial restart report aggregation smoke passed" source="main@8a12c0f">

### Top-K restart trial summary smoke 2026-07-09

Verified trial_summaries with python3 -m py_compile benchmark.py and git diff --check -- benchmark.py. Ran FUSIONDB_URL=http://127.0.0.1:1/query BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk_restart BENCH_INDEX_TOPK_ROWS=120 BENCH_INDEX_TOPK_LIMIT=20 BENCH_INDEX_TOPK_RESTART_BINARY=target/debug/fusiondb BENCH_INDEX_TOPK_RESTART_PORT=18101 BENCH_INDEX_TOPK_RESTART_TRIALS=2 python3 benchmark.py. Run exited 0 and printed BENCH_CLAIM_MODE passed. Report had 20 raw rows, all claim_status=passed, trial_summaries.index_topk_restart.schema_version=1, group_count=10, five restart-first-pass groups and five restart-warm groups. First-pass groups had trial_numbers [1,2] and sample_count=2; warm groups had sample_count=10. metric_per_query kept query_count=1.0 and query_sort_fallback_count=0.0 for inspected groups. No temporary FusionDB listener remained on 18101/18102/8091/8092.

</spec-entry>

<spec-entry category="test" keywords="benchmark,test,restart,review,claim-mode" date="2026-07-09" title="Part 23 review hardening smoke 2026-07-09" description="Review hardening smoke passed and workdir guard preserved sentinel" source="main@8a12c0f">

### Part 23 review hardening smoke 2026-07-09

Verified review hardening with python3 -m py_compile benchmark.py and git diff --check -- benchmark.py. Ran FUSIONDB_URL=http://127.0.0.1:1/query BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk_restart BENCH_INDEX_TOPK_ROWS=120 BENCH_INDEX_TOPK_LIMIT=20 BENCH_INDEX_TOPK_RESTART_BINARY=target/debug/fusiondb BENCH_INDEX_TOPK_RESTART_PORT=18101 BENCH_INDEX_TOPK_RESTART_TRIALS=2 python3 benchmark.py. The run exited 0 and printed BENCH_CLAIM_MODE passed under strict query_count metric checks. Report had 20 rows, all claim_status=passed, case_order_in_trial values 1..5, restart_case_policy=restart_before_each_case_per_trial, restart_trial_scope='one process restart per case per trial...', top-level owned disclosure carried the same policy and shared_data_dir_reused_across_trials=true, and trial_summaries included sample_quality p95/p99 tail-claim warnings. Verified non-empty BENCH_INDEX_TOPK_RESTART_WORKDIR guard by direct part invocation against /tmp/fusiondb_restart_guard_nonempty: it returned an error requiring BENCH_INDEX_TOPK_RESTART_RESET_WORKDIR=1 and left sentinel_exists=True. No temporary FusionDB listener remained on 18101/18102/8091/8092.

</spec-entry>

<spec-entry category="test" keywords="lazy-reverse,test,topk,fusion" date="2026-07-09" title="Lazy reverse activation verification 2026-07-09" description="Lazy reverse targeted tests and checks passed on 2026-07-09" source="main@8a12c0f">

### Lazy reverse activation verification 2026-07-09

Verification after lazy reverse activation implementation: cargo fmt --check passed; cargo check -q passed; git diff --check -- src/storage/fusion.rs passed; cargo test -q storage::fusion::tests::fusion_scan_range_reverse_lazily_activates_sstable_sources_by_frontier -- --nocapture --test-threads=1 passed; cargo test -q storage::fusion::tests::fusion_scan_range_reverse_records_raw_sstable_work_counters -- --nocapture --test-threads=1 passed; cargo test -q storage::fusion::tests::fusion_scan_range_reverse -- --nocapture --test-threads=1 passed with 4 tests. New test creates low/high SSTables and proves LIMIT 1 activates exactly one SSTable source by thread-local hook while returning data:lazy:902.

</spec-entry>

<spec-entry category="test" keywords="lazy-reverse,test,correctness,topk" date="2026-07-09" title="Lazy reverse pending-drain verification 2026-07-09" description="Regression test covers both LIMIT fast path and full scan pending drain" source="main@8a12c0f">

### Lazy reverse pending-drain verification 2026-07-09

Verified pending-drain fix with cargo fmt --check, cargo check -q, git diff --check -- src/storage/fusion.rs, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_lazily_activates_sstable_sources_by_frontier -- --nocapture --test-threads=1, and cargo test -q storage::fusion::tests::fusion_scan_range_reverse -- --nocapture --test-threads=1. The lazy activation test now checks both LIMIT 1 activation count == 1 and unbounded reverse scan activation count == 2 with all high and low SSTable rows returned in descending key order.

</spec-entry>

<spec-entry category="test" keywords="lazy-reverse,equal-frontier,tombstone,test" date="2026-07-09" title="Lazy reverse equal-frontier tombstone verification 2026-07-09" description="Equal-frontier SSTables are activated before reverse merge emits a key" source="main@8a12c0f">

### Lazy reverse equal-frontier tombstone verification 2026-07-09

Added fusion_scan_range_reverse_activates_equal_frontier_sstables_before_emit. The regression creates one SSTable with data:eq:001/data:eq:002 PUTs and a second SSTable with a newer tombstone for data:eq:002, so both SSTables have the same max user-key frontier. A fresh reverse scan over data:eq: must return only data:eq:001. This guards the inclusive pending activation condition pending.frontier_user_key >= active_top.user_key, which is required before emitting a key so newer tombstones or versions in equal-frontier SSTables participate in winner selection.

</spec-entry>

<spec-entry category="test" keywords="lazy-reverse,memtable,test,topk" date="2026-07-09" title="Reverse empty memtable source skip verification 2026-07-09" description="Regression proves empty active memtable is not opened as a reverse source" source="main@8a12c0f">

### Reverse empty memtable source skip verification 2026-07-09

Verified with cargo fmt --check, cargo check -q, git diff --check -- src/storage/fusion.rs, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_skips_empty_memtable_sources -- --nocapture --test-threads=1, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_lazily_activates_sstable_sources_by_frontier -- --nocapture --test-threads=1, and cargo test -q storage::fusion::tests::fusion_scan_range_reverse -- --nocapture --test-threads=1. New current-thread test snapshots one row to SSTable, confirms the active memtable is empty, runs reverse LIMIT 1, and asserts total reverse source opens == 1 and SSTable activations == 1.

</spec-entry>

<spec-entry category="test" keywords="index-prefix,bloom,sstable,reverse,test" date="2026-07-09" title="SQL index-prefix SSTable Bloom pruning verification" description="SQL index-prefix pruning tests passed" source="main@8a12c0f">

### SQL index-prefix SSTable Bloom pruning verification

Verified with cargo fmt --check, cargo check -q, git diff --check on touched files, cargo test -q storage::sstable::tests::versioned_filter_block_supports_sql_index_prefix_negative_checks, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_skips_sstable_by_sql_index_prefix_filter, cargo test -q storage::fusion::tests::fusion_scan_range_reverse, and cargo test -q server::http_server::tests::http_metrics_include_pg_connection_pool_fields. The Fusion test creates an overlapping no-match SSTable spanning i0| to i9| and a matching i5| SSTable, then verifies reverse scan results, one SSTable activation, positive SQL index-prefix probes, and negative skips.

</spec-entry>

<spec-entry category="test" keywords="index-prefix,bloom,benchmark,topk,part24" date="2026-07-09" title="SQL index-prefix pruning benchmark gate" description="Claim-mode benchmark gate for SQL index-prefix SSTable pruning" source="main@8a12c0f">

### SQL index-prefix pruning benchmark gate

Added and verified BENCH_MATRIX=index_topk_prefix_prune / Part 24 for SQL index-prefix SSTable pruning. Small claim-mode smoke on 2026-07-09 used decoys=4, rows_per_host=80, target_host=50, absent_host=51, limit=20, and passed with BENCH_CLAIM_MODE. Positive DESC and range DESC first-pass each observed checks=6, skips=3, positives=3, fail_open=0, reverse_iterator_opens=3, raw_reverse_reads=66, visible_puts=20, sort_fallbacks=0. Warm runs observed checks=30, skips=15, positives=15, opens=15, raw=330, puts=100. Absent first-pass observed checks=6, skips=6, positives=0, fail_open=0, opens=0, raw=0; warm checks=30, skips=30, opens=0, raw=0. Gate uses skip counters plus opens<=positives for positive cases, and a false-positive budget for absent cases.

</spec-entry>

<spec-entry category="test" keywords="no-fill,benchmark,part25,block-cache,sql" date="2026-07-09" title="SQL no-fill cache policy verification" description="Tests and benchmark evidence for SQL no-fill cache policy" source="main@8a12c0f">

### SQL no-fill cache policy verification

Verified SQL no-fill cache policy with cargo fmt --check, cargo check -q, python3 -m py_compile benchmark.py, git diff --check on touched files, cargo test -q storage::fusion::tests::fusion_scan_range_no_fill_cache_reads_without_populating_block_cache, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_matches_forward_after_mvcc_merge_and_limit, cargo test -q storage::fusion::tests::fusion_scan_range_for_each_matches_forward_and_reverse_range, cargo test -q storage::memory::tests, cargo test -q --test sql_stream_scan, cargo test -q --test sql_index_cache test_unbounded_fusion_full_scan_uses_no_fill_cache, cargo test -q --test sql_index_cache test_analyze_and_create_index_backfill_use_no_fill_cache, cargo test -q --test sql_full_scan_parallel, cargo test -q --test sql_in_list, cargo test -q --test sql_distinct_stream, and cargo test -q --test sql_group_aggregate. Added BENCH_MATRIX=sql_no_fill_cache / Part 25; small claim-mode smoke with 512 rows and 512B payload passed with query_count=5, row_count=512, block_cache_miss_count=360, block_cache_fill_skip_count=360, block_cache_insert_count=0, block_cache_insert_bytes=0, sstable_block_read_bytes=128115, sstable_iterator_open_count=15, query_result_cache_eligible_count=0.

</spec-entry>

<spec-entry category="test" keywords="no-fill,part25,benchmark,cache-pollution,control" date="2026-07-09" title="SQL no-fill owned control benchmark 2026-07-09" description="Owned-server SQL no-fill control benchmark verification" source="main@8a12c0f">

### SQL no-fill owned control benchmark 2026-07-09

Part 25 / BENCH_MATRIX=sql_no_fill_cache now owns two temporary FusionDB processes instead of requiring an external server. The benchmark writes config with row_cache_capacity=0, statement_cache_capacity=1, tiny block cache, and sql_bulk_scan_no_fill=true for the no-fill phase and false for the fill-cache control phase. It loads bench_sql_no_fill_hot plus bench_sql_no_fill_bulk, checkpoints, prewarms the hot row, then reports no_fill_bulk, no_fill_hot_after, fill_cache_bulk, and fill_cache_hot_after. Small claim-mode smoke passed on 2026-07-09 with 512 rows, 512B payload, and requested 1 cache block. no_fill_bulk: avg 17.698 ms, rows=512, query_count=5, misses=374, fill_skips=365, helper inserts=9, insert_bytes=37697, evictions=10, read_bytes=135159, iterator_opens=15, qcache eligible=0. fill_cache_bulk control: avg 20.252 ms, rows=512, query_count=5, misses=370, inserts=370, insert_bytes=1613235, fill_skips=0, evictions=362, read_bytes=133275, iterator_opens=15, qcache eligible=0. Hot reread rows remained diagnostic because SQL point probes touch multiple blocks and Moka admission can retain entries; hard cache-pollution proof uses bulk fill-skip/insert/eviction deltas.

</spec-entry>

<spec-entry category="test" keywords="reverse-block,test,sstable,topk,rseek" date="2026-07-09" title="Runtime reverse block decode verification 2026-07-09" description="Verification for bounded reverse block materialization" source="main@8a12c0f">

### Runtime reverse block decode verification 2026-07-09

Verified the runtime bounded reverse block decode slice with cargo fmt --check, cargo check -q, git diff --check -- src/storage/sstable.rs, cargo test -q storage::sstable::tests::reverse_block_bounds_materialize_only_needed_entries -- --nocapture --test-threads=1, cargo test -q storage::sstable::tests::user_key_range_reverse_iterator -- --nocapture --test-threads=1, cargo test -q storage::sstable::tests::no_fill_reverse_iterator_reads_blocks_without_populating_block_cache -- --nocapture --test-threads=1, cargo test -q storage::sstable::tests::user_key_reverse_iterator_returns_same_user_internal_versions_descending -- --nocapture --test-threads=1, cargo test -q storage::fusion::tests::fusion_scan_range_reverse -- --nocapture --test-threads=1, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_records_raw_sstable_work_counters -- --nocapture --test-threads=1, and cargo test -q storage::sstable::tests -- --nocapture --test-threads=1. The focused helper test proves a 100-entry decoded block with bounds [k090,k095) decodes/yields 5 entries rather than materializing the whole block.

</spec-entry>

<spec-entry category="test" keywords="rseek,sidecar,test,reverse-block,metrics" date="2026-07-09" title="Persisted reverse seek sidecar verification 2026-07-09" description="Verification for persisted reverse seek sidecar" source="main@8a12c0f">

### Persisted reverse seek sidecar verification 2026-07-09

Verified persisted .rseek sidecar with cargo fmt --check, cargo check -q, python3 -m py_compile benchmark.py, git diff --check on touched files, cargo test -q storage::sstable::tests::reverse_seek_sidecar_round_trips_and_rejects_stale_or_corrupt_bytes -- --nocapture --test-threads=1, cargo test -q storage::sstable::tests::reverse_iterator_uses_persisted_reverse_seek_sidecar -- --nocapture --test-threads=1, cargo test -q server::http_server::tests::http_metrics_include_pg_connection_pool_fields -- --nocapture --test-threads=1, cargo test -q storage::sstable::tests -- --nocapture --test-threads=1, cargo test -q storage::fusion::tests::fusion_scan_range_reverse -- --nocapture --test-threads=1, cargo test -q storage::fusion::tests::fusion_scan_range_reverse_records_raw_sstable_work_counters -- --nocapture --test-threads=1, cargo test -q storage::sstable::tests::no_fill_reverse_iterator_reads_blocks_without_populating_block_cache -- --nocapture --test-threads=1, and cargo test -q --test sql_index_cache secondary_btree_order_by_limit -- --nocapture --test-threads=1. The new end-to-end SSTable test proves builder.finish persists .rseek, a bounded reverse iterator over [k090,k095) returns k094..k090, sidecar hit/use counters increase, and fail_open does not increase.

</spec-entry>

<spec-entry category="test" keywords="rseek,benchmark,test,topk,restart" date="2026-07-09" title="rseek sidecar benchmark gate smoke 2026-07-09" description="Smoke verification for .rseek benchmark gate" source="main@8a12c0f">

### rseek sidecar benchmark gate smoke 2026-07-09

Verified benchmark.py .rseek sidecar claim gate with python3 -m py_compile benchmark.py, git diff --check -- benchmark.py, cargo build -q --bin fusiondb, and BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk_restart BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 BENCH_INDEX_TOPK_RESTART_BINARY=target/debug/fusiondb BENCH_INDEX_TOPK_RESTART_PORT=18133 BENCH_INDEX_TOPK_RESTART_TRIALS=1 BENCH_INDEX_TOPK_RESTART_RESET_WORKDIR=1 python3 benchmark.py. The run exited 0 and printed BENCH_CLAIM_MODE passed. Report benchmark_report_small_http_matrix_index_topk_restart.json records reverse_seek_sidecar_files_after_load=1, reverse_seek_sidecar_bytes_after_load=110708, restart-first-pass claim_reverse_seek_sidecar_status=observed with hit=1/use>0/path_failures=0, and warm rows observed use>0 with hit=0 as expected.

</spec-entry>

<spec-entry category="test" keywords="rseek,counters,test,benchmark,topk" date="2026-07-09" title="rseek block-internal counter verification 2026-07-09" description="Verification for .rseek block-internal counters" source="main@8a12c0f">

### rseek block-internal counter verification 2026-07-09

Verified rseek block-internal counters with cargo fmt --check, cargo check -q, python3 -m py_compile benchmark.py, git diff --check -- src/storage/sstable.rs src/monitor.rs src/server/http_server.rs benchmark.py, cargo test -q storage::sstable::tests::reverse_block_bounds_materialize_only_needed_entries -- --nocapture --test-threads=1, cargo test -q storage::sstable::tests::reverse_iterator_uses_persisted_reverse_seek_sidecar -- --nocapture --test-threads=1, cargo test -q server::http_server::tests::http_metrics_include_pg_connection_pool_fields -- --nocapture --test-threads=1, cargo test -q storage::sstable::tests -- --nocapture --test-threads=1, cargo build -q --bin fusiondb, and BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk_restart BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 BENCH_INDEX_TOPK_RESTART_BINARY=target/debug/fusiondb BENCH_INDEX_TOPK_RESTART_PORT=18134 BENCH_INDEX_TOPK_RESTART_TRIALS=1 BENCH_INDEX_TOPK_RESTART_RESET_WORKDIR=1 python3 benchmark.py. The smoke exited 0 with BENCH_CLAIM_MODE passed; report rows show claim_reverse_block_span_scan_count=0, claim_reverse_block_span_scan_entry_count=0, claim_reverse_block_span_materialize_entry_count=0, and nonzero sidecar indexed/materialize/offset-probe counters.

</spec-entry>

<spec-entry category="test" keywords="rseek,benchmark,ab,test,topk" date="2026-07-09" title="rseek A/B restart benchmark smoke 2026-07-09" description="Smoke verification for .rseek A/B restart benchmark" source="main@8a12c0f">

### rseek A/B restart benchmark smoke 2026-07-09

Verified benchmark.py rseek A/B restart benchmark with python3 -m py_compile benchmark.py, git diff --check -- benchmark.py, BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk_rseek_ab BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 BENCH_INDEX_TOPK_RESTART_BINARY=target/debug/fusiondb BENCH_INDEX_TOPK_RESTART_PORT=18136 BENCH_INDEX_TOPK_RESTART_TRIALS=1 BENCH_INDEX_TOPK_RESTART_RESET_WORKDIR=1 python3 benchmark.py, and a default compatibility run with BENCH_MATRIX=index_topk_restart rows=200 on port 18137. The A/B run exited 0 with BENCH_CLAIM_MODE passed and wrote benchmark_report_small_http_matrix_index_topk_rseek_ab.json with 20 rows. Representative counters: rseek-kept restart-first-pass had use=1, hit=1, miss=0, span_scan=0, sidecar indexed/materialize/probe nonzero; rseek-removed restart-first-pass had use=0, hit=0, miss=1, span_scan=1, span_scan_entries=54, span_materialize=52, sidecar indexed/materialize/probe all zero. The report disclosure recorded rseek_ab_enabled=true, variants [rseek-kept, rseek-removed], and fallback removal of 1 .rseek file / 110708 bytes.

</spec-entry>

<spec-entry category="test" keywords="rseek,benchmark,ab,checksum,test" date="2026-07-09" title="rseek A/B checksum pair verification 2026-07-09" description="Verification for paired .rseek kept/removed result checksums" source="main@8a12c0f">

### rseek A/B checksum pair verification 2026-07-09

Verified benchmark.py paired A/B result evidence with python3 -m py_compile benchmark.py, git diff --check -- benchmark.py, BENCH_CLAIM_MODE=1 BENCH_SCALE=small BENCH_PROTO=http BENCH_MATRIX=index_topk_rseek_ab BENCH_INDEX_TOPK_ROWS=500 BENCH_INDEX_TOPK_LIMIT=20 BENCH_INDEX_TOPK_RESTART_BINARY=target/debug/fusiondb BENCH_INDEX_TOPK_RESTART_PORT=18139 BENCH_INDEX_TOPK_RESTART_TRIALS=1 BENCH_INDEX_TOPK_RESTART_RESET_WORKDIR=1 python3 benchmark.py, and a non-A/B compatibility run with BENCH_MATRIX=index_topk_restart rows=200 on port 18140. The A/B smoke exited 0 with BENCH_CLAIM_MODE passed. benchmark_report_small_http_matrix_index_topk_rseek_ab.json records complete_pair_count=10, checksum_match_count=10, checksum_mismatch_count=0, row_count_match_count=10, row_count_mismatch_count=0, all_result_checksums_match=true, and all_row_counts_match=true. All 20 A/B benchmark rows have metadata.rseek_ab_pair_claim_status=passed. Metric delta totals show removed-sidecar runtime span work of 38 scans, 1614 scanned entries, and 1124 materializations, while kept-sidecar rows account for 38 sidecar uses, 1614 indexed entries, 1124 materializations, and 532 offset probes.

</spec-entry>


<spec-entry category="test" keywords="block-index-prefix,sql-index-prefix,sstable,test,metrics" date="2026-07-09" title="Block-level SQL index-prefix verification 2026-07-09" description="Focused tests for block SQL index-prefix pruning" source="main@8a12c0f">

### Block-level SQL index-prefix verification 2026-07-09

Verification completed after adding block SQL index-prefix properties: cargo check -q passed; python3 -m py_compile benchmark.py passed; rustfmt --check --edition 2021 src/storage/sstable.rs src/monitor.rs src/server/http_server.rs passed; git diff --check on sstable/monitor/http_server/benchmark passed. Focused cargo tests passed: block_table_prefix_meta_decodes_without_sql_index_prefixes, user_key_range_iterator_skips_block_without_target_sql_index_prefix_property, user_key_range_iterator_fails_open_on_incomplete_sql_index_prefix_property, user_key_range_reverse_iterator_skips_block_without_target_sql_index_prefix_property, http_metrics_include_pg_connection_pool_fields, versioned_filter_block_supports_sql_index_prefix_negative_checks, user_key_range_iterator_skips_block_without_target_table_prefix_property, fusion_scan_range_reverse_skips_sstable_by_sql_index_prefix_filter.

</spec-entry>

<spec-entry category="test" keywords="benchmark,block-index-prefix,sql-index-prefix,sstable,part29" date="2026-07-09" title="SSTable block SQL index-prefix microbenchmark 2026-07-09" description="Part 29 block SQL index-prefix benchmark gate" source="main@8a12c0f">

### SSTable block SQL index-prefix microbenchmark 2026-07-09

Added BENCH_MATRIX=sstable_block_index_prefix / Part 29 and src/bin/sstable-block-index-prefix-bench.rs. The benchmark builds optimized, fail_open, and incomplete SSTable sets. Optimized uses a deterministic table-level SQL index-prefix positive probe while block properties lack the target prefix, so it proves file-level MayMatch plus block-level negative skip. Fail-open disables prefix metadata; incomplete injects malformed block entry metadata. Small smoke: 8 SSTables x 2 iters x 128B payload passed with optimized 16 table positives, 16 block-index skips, 0 misses; fail_open 16 fail-opens and 16 misses; incomplete 16 table positives, 16 block fail-opens, 16 misses. Default release run: 512 SSTables x 5 iters x 1024B payload, optimized avg 0.936 ms with 2,560 table positives, 2,560 block skips, 0 misses; fail_open avg 46.468 ms with 2,560 misses/fail-opens; incomplete avg 52.276 ms with 2,560 table positives and 2,560 block fail-opens/misses; about 49.6x speedup versus fail-open. Report: benchmark_report_medium_http_matrix_sstable_block_index_prefix.json.

</spec-entry>

<spec-entry category="test" keywords="part29,natural-false-positive,block-index-prefix,benchmark,sstable" date="2026-07-09" title="Part 29 natural false-positive block index-prefix gate 2026-07-09" description="Part 29 natural Bloom false-positive gate" source="main@8a12c0f">

### Part 29 natural false-positive block index-prefix gate 2026-07-09

Part 29 now includes a natural_false_positive phase in src/bin/sstable-block-index-prefix-bench.rs. This phase inserts 32,768 real distinct neighboring SQL index prefixes by default (i1NNNNNNNN and i3NNNNNNNN), does not insert the target prefix and does not use a filter-only key, then searches canonical i2NNNNNNNN prefixes for a real SSTable-level Bloom false positive. The gate requires table-level SQL index-prefix positives, block-level exact SQL index-prefix skips, zero block misses, zero block fail-opens, and row_count == 0. Small smoke with 8 regular SSTables, 2 iters, 128B payload, natural 32,768 prefixes, 2 natural iters passed: natural target index:metrics:host_id,ts:i200001316|, 2 table positives, 2 block skips, 0 misses. Default release BENCH_MATRIX=sstable_block_index_prefix passed: optimized avg 0.348 ms with 2,560 table positives/block skips and 0 misses; fail_open avg 43.267 ms with 2,560 misses/fail-opens; incomplete avg 33.678 ms with 2,560 fail-opens/misses; natural false-positive avg 0.081 ms with 3 table positives, 3 block skips, 0 misses; speedup vs fail_open about 124.5x.

</spec-entry>

<spec-entry category="test" keywords="part30,sql-block-index-prefix,smoke,benchmark" date="2026-07-09" title="Part 30 SQL block index-prefix focused smoke 2026-07-09" description="Part 30 focused claim-mode smoke result and verification boundary" source="main@8a12c0f">

### Part 30 SQL block index-prefix focused smoke 2026-07-09

Verification after adding Part 30: python3 -m py_compile benchmark.py passed; git diff --check -- benchmark.py passed; cargo check -q passed. A full BENCH_MATRIX=sql_block_index_prefix_prune BENCH_CLAIM_MODE=1 smoke loaded 100,064 rows and discovered natural false-positive host 1000095 with file_positives=1 and block_skips=1, but initially failed because the ASC path was mistakenly registered as DESC. After fixing path classification, a focused Part 30 claim invocation on the same loaded data passed: first-pass rows=10, file_pos=2, block_skip=1, compactions=0; warm rows=10 over 5/5 iterations, file_pos=10, block_skip=5, compactions=0, error=None. Full main was not rerun after the classification fix to avoid another 100k-row reload.

</spec-entry>

<spec-entry category="test" keywords="part30,sql,block-index-prefix,delayed-index,smoke,claim" date="2026-07-09" title="Part 30 delayed index clean smoke 2026-07-09" description="Clean Part 30 delayed-index claim-mode smoke passed with corrected load timing." source="main@8a12c0f">

### Part 30 delayed index clean smoke 2026-07-09

Clean data-dir smoke used /tmp/fusiondb_part30_clean_delay.ZAqfFL and target/debug/fusiondb, with BENCH_MATRIX=sql_block_index_prefix_prune BENCH_SCALE=small BENCH_CLAIM_MODE=1 BENCH_INDEX_TOPK_LIMIT=10 BENCH_SQL_BLOCK_INDEX_PREFIX_DECOY_SSTABLES=1 BENCH_SQL_BLOCK_INDEX_PREFIX_PREFIXES_PER_SSTABLE=100000 BENCH_SQL_BLOCK_INDEX_PREFIX_CANDIDATE_PROBES=20000 BENCH_SQL_BLOCK_INDEX_PREFIX_DELAY_INDEX=1 python3 benchmark.py. Result: BENCH_CLAIM_MODE passed. Loaded 100064 rows in 20057 ms. load_sql_block_index_prefix_decoys=18276 ms, load_sql_block_index_prefix_create_index=1672 ms, discovery=68 ms, target=41 ms. Natural false-positive host=1000011, file-level positives=1, block skips=1, checkpoint successes=1, target checkpoint ok=1. Query latency: first-pass avg 6.1 ms, warm avg 3.0 ms, 10 rows. JSON report confirmed total_load_ms and component timers are no longer overwritten.

</spec-entry>

<spec-entry category="test" keywords="part30,copy-stdin,smoke,claim,benchmark" date="2026-07-09" title="Part 30 COPY loader clean smoke 2026-07-09" description="Clean Part 30 COPY chunk 1000 smoke passed and improved setup; larger chunks were worse or invalid." source="main@8a12c0f">

### Part 30 COPY loader clean smoke 2026-07-09

Final clean smoke used /tmp/fusiondb_part30_copy_final.f6biXT with target/debug/fusiondb and default COPY loader: BENCH_MATRIX=sql_block_index_prefix_prune BENCH_SCALE=small BENCH_CLAIM_MODE=1 BENCH_INDEX_TOPK_LIMIT=10 BENCH_SQL_BLOCK_INDEX_PREFIX_DECOY_SSTABLES=1 BENCH_SQL_BLOCK_INDEX_PREFIX_PREFIXES_PER_SSTABLE=100000 BENCH_SQL_BLOCK_INDEX_PREFIX_CANDIDATE_PROBES=20000 BENCH_SQL_BLOCK_INDEX_PREFIX_DELAY_INDEX=1 python3 benchmark.py. Result: BENCH_CLAIM_MODE passed. total_load_ms=16367, decoys=14543, create_index=1686, discovery=99, target=39. load_method=copy_stdin_csv, copy_format=csv, chunk_rows=1000, expected_loaded_rows=100064, loaded_rows=100064, expected_copy_batches=101, copy_batches=101, copy_rows=100064, copy_bytes=3846230, max_payload_bytes=41000, insert_value_batches=0, setup_compaction_run_count=0. Natural false-positive host=1000021, file positives=1, block skips=1. Query latency first-pass avg=4.9 ms, warm avg=2.9 ms. Compared to prior clean INSERT delayed-index smoke total_load_ms=20057 and decoys=18276, COPY chunk 1000 improves total setup about 1.23x and decoy load about 1.26x. Negative control: COPY chunk 20000 passed claim but was slower, total_load_ms=50805 and decoys=48408; 100000-row payload failed HTTP 413 Payload Too Large.

</spec-entry>

<spec-entry category="test" keywords="part31,zone-map,metrics,test,verification" date="2026-07-09" title="Part 31 zone-map scaffold verification 2026-07-09" description="Part 31 zone-map metrics scaffold passed compile, diff, and HTTP metrics tests." source="main@8a12c0f">

### Part 31 zone-map scaffold verification 2026-07-09

Verification after adding the Part 31 zone-map observability scaffold: cargo fmt -- src/storage/mod.rs src/monitor.rs src/server/http_server.rs completed; cargo check -q passed; python3 -m py_compile benchmark.py passed; git diff --check -- src/storage/mod.rs src/monitor.rs src/server/http_server.rs benchmark.py passed; cargo test -q http_metrics_include_pg_connection_pool_fields -- --nocapture passed. The focused HTTP metrics test verifies JSON /metrics and Prometheus include the new sstable_block_zone_map_* counters. No Part 31 pruning benchmark is claimable yet because no zone-map metadata or skip path has been enabled.

</spec-entry>

<spec-entry category="test" keywords="part31,sstable,zone-map,metadata,tests" date="2026-07-09" title="Part 31 SSTable zone-map metadata verification 2026-07-09" description="Focused verification for Part 31 SSTable zone-map metadata scaffold" source="main@8a12c0f">

### Part 31 SSTable zone-map metadata verification 2026-07-09

Verified after final field-shape adjustment: cargo fmt -- src/storage/sstable.rs && cargo check -q passed; cargo test -q meta_ -- --nocapture passed with 13 matching tests; cargo test -q block_sql -- --nocapture passed with 2 matching tests; cargo test -q builder_without_sql_zone_maps_keeps_v4_meta_format -- --nocapture passed; git diff --check -- src/storage/sstable.rs passed.

</spec-entry>

<spec-entry category="test" keywords="part31,sstable,v5,metadata,tests,wire-format" date="2026-07-09" title="Part 31 framed SSTable v5 metadata verification 2026-07-09" description="Focused verification for framed SSTable v5 metadata hardening" source="main@8a12c0f">

### Part 31 framed SSTable v5 metadata verification 2026-07-09

Verification after framed v5 hardening: cargo fmt -- src/storage/sstable.rs && cargo check -q passed; cargo test -q meta_ -- --nocapture passed with 17 matching tests; cargo test -q sstable_meta_decode_rejects -- --nocapture passed with 2 matching tests; cargo test -q builder_with_sql_zone_maps_writes_v5_meta_format -- --nocapture passed; cargo test -q block_sql -- --nocapture passed with 3 matching tests; git diff --check -- src/storage/sstable.rs passed. New tests cover framed v5 decode, unframed temporary v5 decode, trailing metadata rejection, unknown framed version rejection, and builder v5 write/read.

</spec-entry>

<spec-entry category="test" keywords="part31,zone-map,producer,tests,sstable,fusion" date="2026-07-09" title="Part 31 SQL zone-map metadata producer verification 2026-07-09" description="Focused verification for Part 31 SQL zone-map metadata producer" source="main@8a12c0f">

### Part 31 SQL zone-map metadata producer verification 2026-07-09

Verification after producer implementation: cargo fmt -- src/storage/sstable.rs src/storage/fusion.rs && cargo check -q passed; cargo test -q builder_collects_sql_zone_maps_for_supported_data_rows -- --nocapture passed; cargo test -q builder_sql_zone_map_collection -- --nocapture passed with 2 matching tests; cargo test -q sql_zone_map_schema_fingerprint -- --nocapture passed; cargo test -q fusion_snapshot_flush_writes_sql_zone_map_metadata -- --nocapture passed; cargo test -q meta_ -- --nocapture passed with 17 matching tests; cargo test -q block_sql -- --nocapture passed with 3 matching tests; cargo test -q fusion_flush_candidate_remains_visible_until_sstable_registration -- --nocapture passed; git diff --check -- src/storage/sstable.rs src/storage/fusion.rs passed.

</spec-entry>

<spec-entry category="test" keywords="part31,zone-map,producer,safety,tests,compaction" date="2026-07-09" title="Part 31 producer safety regression verification 2026-07-09" description="Producer fail-open and compaction tombstone verification before pruning" source="main@8a12c0f">

### Part 31 producer safety regression verification 2026-07-09

Added and verified producer safety regressions before any pruning is enabled. New SSTable tests cover type mismatch fail-open, malformed row payload / invalid value flag fail-open, unsupported-only schema staying v4, and existing supported-row producer behavior. New Fusion compaction test verifies compaction recomputes SQL zone maps from output entries and carries tombstone_count for future fail-open pruning. Verification commands passed: cargo fmt -- src/storage/sstable.rs src/storage/fusion.rs && cargo check -q; cargo test -q builder_sql_zone_map_collection_fails_open_on_type_mismatch -- --nocapture; cargo test -q builder_sql_zone_map_collection_fails_open_on_malformed_values -- --nocapture; cargo test -q fusion_compaction_recomputes_sql_zone_maps_with_tombstones -- --nocapture; cargo test -q builder_sql_zone_map_collection -- --nocapture; cargo test -q fusion_snapshot_flush_writes_sql_zone_map_metadata -- --nocapture; cargo test -q meta_ -- --nocapture; cargo test -q block_sql -- --nocapture; git diff --check -- src/storage/sstable.rs src/storage/fusion.rs.

</spec-entry>

<spec-entry category="test" keywords="part31,mvcc,zone-map,verification,cargo-test" date="2026-07-09" title="Part 31 MVCC gate scaffold verification 2026-07-09" description="Focused verification for Part 31 MVCC gate scaffold" source="main@8a12c0f">

### Part 31 MVCC gate scaffold verification 2026-07-09

Verification after the non-skipping MVCC gate scaffold: cargo check -q passed; cargo test -q sql_block_zone_map -- --nocapture passed 3 tests; cargo test -q builder_sql_zone_map_collection -- --nocapture passed 4 tests; cargo test -q fusion_snapshot_flush_writes_sql_zone_map_metadata -- --nocapture passed; cargo test -q scan_prefix_parallel_matches_serial_across_split_boundaries -- --nocapture passed; cargo test -q meta_ -- --nocapture passed 17 tests; git diff --check passed for src/storage/mod.rs, src/storage/sstable.rs, src/storage/fusion.rs, src/execution/mod.rs, and src/execution/scan/mod.rs.

</spec-entry>

<spec-entry category="test" keywords="part31,zone-map,decision,evaluator,verification" date="2026-07-09" title="Part 31 zone-map decision evaluator verification 2026-07-09" description="Focused verification for SQL zone-map decision evaluator scaffold" source="main@8a12c0f">

### Part 31 zone-map decision evaluator verification 2026-07-09

Verification after adding the non-wired zone-map decision evaluator: rustfmt --edition 2021 src/storage/mod.rs passed; cargo check -q passed; cargo test -q sql_block_zone_map_decision -- --nocapture passed 4 tests; cargo test -q sql_block_zone_map -- --nocapture passed 7 tests; cargo test -q builder_sql_zone_map_collection -- --nocapture passed 4 tests; cargo test -q meta_ -- --nocapture passed 17 tests; git diff --check passed for src/storage/mod.rs, src/storage/sstable.rs, src/storage/fusion.rs, src/execution/mod.rs, and src/execution/scan/mod.rs.

</spec-entry>

<spec-entry category="test" keywords="part31,sstable,iterator,zone-map,verification" date="2026-07-09" title="Part 31 approved block skip iterator verification 2026-07-09" description="Focused verification for approved block skip iterator scaffold" source="main@8a12c0f">

### Part 31 approved block skip iterator verification 2026-07-09

Verification after adding the approved block skip iterator scaffold: rustfmt --edition 2021 src/storage/sstable.rs passed; cargo check -q passed without warnings; cargo test -q block_property_user_key_interval -- --nocapture passed; cargo test -q forward_iterator_skips_only_fusion_approved_block_offsets -- --nocapture passed; cargo test -q sql_block_zone_map -- --nocapture passed 7 tests; cargo test -q meta_ -- --nocapture passed 17 tests; cargo test -q builder_sql_zone_map_collection -- --nocapture passed 4 tests; git diff --check passed for src/storage/sstable.rs, src/storage/mod.rs, src/storage/fusion.rs, src/execution/scan/mod.rs, and src/execution/mod.rs.

</spec-entry>

<spec-entry category="test" keywords="part31,zone-map,fusion,test,cdc" date="2026-07-09" title="Part 31 Fusion approved zone-map skip verification 2026-07-09" description="Verification for Fusion approved zone-map block skip" source="main@8a12c0f">

### Part 31 Fusion approved zone-map skip verification 2026-07-09

L3: Verified the Fusion approved block skip planner with cargo check -q; cargo test -q fusion_forward_scan_skips_approved_sql_zone_map_blocks -- --nocapture; cargo test -q fusion_sql_zone_map_skip_fails_open_on_overlapping_newer_sstable -- --nocapture; cargo test -q sql_block_zone_map -- --nocapture; cargo test -q forward_iterator_skips_only_fusion_approved_block_offsets -- --nocapture; cargo test -q fusion_snapshot_flush_writes_sql_zone_map_metadata -- --nocapture; cargo test -q fusion_compaction_recomputes_sql_zone_maps_with_tombstones -- --nocapture; git diff --check over touched Part 31 files. Tests intentionally bulk-write metrics rows and choose a fully data-prefixed block from validated block properties so CDC records do not make the candidate block interval fail-open before the skip/MVCC gate under test.

</spec-entry>

<spec-entry category="test" keywords="part31,benchmark,owned-server,zone-map,verification" date="2026-07-09" title="Part 31 owned-server zone-map claim verification 2026-07-09" description="Small and large Part 31 claim gates pass on benchmark-owned high-mem server" source="main@8a12c0f">

### Part 31 owned-server zone-map claim verification 2026-07-09

Verification after adding the owned-server runner: python3 -m py_compile benchmark.py passed; git diff --check -- benchmark.py passed; cargo build -q passed; BENCH_MATRIX=sql_block_zone_map_prune BENCH_SCALE=small BENCH_CLAIM_MODE=1 python3 benchmark.py passed with 7,168 rows, benchmark_owned.active=true, memtable_flush_mb=256, 3 SSTables, setup compaction 0, and 8/8 claim rows passed; BENCH_MATRIX=sql_block_zone_map_prune BENCH_SCALE=large BENCH_CLAIM_MODE=1 python3 benchmark.py passed with 99,328 rows, benchmark_owned.active=true, memtable_flush_mb=256, 3 SSTables, setup/query compaction 0, and 8/8 claim rows passed.

</spec-entry>

<spec-entry category="test" keywords="part31,zone-map,mvcc,metrics,verification" date="2026-07-09" title="Part 31 MVCC reason counter verification 2026-07-09" description="Focused tests and small/large claims pass for MVCC reason counters" source="main@8a12c0f">

### Part 31 MVCC reason counter verification 2026-07-09

Verification after adding MVCC fail-open reason counters: python3 -m py_compile benchmark.py passed; cargo fmt -- src/monitor.rs src/server/http_server.rs src/storage/fusion.rs passed; cargo check -q passed; git diff --check -- src/monitor.rs src/server/http_server.rs src/storage/fusion.rs benchmark.py passed; cargo test -q http_metrics_include_pg_connection_pool_fields -- --nocapture passed; cargo test -q fusion_sql_zone_map_skip_fails_open_on_overlapping_newer_sstable -- --nocapture passed and now asserts SSTable-overlap reason plus reason-sum accounting; cargo test -q sql_block_zone_map -- --nocapture passed; BENCH_MATRIX=sql_block_zone_map_prune BENCH_SCALE=small BENCH_CLAIM_MODE=1 python3 benchmark.py passed; BENCH_MATRIX=sql_block_zone_map_prune BENCH_SCALE=large BENCH_CLAIM_MODE=1 python3 benchmark.py passed with 99,328 rows, owned server true, memtable_flush_mb=256, 3 SSTables, setup/query compaction 0, MVCC case sstable-overlap reason equal to aggregate MVCC fail-open, and 8/8 claim rows passed.

</spec-entry>

<spec-entry category="test" keywords="part31,zone-map,benchmark,verification,claim,large" date="2026-07-09" title="Part 31 enabled-disabled control verification 2026-07-09" description="Small and large Part 31 enabled/disabled control claims passed" source="main@8a12c0f">

### Part 31 enabled-disabled control verification 2026-07-09

Verification after adding the Part 31 enabled/disabled control gate: python3 -m py_compile benchmark.py passed; cargo fmt -- src/execution/mod.rs src/execution/scan/mod.rs src/server/http_server.rs passed; git diff --check over benchmark.py and touched Rust files passed; cargo check -q passed; cargo test -q sql_block_zone_map_scan_options_respect_scoped_disable -- --nocapture passed; cargo test -q strips_sql_block_zone_map_prune_hint_from_leading_comment -- --nocapture passed; cargo test -q sql_block_zone_map -- --nocapture passed; cargo build -q passed. BENCH_MATRIX=sql_block_zone_map_prune BENCH_SCALE=small BENCH_CLAIM_MODE=1 python3 benchmark.py passed with 7,168 rows, 12/12 claim rows, 4 complete control pairs, checksum/row_count match, and clustered positive deltas. BENCH_MATRIX=sql_block_zone_map_prune BENCH_SCALE=large BENCH_CLAIM_MODE=1 python3 benchmark.py passed with 99,328 rows, 4 complete pairs, checksum/row_count match, clustered positive deltas, and total block_read_requests_disabled_minus_enabled=380,140 plus sstable_block_read_bytes_disabled_minus_enabled=139,401,380.

</spec-entry>

<spec-entry category="test" keywords="composite,topk,benchmark,claim,index-only" date="2026-07-09" title="Composite Top-K key-column covering verification 2026-07-09" description="Tests and Part 20 claim for composite key-column covering" source="main@8a12c0f">

### Composite Top-K key-column covering verification 2026-07-09

Verified focused composite covering with cargo test -q composite_ordered_scan_covers_primary_key_and_index_columns -- --nocapture. Verified broader composite coverage with cargo test -q composite -- --nocapture: 22 passed. Verified build health with cargo check -q and git diff --check. Ran BENCH_MATRIX=index_topk BENCH_SCALE=small BENCH_CLAIM_MODE=1 BENCH_INDEX_TOPK_ROWS=2048 python3 benchmark.py; BENCH_CLAIM_MODE passed. Report benchmark_report_small_http_matrix_index_topk.json shows composite_ordered_index_asc/desc/range_desc/window_desc all have row_reads_per_query=0.0, index_ordered_topk_scans_per_query=1.0, index_ordered_topk_entry_visits_per_query=50.0, query_sort_fallbacks_per_query=0.0, claim_status=passed.

</spec-entry>

<spec-entry category="test" keywords="composite,include,benchmark,claim,topk,row-read" date="2026-07-09" title="Composite INCLUDE covering Top-K verification 2026-07-09" description="Test and benchmark evidence for composite INCLUDE covering Top-K support." source="main@8a12c0f">

### Composite INCLUDE covering Top-K verification 2026-07-09

Validation commands run after adding composite INCLUDE covering support: cargo fmt on composite_index.rs, ddl/index.rs, ddl/table.rs, and scan/index_plan.rs; cargo test -q composite_include_ordered_scan_covers_payload_columns -- --nocapture; cargo test -q composite_index_meta_value_with_include_roundtrips -- --nocapture; cargo check -q; cargo test -q composite -- --nocapture; cargo test -q --test sql_index_cache include -- --nocapture; git diff --check for benchmark.py and touched Rust files; python3 -m py_compile benchmark.py. Benchmark gate run: BENCH_MATRIX=index_topk BENCH_SCALE=small BENCH_CLAIM_MODE=1 BENCH_INDEX_TOPK_ROWS=2048 python3 benchmark.py. Result: BENCH_CLAIM_MODE passed. New case TopK composite covering payload ASC reported avg_ms about 59.551, row_reads_per_query 0.0, ordered_topk_scans_per_query 1.0, ordered_topk_entry_visits_per_query 50.0, query_sort_fallbacks_per_query 0.0, claim_status passed. Server was stopped cleanly after benchmark.

</spec-entry>

<spec-entry category="test" keywords="topk,metrics,benchmark,claim,index-only,row-cache" date="2026-07-09" title="Ordered Top-K row-source metrics verification 2026-07-09" description="Test and benchmark evidence for ordered Top-K row-source counters." source="main@8a12c0f">

### Ordered Top-K row-source metrics verification 2026-07-09

Verified ordered Top-K row-source metrics with: cargo fmt over monitor.rs, http_server.rs, scan/mod.rs, scan/index_plan.rs, composite_index.rs, tests/sql_index_cache.rs; cargo test -q --test sql_index_cache test_ordered_topk_metrics_count_index_and_sort_paths -- --nocapture; cargo test -q server::http_server::tests::http_metrics_include_pg_connection_pool_fields -- --nocapture; cargo check -q; python3 -m py_compile benchmark.py; git diff --check over touched files; cargo build -q --bin fusiondb; BENCH_MATRIX=index_topk BENCH_SCALE=small BENCH_CLAIM_MODE=1 BENCH_INDEX_TOPK_ROWS=240 BENCH_INDEX_TOPK_LIMIT=20 against a fresh current-binary server on 127.0.0.1:18142. Claim mode passed. Report evidence: TopK composite covering payload ASC index_only_rows_per_query=20.0, base_row_fetches_per_query=0.0, row_reads_per_query=0.0, visits_per_query=20.0. Secondary heap-fetch control showed index_only_rows_per_query=0.0 and base_row_fetches_per_query=20.0 even when row_reads_per_query=0.0, proving the new counter catches row-cache-masked base-row materialization. Broader regressions also passed: cargo test -q composite -- --nocapture and cargo test -q --test sql_index_cache include -- --nocapture.

</spec-entry>

<spec-entry category="test" keywords="composite,include,c5,test,benchmark,claim" date="2026-07-09" title="Composite INCLUDE c5 verification 2026-07-09" description="Verification for c5 composite INCLUDE metadata and Top-K covering compatibility." source="main@8a12c0f">

### Composite INCLUDE c5 verification 2026-07-09

Verified c5 composite INCLUDE metadata hardening with: cargo fmt -- src/execution/composite_index.rs tests/sql_index_cache.rs; cargo test -q composite_index_meta_value_with_include -- --nocapture; cargo test -q composite_index_table_directory_filter_avoids_colon_prefix_collisions -- --nocapture; cargo test -q --test sql_index_cache test_composite_include_metadata_preserves_delimiter_identifiers -- --nocapture; cargo test -q --test sql_index_cache include -- --nocapture; cargo test -q composite -- --nocapture; cargo check -q; git diff --check -- src/execution/composite_index.rs tests/sql_index_cache.rs; cargo build -q --bin fusiondb; and a current-binary Part 20 smoke BENCH_MATRIX=index_topk BENCH_SCALE=small BENCH_CLAIM_MODE=1 BENCH_INDEX_TOPK_ROWS=240 BENCH_INDEX_TOPK_LIMIT=20. Claim mode passed; TopK composite covering payload ASC had row_reads_per_query=0.0, index_only_rows_per_query=20.0, base_row_fetches_per_query=0.0, visits_per_query=20.0, sort_fallbacks_per_query=0.0.

</spec-entry>

<spec-entry category="test" keywords="single-column,include,s3,test,compatibility,topk" date="2026-07-09" title="Single-column INCLUDE s3 verification 2026-07-10" description="Verification for s3 single-column INCLUDE metadata compatibility." source="main@8a12c0f">

### Single-column INCLUDE s3 verification 2026-07-10

Verified s3 single-column INCLUDE metadata hardening with: cargo fmt -- src/execution/composite_index.rs tests/sql_index_cache.rs; cargo test -q single_column_index_meta_value_with_include -- --nocapture; cargo test -q --test sql_index_cache test_secondary_btree_include_metadata_preserves_delimiter_identifiers -- --nocapture; cargo test -q --test sql_index_cache include -- --nocapture; cargo test -q composite_index_meta_value_with_include -- --nocapture; cargo test -q composite_index -- --nocapture; cargo check -q; git diff --check -- src/execution/composite_index.rs tests/sql_index_cache.rs. New tests cover s3 exact round-trip, delimiter identifiers in table/key/include names, legacy s2 reads, malformed s3 rejection, and SQL-level quoted table/index delimiter discovery for ordered Top-K covering payload.

</spec-entry>

<spec-entry category="test" keywords="data-prefix,include,ddl,test,alter-table,collision" date="2026-07-09" title="Prefix scan and INCLUDE DDL dependency verification 2026-07-10" description="Verification for exact data-prefix filtering and INCLUDE dependency checks." source="main@8a12c0f">

### Prefix scan and INCLUDE DDL dependency verification 2026-07-10

Verified exact table data-prefix filtering and single-column INCLUDE DDL dependency protection with: cargo fmt -- src/execution/mod.rs src/execution/ddl/table.rs tests/sql_ddl.rs; cargo test -q scan_routed_data_prefixes_filters_table_name_prefix_collisions -- --nocapture; cargo test -q --test sql_ddl test_alter_table_rejects_single_column_include_index_dependencies -- --nocapture; cargo test -q --test sql_ddl alter_table -- --nocapture; cargo check -q; git diff --check -- src/execution/mod.rs src/execution/ddl/table.rs tests/sql_ddl.rs. The prefix test inserts raw keys for table 'tenant' and colliding raw table 'tenant:archive', including a valid string primary key containing ':', and verifies both unlimited and LIMIT 1 scans return only the exact table row. The DDL test verifies DROP/RENAME is rejected for single-column INCLUDE key and payload columns while unrelated columns still drop.

</spec-entry>

<spec-entry category="test" keywords="single-column,include,s2,s3,fail-open,payload,test" date="2026-07-09" title="Single-column INCLUDE SQL fail-open compatibility verification 2026-07-10" description="SQL-level s2 compatibility and s3/payload fail-open regressions." source="main@8a12c0f">

### Single-column INCLUDE SQL fail-open compatibility verification 2026-07-10

Added SQL-level compatibility regressions for single-column BTree INCLUDE metadata and payload fail-open behavior. New tests in tests/sql_index_cache.rs cover: legacy s2 metadata still drives covering payload after base payload corruption; malformed s3 metadata is ignored and the query falls back to the current base row; malformed/empty INCLUDE index payload with valid s3 metadata falls back to base rows instead of using unsafe partial coverage. Verified with cargo fmt -- tests/sql_index_cache.rs; cargo test -q --test sql_index_cache test_secondary_btree_include_legacy_s2_metadata_covers_payload -- --nocapture; cargo test -q --test sql_index_cache test_secondary_btree_include_malformed_s3_metadata_falls_back_to_base_row -- --nocapture; cargo test -q --test sql_index_cache test_secondary_btree_include_malformed_payload_falls_back_to_base_rows -- --nocapture; cargo test -q --test sql_index_cache include -- --nocapture; cargo check -q; git diff --check -- tests/sql_index_cache.rs.

</spec-entry>

<spec-entry category="test" keywords="quoted,column,identifier,include,alter-table,test" date="2026-07-09" title="Quoted delimiter column verification 2026-07-10" description="Verification for quoted delimiter column CREATE/ALTER/INCLUDE behavior." source="main@8a12c0f">

### Quoted delimiter column verification 2026-07-10

Verified the minimal column identifier canonicalization slice with: cargo fmt -- src/execution/ddl/table.rs tests/sql_index_cache.rs tests/sql_ddl.rs; cargo test -q --test sql_index_cache test_secondary_btree_include_supports_quoted_delimiter_columns -- --nocapture; cargo test -q --test sql_ddl test_alter_table_supports_quoted_delimiter_columns -- --nocapture; cargo test -q --test sql_ddl alter_table -- --nocapture; cargo test -q --test sql_index_cache include -- --nocapture; cargo check -q; git diff --check -- src/execution/ddl/table.rs tests/sql_index_cache.rs tests/sql_ddl.rs. The SQL index test creates quoted columns "score:rank" and "payload,value", builds a single-column INCLUDE index, corrupts the base payload, and confirms ordered Top-K returns the index payload. The ALTER test adds, renames, selects, and drops quoted delimiter columns.

</spec-entry>