---
title: "Coding Conventions"
readMode: required
priority: high
category: coding
keywords:
  - style
  - naming
  - import
  - pattern
  - convention
  - formatting
---

# Coding Conventions

## Formatting

## Naming

## Imports

## Patterns

## Entries



<spec-entry category="coding" keywords="sql,3vl,null,predicate" date="2026-07-08" title="SQL predicate 3VL invariant" description="SQL predicate evaluation must preserve UNKNOWN internally" source="delegate:fdb-research-3vl-predicate-v2">

### SQL predicate 3VL invariant

Predicate evaluation keeps SqlTruth::{True,False,Unknown} internally. WHERE, JOIN filters, UPDATE/DELETE filters keep only True; projection converts Unknown to NULL; CHECK constraints accept True or Unknown and reject only False. NULL comparisons, NOT, AND, OR, BETWEEN, LIKE, IN/NOT IN, and array ANY/ALL must preserve Unknown until the final consumer.

</spec-entry>

<spec-entry category="coding" keywords="frontier,benchmark,fusion,public-api,lazy-reverse" date="2026-07-09" title="Part 28 Fusion reverse frontier public API gate" description="Production FusionStorage API benchmark for reverse frontier activation" source="working-tree">

### Part 28 Fusion reverse frontier public API gate

benchmark.py now includes non-default BENCH_MATRIX=fusion_reverse_frontier / Part 28, backed by src/bin/fusion-reverse-frontier-bench.rs. Unlike Part 27, this benchmark does not hand-build SSTables and does not use private injection hooks: it constructs data through public FusionStorage/Storage/Transaction APIs, commits writes/deletes, calls Storage::create_snapshot, and runs Transaction::scan_range_reverse through the production Fusion reverse merge heap. The default hard-gate dataset keeps target + decoys below COMPACTION_FANIN and fails if compaction_run_count is nonzero. It checks three production scenarios: LIMIT 1 deferred activation, full-drain pending activation, and equal-frontier tombstone correctness. The hard evidence is exact scan/frontier/pending/activation/deferred/equal-frontier/reverse-iterator/visible-put counters; latency remains smoke-only.

</spec-entry>

<spec-entry category="coding" keywords="frontier,benchmark,sstable,lazy-reverse,hard-gate" date="2026-07-09" title="Part 27 SSTable reverse frontier hard gate" description="Deterministic SSTable-level reverse frontier activation benchmark" source="working-tree">

### Part 27 SSTable reverse frontier hard gate

benchmark.py now includes non-default BENCH_MATRIX=sstable_reverse_frontier / Part 27, backed by src/bin/sstable-reverse-frontier-bench.rs. The binary hand-builds deterministic SSTables with public SsTableBuilder APIs: one target SSTable with the highest in-range key and configurable decoy SSTables whose file-level max keys overlap the query upper bound while their range-local block frontiers are below the active top key. It compares the optimized range-local frontier policy against a file-level control policy, emits JSON, and hard-gates same result checksum, exact activation reduction, pending/deferred counters, frontier probe/in-range/file counts, and reverse iterator opens. This benchmark deliberately avoids a FusionStorage injection API; the remaining optional follow-up is a separate FusionStorage public-API release benchmark that exercises the production merge heap end to end.

</spec-entry>

<spec-entry category="coding" keywords="frontier,metrics,lazy-reverse,sstable,benchmark" date="2026-07-09" title="Reverse frontier observability counters" description="Fusion reverse frontier counters and diagnostic benchmark gate" source="main@8a12c0f">

### Reverse frontier observability counters

Fusion reverse SSTable lazy activation now exposes a dedicated pending/frontier counter layer below SQL Top-K counters and above SSTable reverse iterator counters. `SsTable::reverse_frontier_for_range` returns a classified frontier (`BlockProperty` or `FileFallback`) while `reverse_frontier_user_key_for_range` remains the user-key wrapper. Fusion increments frontier probe, in-range, file fallback, tighten, empty skip, pending, activation, deferred unopened, and equal-frontier activation counters. `deferred_unopened` counts only SSTables left in the pending heap when the reverse scan stops; active but partially consumed sources are not included. `/metrics`, Prometheus, and `benchmark.py` export these counters. `BENCH_MATRIX=index_topk_frontier` is a non-default diagnostic Part 26 gate: it hard-gates frontier observability, in-range/tighten counters, result correctness, and Bloom-positive coverage, but does not hard-claim SQL activation reduction because SQL-generated SSTable block boundaries can straddle query upper bounds. Exact activation reduction remains covered by focused storage tests with hand-built SSTables.

</spec-entry>

<spec-entry category="coding" keywords="frontier,lazy-reverse,sstable,topk,performance" date="2026-07-09" title="In-range SSTable reverse frontier activation" description="Lazy reverse SSTable activation now uses range-local block frontier" source="main@8a12c0f">

### In-range SSTable reverse frontier activation

Fusion reverse SSTable lazy activation should use SsTable::reverse_frontier_user_key_for_range(start, end, TS_SIZE) instead of the table-level last_key frontier. The frontier is a conservative upper bound for the highest possible user key in the requested half-open range, derived from aligned block properties when available. It may be higher than the true in-range key, including using the exclusive upper bound as a sentinel, but must never be lower. Unknown metadata, legacy/misaligned block properties, too-short keys, or any sidecar/filter uncertainty must fail open to a table-level frontier. Returning None is only valid when empty range, table min/max, or aligned block properties prove the SSTable has no key in the query range. Fusion must still activate pending SSTables when pending frontier is equal to the active top user key to preserve tombstone/newer-version correctness.

</spec-entry>

<spec-entry category="coding" keywords="explain,analyze,q-error,planner,observability" date="2026-07-08" title="EXPLAIN ANALYZE top-level observability contract" description="Top-level EXPLAIN ANALYZE output and write-routing contract" source="delegate:fdb-review-explain-analyze-v3">

### EXPLAIN ANALYZE top-level observability contract

EXPLAIN ANALYZE first builds the explain plan and estimated root rows, executes the inner statement, then reports Planning Time, Execution Time, Actual Rows, Estimate Rows, and Q-Error before the plan text. Q-Error is 1.00 for 0/0, inf for one-sided zero, n/a without an estimate, and two decimals otherwise. Estimate rows are adjusted for simple LIMIT/OFFSET. EXPLAIN ANALYZE of write statements must be routed as writes because the inner statement executes.

</spec-entry>

<spec-entry category="coding" keywords="include-index,covering-index,btree,index-only,performance" date="2026-07-08" title="Single-column BTree INCLUDE index implementation" description="Single-column BTree INCLUDE indexes store payload in index entry values and support index-only scans" source="codex:include-index-2026-07-08">

### Single-column BTree INCLUDE index implementation

FusionDB supports CREATE INDEX ... ON table (key) INCLUDE (payload...) for single-column BTree secondary indexes. sqlparser CreateIndex.include is passed into DDL; metadata uses s2:<table>:<key>:<include_columns> while old table:column and v3/u3 formats remain compatible. BTree index entry values encode included columns with RowEncoder; old empty values fall back to covering only primary key plus indexed column. Insert/upsert/update refresh payloads, including include-only updates; primary-key update fast path is bypassed when an assignment touches an included column. Equality, IN, and range/BETWEEN index-only scans can use included payload columns when every entry has decodable payload. Composite INCLUDE indexes are intentionally not implemented yet.

</spec-entry>

<spec-entry category="coding" keywords="sql,join,limit,cross-join,performance" date="2026-07-08" title="CROSS JOIN LIMIT pushdown" description="CROSS JOIN LIMIT can early-stop safely in simple unordered row queries" source="codex:cross-join-limit-2026-07-08">

### CROSS JOIN LIMIT pushdown

Unordered, non-aggregate, non-DISTINCT single CROSS JOIN between plain tables may push limit+offset into execute_join. The join layer already stops while producing rows; the query layer must treat JoinOperator::CrossJoin as eligible only when predicates are absent or single-relation local predicates. Cross-relation WHERE predicates, ORDER BY, GROUP BY, HAVING, DISTINCT, aggregates, windows, materialized/deferred subquery filters remain excluded.

</spec-entry>

<spec-entry category="coding" keywords="sql,join,limit,row-goal,performance" date="2026-07-08" title="Multi-table INNER JOIN LIMIT row-goal boundary" description="Safe multi-table INNER JOIN LIMIT pushdown applies only at the final join step" source="codex:multi-join-limit-2026-07-08">

### Multi-table INNER JOIN LIMIT row-goal boundary

Unordered, non-aggregate, non-DISTINCT multi-table INNER JOIN chains between plain tables may pass the final limit+offset into execute_join when each ON predicate connects the newly joined relation to an already-joined relation and outer WHERE predicates are single-relation local predicates. execute_join must not pass the final LIMIT to intermediate join steps; it only applies the row goal to the final join step, then preserves remaining filtering/projection behavior. This avoids incorrect truncation where early intermediate rows do not survive later joins.

</spec-entry>

<spec-entry category="coding" keywords="analyze,stats,cbo,serialization,bincode,compatibility" date="2026-07-08" title="Versioned ANALYZE table stats storage" description="ANALYZE stats storage is now versioned with legacy read fallback" source="codex:stats-versioning-2026-07-08">

### Versioned ANALYZE table stats storage

ANALYZE table statistics are persisted as StoredTableStats { version, stats } with TABLE_STATS_STORAGE_VERSION = 1. load_table_stats first decodes the versioned wrapper, rejects unsupported versions, and falls back to legacy bare TableStats bytes for existing data. Public planner consumers still receive TableStats unchanged. This is the compatibility foundation for adding MCV, histogram, sample metadata, and multi-column stats without breaking old stats records.

</spec-entry>

<spec-entry category="coding" keywords="analyze,stats,cbo,serialization,bincode,wire-format,compatibility" date="2026-07-08" title="ANALYZE stats stable V1 wire structs" description="Stats persistence uses stable V1 wire structs independent from runtime TableStats" source="codex:stats-wire-v1-2026-07-08">

### ANALYZE stats stable V1 wire structs

ANALYZE stats storage now uses independent wire structs StoredTableStats { version, stats: TableStatsV1 }, TableStatsV1, and ColumnStatsV1. Runtime planner APIs still use TableStats/ColumnStats, and serialize/deserialize converts between runtime and V1 wire types. Legacy bare stats fallback decodes TableStatsV1, not runtime TableStats, so future runtime fields for MCV/histogram/sample metadata can be added without making V1 bytes undecodable.

</spec-entry>

<spec-entry category="coding" keywords="stats,cbo,analyze,v2,estimator,serialization" date="2026-07-08" title="Stats V2 storage and shared estimator foundation" description="Stats V2 wire format plus shared selectivity estimator foundation" source="codex:stats-v2-estimator-2026-07-08">

### Stats V2 storage and shared estimator foundation

ANALYZE table stats now write version 2 storage with explicit header dispatch. Runtime TableStats carries analyzed_rows, sampled, NDV metadata, empty MCV, and empty histogram buckets; V1 wrapper and bare V1 legacy bytes still decode to exact full-scan stats. Scan planning, join local predicate estimates, and EXPLAIN estimates use a shared StatsEstimator for equality, IN, NULL, range, LIKE, and selectivity-to-row conversion.

</spec-entry>

<spec-entry category="coding" keywords="analyze,stats,ndv,hll,cbo,canonical-hash" date="2026-07-08" title="ANALYZE adaptive NDV HLL collector" description="Adaptive exact-to-HLL NDV collection for ANALYZE stats" source="codex:adaptive-ndv-hll-2026-07-08">

### ANALYZE adaptive NDV HLL collector

ANALYZE column stats now use an adaptive distinct collector. Low NDV columns keep exact canonical byte keys; when unique keys exceed the bounded exact limit, collection switches to dense HyperLogLog with precision 12 and records DistinctCountKind::Estimated plus DistinctCountMethod::HyperLogLog. Canonical NDV keys normalize decimal text, float -0.0, and NaN before stable hashing, so exact and HLL paths share the same key semantics. HLL estimates are clamped to observed non-null rows and keep existing V1 decode compatibility.

</spec-entry>

<spec-entry category="coding" keywords="stats,cbo,join-cardinality,ndv,explain,q-error" date="2026-07-08" title="NDV equality join estimate in EXPLAIN" description="NDV-based equality join cardinality for EXPLAIN estimates" source="codex:ndv-equality-join-explain-2026-07-08">

### NDV equality join estimate in EXPLAIN

StatsEstimator now exposes an equality join cardinality estimate using non-null row scaling and max(left_ndv, right_ndv). EXPLAIN join order cost uses parsed equality join edges when both sides have table stats, falling back to the prior connected-join heuristic when stats or NDV are unavailable. EXPLAIN ANALYZE therefore receives improved root Estimate Rows and Q-Error for analyzed equality joins without changing execution row-goal boundaries.

</spec-entry>

<spec-entry category="coding" keywords="join,reorder,ndv,statsestimator,limit" date="2026-07-08" title="Actual join reorder uses NDV equality projected-row tie-break" description="Execution-side comma/inner join reorder uses NDV equality estimates as a safe tie-breaker." source="main@8a12c0f">

### Actual join reorder uses NDV equality projected-row tie-break

In src/execution/scan/join.rs, comma/inner join reorder now collects directional equality join edges and uses StatsEstimator::equality_join_estimate to estimate candidate projected rows when stats/NDV are available. Keep connected-candidate score as the primary ordering rule, use projected rows only as a tie-breaker, fall back to the previous row-count heuristic when estimates are missing, and preserve the final-step LIMIT row-goal boundary. Verified with cargo fmt --check, cargo test -q --test sql_join, cargo test -q --test sql_ddl, cargo test -q stats --lib, cargo test -q, and cargo build --release --bin fusiondb.

</spec-entry>

<spec-entry category="coding" keywords="scan,predicate,pushdown,late-materialization,benchmark" date="2026-07-08" title="Predicate-first filtered full scan" description="Full-table filtered scans decode predicate columns before decoding output rows for simple predicates." source="main@8a12c0f">

### Predicate-first filtered full scan

Filtered full-table scans now build a private ScanPredicatePlan for simple AND-connected column/constant comparisons (=, !=, <, <=, >, >=). When the plan covers the whole WHERE expression, FilteredScanVisitor first decodes only predicate columns with RowDecoder::decode_column and skips nonmatching rows before decoding projection/full rows. Cache hits preserve the old behavior by evaluating the cached row for both filter and output. Unsupported predicates fall back to the existing evaluate_expr path, and matched-row LIMIT semantics are unchanged. benchmark.py adds Full scan narrow val=X to expose this path. Verified with cargo fmt --check, python3 -m py_compile benchmark.py, git diff --check, cargo test -q --test sql_select, cargo test -q --test sql_stream_scan, cargo test -q --test sql_in_list, cargo test -q --test sql_group_aggregate, cargo test -q --test sql_ddl, cargo test -q, cargo build --release --bin fusiondb, and small/medium HTTP benchmark runs with 0 errors.

</spec-entry>

<spec-entry category="coding" keywords="scan,predicate_first,parallel,late_materialization,wide_scan" date="2026-07-08" title="Parallel predicate-first no-LIMIT full scan" description="No-LIMIT predicate-first full scans now use the parallel materialized scan path while LIMIT scans stay streaming." source="main@8a12c0f">

### Parallel predicate-first no-LIMIT full scan

No-LIMIT filtered full scans with a supported ScanPredicatePlan now materialize routed kv_pairs and apply predicate-first filtering through the CPU parallel path instead of returning early through the serial FilteredScanVisitor. LIMIT-bearing filtered scans still use the streaming visitor so matched-row LIMIT semantics are unchanged. A shared decode_predicate_first_filtered_row helper preserves the row-cache-hit behavior of evaluating the full cached row, decodes only predicate columns for cache misses, and materializes projection/full rows only after a match. For >1000 kv pairs the predicate-first no-LIMIT path uses rayon over the materialized kv_pairs and collects in input order; smaller scans use the same helper serially. Validation on 2026-07-08: cargo fmt --check, cargo check -q, cargo test -q --test sql_full_scan_parallel, sql_stream_scan, sql_select, sql_in_list all passed. A small HTTP wide_scan run over 20,000 wide rows showed warm phase improving from the prior roughly 50-70 ms band to 14-32 ms, with per-case metrics_delta confirming 5 measured queries and no slow warm queries.

</spec-entry>

<spec-entry category="coding" keywords="scan,parallel,error,correctness,full_scan" date="2026-07-08" title="Parallel fallback full-scan error propagation" description="Unsupported parallel full-scan fallback now propagates expression errors instead of swallowing them." source="main@8a12c0f">

### Parallel fallback full-scan error propagation

The unsupported no-LIMIT parallel full-scan fallback now propagates decode/evaluate errors instead of treating them as nonmatching rows. The rayon branch maps each key/value to Result<Option<Row>>, preserves input order by collecting a Vec and then applying item?, and returns the first FusionError in scan order. This removes the previous evaluate_expr(...).unwrap_or(false) behavior. tests/sql_full_scan_parallel.rs adds test_parallel_full_scan_propagates_filter_errors, using SELECT * FROM bench WHERE 10 / (val - val) > 0 on both a 1500-row parallel path and a 500-row serial path; both must return Division by zero. Verified with cargo fmt --check, cargo check -q, cargo test -q --test sql_full_scan_parallel, sql_stream_scan, sql_select, sql_group_aggregate, and cargo test -q before the final SELECT * wording cleanup; after cleanup, cargo test -q --test sql_full_scan_parallel passed again.

</spec-entry>

<spec-entry category="coding" keywords="scan,predicate,in-list,3vl,full-scan" date="2026-07-08" title="Predicate-first positive IN full scan" description="Positive IN list predicate-first full scan support with SQL 3VL boundaries" source="main@local">

### Predicate-first positive IN full scan

Positive column IN (constant...) predicates are now eligible for ScanPredicatePlan in full-table filtered scans. The fast path only represents the WHERE-True row set: RHS constants are evaluated once, coerced to the comparison column type, and NULL RHS values are skipped because a nonmatch with NULL is UNKNOWN and WHERE filters it out. NULL probe values never match. Empty or all-NULL IN lists therefore return no rows in WHERE. NOT IN, subquery IN, tuple IN, RHS column references, and any unsupported conjunct still fall back to the full expression evaluator. Tests cover serial no-LIMIT, parallel no-LIMIT, LIMIT visitor, row-cache hit reuse, numeric coercion, RHS NULL, NULL probe exclusion, scan-order preservation, and NOT IN 3VL regression. Verified with cargo fmt --check, git diff --check, cargo test -q --test sql_in_list, cargo test -q --test sql_full_scan_parallel, cargo check -q, cargo test -q, release build, and small/medium HTTP Part 1 benchmark runs.

</spec-entry>

<spec-entry category="coding" keywords="scan,predicate,or,in-list,3vl,benchmark" date="2026-07-08" title="Predicate-first OR equality full scan" description="Same-column OR equality predicate-first full scan support and OR/IN benchmark matrix" source="main@local">

### Predicate-first OR equality full scan

Same-column equality OR chains in single-table full-scan WHERE predicates can now lower into the existing ScanPredicateTermKind::InList term. The planner only accepts positive disjunctions where every branch is an equality between the same resolved schema column and a constant/parameter expression accepted by scan_predicate_value_expr; commuted equality is supported. Values are coerced to the column type and NULL branch values are skipped, which is safe only for WHERE True-row-set filtering because False and Unknown are both not returned. Mixed columns, mixed operators, unsupported branches, NOT/NOT IN, IS NULL, subqueries, tuple IN, and volatile/column RHS cases fall back to the full evaluator, preserving SQL 3VL and error propagation. Tests cover corrupt nonmatching row decode skipping, serial/parallel no-LIMIT order, LIMIT matched-row semantics, commuted equality, all-NULL OR, common OR conjunct lifting composition, and mixed-OR error propagation. benchmark.py adds non-default BENCH_MATRIX=or_in_scan / Part 12 with equivalent OR/IN narrow and wide pairs plus fallback controls; wide cases stabilize storage before measurement to avoid cold-cache skew.

</spec-entry>

<spec-entry category="coding" keywords="scan,predicate,between,3vl,index,benchmark" date="2026-07-08" title="Predicate-first BETWEEN full scan" description="Positive BETWEEN predicate-first full scan plus column-bound fallback panic fix" source="main@local">

### Predicate-first BETWEEN full scan

Positive single-column BETWEEN predicates in full-table WHERE scans now lower into two ScanPredicateTermKind::Compare terms: column >= low and column <= high. The optimization only accepts non-negated BETWEEN whose probe side resolves to one schema column and whose low/high bounds are constant or parameter expressions accepted by scan_predicate_value_expr; bounds are coerced to the column type. NULL probe values or NULL bounds do not match in the fast path, which is safe only for WHERE True-row-set filtering because SQL UNKNOWN is filtered out. NOT BETWEEN and column-bound BETWEEN fall back to the full evaluator. The index BETWEEN planner now refuses column-referencing bounds before evaluating low/high with an empty row, and evaluate_value now returns an Execution error instead of panicking when a row lacks a resolved column. Tests cover parallel/serial order, LIMIT matched-row semantics, nonmatching corrupted payload skip, NULL probe/bounds, inverted bounds, NOT BETWEEN and bound expression error propagation, and column-bound fallback without projection loss. benchmark.py adds non-default BENCH_MATRIX=between_scan / Part 13 with BETWEEN vs >= AND <= pairs, wide-row cases, NULL/inverted edge cases, and column-bound fallback controls.

</spec-entry>

<spec-entry category="coding" keywords="like,prefix,predicate,scan,benchmark" date="2026-07-08" title="Predicate-first LIKE prefix full scan" description="LIKE prefix predicate-first eligibility, index exact boundary, tests, and benchmark caveat" source="main@8a12c0f" confidence="contested" conflict-marker="CMK-20260708-1qcr" conflict-note="Superseded on 2026-07-08: complex constant LIKE patterns and NOT LIKE can now use exact predicate-first LikePattern; only ESCAPE/LIKE ANY/nonconstant or NULL pattern still fallback.">

### Predicate-first LIKE prefix full scan

Full-table WHERE scans may lower positive LIKE into ScanPredicateTermKind::LikePrefix only for exact pure-prefix string patterns: positive Expr::Like, any=false, no ESCAPE, one schema-column LHS, constant/parameter RHS evaluated to Value::String, pattern has nonempty fixed prefix followed only by one or more trailing percent signs, and the prefix contains no percent, underscore, or FusionDB question-mark wildcard. NULL/non-string probe values and NULL patterns do not match in the fast path, which is safe only for WHERE True-row-set filtering under SQL 3VL. NOT LIKE, ILIKE, leading/middle wildcard, underscore/question wildcard, ESCAPE, column-referencing patterns, and evaluation/coercion errors must fall back to the full evaluator. Indexed LIKE prefix exact scans must use the same pure-prefix helper; like_fixed_prefix alone is not enough because patterns such as ab%c or abc are not equivalent to starts_with(prefix) without residual filtering. Tests cover parallel no-LIMIT order, LIMIT matched-row semantics, corrupt nonmatching payload skip, complex wildcard fallback, and indexed LIKE residual regression. benchmark.py Part 14 / BENCH_MATRIX=like_prefix_scan adds paired prefix/fallback narrow and wide cases with bench_wide.label. Small/medium runs on 2026-07-08 showed correctness but modest speedup because full-table block reads dominate and cache order strongly affects raw averages; warm alternating medium runs were roughly 1.0x-1.07x.

</spec-entry>

<spec-entry category="coding" keywords="like,pattern,predicate,scan,late-materialization" date="2026-07-08" title="Predicate-first general LIKE pattern full scan" description="General constant LIKE/NOT LIKE predicate-first exact matching and benchmark update" source="main@8a12c0f">

### Predicate-first general LIKE pattern full scan

Full-table WHERE scans may now lower single-column constant LIKE patterns into an exact ScanPredicateTermKind::LikePattern. The planner accepts Expr::Like with any=false, no ESCAPE, one schema-column LHS, and a constant/parameter RHS that evaluates to Value::String. Pure positive prefix% still uses LikePrefix starts_with for speed; all other accepted patterns, including leading percent, underscore wildcard, FusionDB question-mark wildcard, middle percent, and NOT LIKE, use Executor::like_match on the decoded predicate column and apply negation inside the term. NULL probe values, NULL patterns, and non-string probe values do not match in the fast path, preserving WHERE True-row-set semantics under SQL 3VL and current evaluator behavior. LIKE ANY, ESCAPE, ILIKE, column-referencing patterns, pattern evaluation errors, subqueries, and unsupported conjuncts still fall back. Tests cover complex wildcard order, corrupt nonmatching payload skip for wildcard and NOT LIKE, prefix LIMIT/order, and indexed LIKE exact-prefix regression. benchmark.py Part 14 remains BENCH_MATRIX=like_prefix_scan for compatibility but is now titled LIKE Pattern Predicate-First Scan and includes prefix, leading wildcard, underscore wildcard, payload, and full-row wide cases. Medium HTTP benchmark on 2026-07-08 over 100k wide rows: Wide LIKE full 1% prefix 347.6 ms, underscore 368.4 ms; interleaved warm measurement showed full underscore/prefix around 0.88x, id around 1.03x, payload around 1.23x. Block reads remain the main limit, so next storage work should target MVCC-safe zone maps or SSTable iterator upper bounds.

</spec-entry>

<spec-entry category="coding" keywords="sstable,range,iterator,upper-bound,mvcc" date="2026-07-08" title="SSTable iterator user-key upper bound range scan" description="MVCC-safe SSTable range iterator upper-bound rule" source="main@8a12c0f">

### SSTable iterator user-key upper bound range scan

SSTable raw range iterators may use an exclusive raw upper bound, but Fusion MVCC range merge must use a decoded user-key upper bound with suffix_len=TS_SIZE instead of raw encode_key(end, u64::MAX). Internal-key ordering appends an inverted timestamp suffix, so raw upper bounds can incorrectly exclude shorter user keys in ranges such as [a, a\0). Keep compaction on unbounded SSTable iteration; use the bounded iterator only for user-facing range/prefix scans.

</spec-entry>

<spec-entry category="coding" keywords="planner,primary-key,range,scan,explain" date="2026-07-08" title="Conjunctive primary-key range scan planning" description="Safe SQL AND PK range lowering to storage scan_range" source="main@8a12c0f">

### Conjunctive primary-key range scan planning

Single-table SELECT can lower AND-connected primary-key range comparisons into one storage scan_range only when every conjunct is a comparison between the same primary-key column and a constant/parameter expression using <, <=, >, or >=. Merge the tightest lower and upper row-id bounds; use exclusive half-open storage ranges, with > and <= represented by appending NUL to the encoded row id. Mark the selection fully applied even when the range is empty so empty bounded ranges do not fall back to full table scan. EXPLAIN must use schema.get_primary_key_index(), not a hard-coded column 0.

</spec-entry>

<spec-entry category="coding" keywords="sstable,block-properties,range,lower-bound,mvcc" date="2026-07-08" title="SSTable block properties lower-bound skip" description="MVCC-safe SSTable per-block metadata and lower-bound block skip" source="main@8a12c0f">

### SSTable block properties lower-bound skip

SSTable metadata now includes per-block properties with offset, first_key, last_key, and entry_count while keeping legacy meta decode fallback for old SSTables. Iterators and find_ge use block last_key properties to choose the first possible block for a lower bound; if block properties are missing or offset-aligned validation fails, they fall back to the old first-key binary-search behavior. This is MVCC-safe because it only skips blocks whose last internal key is strictly before the raw lower bound. It does not use SQL predicate zone maps before visible-version merge.

</spec-entry>

<spec-entry category="coding" keywords="sstable,metadata,versioning,block-properties,compatibility" date="2026-07-08" title="Versioned SSTable block properties metadata" description="Versioned and backward-compatible SSTable block properties metadata" source="main@8a12c0f">

### Versioned SSTable block properties metadata

SSTable meta now writes format_version=2 with per-block properties. decode_meta is fail-open across three shapes: v2 current meta, v1 block_properties without explicit version, and v0 legacy first_key/last_key only. Block-property-based lower-bound skipping must validate property offsets match index_offsets before skipping; otherwise fall back to first-key binary search. Future extensions such as user-key timestamp intervals and prefix Bloom should continue this versioned/fail-open pattern.

</spec-entry>

<spec-entry category="coding" keywords="sstable,prefix-bloom,filter,versioning,mvcc" date="2026-07-08" title="SSTable versioned prefix Bloom filter" description="Versioned whole-key plus optional user-key table-prefix Bloom filter" source="main@8a12c0f">

### SSTable versioned prefix Bloom filter

SSTable filter blocks now write format_version=2 with a whole_key_filter plus optional table-prefix Bloom. decode_filter_block is fail-open: current v2 uses prefix metadata, unknown wrapper versions keep whole-key filtering but disable prefix skip, and legacy bare BloomFilter blocks decode with no prefix filter. Fusion SSTable builders enable user-key prefix filtering with suffix_len=TS_SIZE, so prefixes are extracted from decoded MVCC user keys. prefix_may_match only returns false for supported table/shard table prefixes and otherwise returns true.

</spec-entry>

<spec-entry category="coding" keywords="mvcc,compaction,sstable,snapshot" date="2026-07-08" title="MVCC-safe SSTable compaction" description="Compaction must not drop MVCC versions without a safe read timestamp watermark" source="main@8a12c0f">

### MVCC-safe SSTable compaction

FusionDB SSTable compaction must preserve every MVCC internal-key version unless a future version-GC implementation has an explicit oldest-active-read-ts watermark. A 2026-07-09 regression proved the previous same-user-key dedup broke existing transactions: transaction A read at old read_ts, transaction B wrote a newer version, snapshot/compaction kept only the new version, and transaction A returned None instead of the old value. compact_once_inner now preserves all versions and only merges files; compaction_dropped_version_count remains reserved for future safe GC. Test: storage::fusion::tests::fusion_compaction_preserves_versions_visible_to_existing_snapshot.

</spec-entry>

<spec-entry category="coding" keywords="mvcc,compaction,version-gc,read-ts" date="2026-07-08" title="Active read-ts watermark for MVCC version GC" description="Active transaction read timestamp tracking enables safe SSTable version GC" source="main@8a12c0f">

### Active read-ts watermark for MVCC version GC

FusionDB now tracks active transaction read_ts values in FusionStorage.active_read_timestamps. begin_transaction registers the read_ts and FusionTransaction::drop unregisters it; manually constructed internal transactions must set read_ts_registered=false. SSTable compaction uses oldest_active_read_ts as the safe GC watermark: with no active readers it keeps only the newest version for each user key; with active readers it keeps the newest version, all versions with ts greater than the oldest active read_ts, and the first floor version with ts <= oldest active read_ts. This preserves every existing snapshot while allowing obsolete versions to be reclaimed after readers release. Tests: fusion_compaction_preserves_versions_visible_to_existing_snapshot and fusion_get_uses_latest_mvcc_timestamp_after_compaction.

</spec-entry>

<spec-entry category="coding" keywords="sstable,block-properties,prefix-filter,zone-map,mvcc" date="2026-07-08" title="SSTable block table-prefix property filtering" description="Per-block table-prefix metadata skip with fail-open compatibility" source="main@8a12c0f">

### SSTable block table-prefix property filtering

SSTable meta v3 stores per-block table_prefixes extracted from MVCC user keys when the builder has a user-key suffix length. Iterator block-prefix filtering is enabled only for exact table-prefix user-key ranges where upper_bound == prefix_end(start_user_key). It validates block_properties length and offset alignment, skips only when the target prefix is absent from a non-empty per-block prefix set, and fail-opens on missing/misaligned/empty/unknown metadata. Legacy v2 block properties keep lower-bound skip but clear table_prefixes; unknown same-shape meta clears optional block properties. Metrics are exported as sstable_block_prefix_filter_check/positive/skip/fail_open counters.

</spec-entry>

<spec-entry category="coding" keywords="sstable,user-key-bloom,mvcc,point-get,fail-open" date="2026-07-08" title="SSTable MVCC user-key Bloom filter" description="Fail-open semantics for SSTable MVCC user-key Bloom point-get optimization" source="main@8a12c0f">

### SSTable MVCC user-key Bloom filter

SSTable filter block v3 may include optional user_key_filter in addition to whole_key_filter and table prefix filter. Point get must probe user_key_filter before find_ge only as a negative read-amplification guard: MayMatch continues, NoMatch skips that SSTable, FailOpen continues to find_ge. The probe must validate extractor_id and the caller-provided expected MVCC suffix length; suffix mismatch, missing filter, unknown extractor, zero expected suffix, or legacy/v2 filter all fail open. Builder add_key inserts the decoded user key (internal key without suffix) into user_key_filter and disables optional prefix/user-key filters if it ever sees a key shorter than the configured suffix, while whole_key_filter remains mandatory.

</spec-entry>

<spec-entry category="coding" keywords="sstable,block-cache,no-fill,read-options,compaction" date="2026-07-08" title="SSTable no-fill block cache read policy" description="Explicit no-fill read policy for SSTable bulk scans" source="main@8a12c0f">

### SSTable no-fill block cache read policy

SSTable reads now support explicit SsTableReadOptions with fill_cache true by default and no_fill_cache for bulk scans. read_block_at still checks the shared block cache first; on miss it performs the same file read, CRC/decode/decompress path, then inserts only when fill_cache=true. With fill_cache=false it increments block_cache_fill_skip_count and returns the decoded block without populating block cache. Point get/find_ge and normal public iterators keep default fill-cache behavior. Internal startup max-timestamp restore and compaction input iteration use no_fill_cache to avoid polluting the hot read cache. Future SQL scan no-fill must be explicit and should pass options through merge_visible_range instead of changing default iterator behavior.

</spec-entry>

<spec-entry category="coding" keywords="sstable,file-handle,iterator,find-ge,block-read" date="2026-07-08" title="SSTable local file-handle reuse for cold block reads" description="Local file handle reuse for SSTable iterator and point lower-bound reads" source="main@8a12c0f">

### SSTable local file-handle reuse for cold block reads

SSTable cold block reads now support local file-handle reuse in iterator and find_ge paths. read_block_at_with_reusable_file checks the block cache before opening any file; on cache miss it lazily opens a tokio::fs::File once per reader, reuses it for seek/read_exact, and increments sstable_block_file_open_count only after a successful open. read_block_bytes records sstable_block_read_bytes after successful read_exact. Default direct read_block/read_block_with_options keeps the previous direct path, while SsTableIterator and find_ge use a local Option<File> to reduce repeated File::open calls without sharing a seek cursor across tasks. Cache hit/miss/insert/fill-skip and CRC/decode semantics are unchanged.

</spec-entry>

<spec-entry category="coding" keywords="topk,index,order-by,btree,planner,not-null" date="2026-07-08" title="Secondary BTree ASC ordered Top-K path" description="Safe pure ORDER BY indexed_col ASC LIMIT path for single-column secondary BTree" source="codex:index-topk-asc-2026-07-09">

### Secondary BTree ASC ordered Top-K path

For pure single-table SELECT without WHERE, FusionDB now has a narrowly gated ordered secondary index Top-K path. It only fires through streaming_order_limit eligibility, so projection-alias ORDER BY still falls back. The safe subset is single ORDER BY column, ASC, non-sharded executor, non-primary single-column BTree, NOT NULL, and types with existing order-preserving secondary index value decode: integer/date/timestamp/datetime/interval. The path uses scan_secondary_index_range over the whole column prefix with limit+offset, preserves ordered_row_ids, reuses covered/INCLUDE rows when projection permits, and sets rows_satisfy_order_by so query sort is skipped while final LIMIT/OFFSET trimming remains centralized.

</spec-entry>

<spec-entry category="coding" keywords="topk,explain,index,orderby" date="2026-07-09" title="EXPLAIN visibility for secondary BTree ordered Top-K" description="EXPLAIN reports ASC secondary BTree ORDER BY/LIMIT index path only when execution can use it" source="main@8a12c0f">

### EXPLAIN visibility for secondary BTree ordered Top-K

EXPLAIN now considers the same safe ASC secondary BTree ORDER BY/LIMIT slice as execution when there is no WHERE selection: single-table, no joins, no DISTINCT/HAVING/non-empty GROUP BY, no aggregate/window projection, streaming ORDER BY not resolved through projection alias, LIMIT/OFFSET present, non-sharded, single-column non-primary NOT NULL BTree, and supported ordered key type via Executor::secondary_index_order_type_supported. It reports Access Path: Index Scan using ordered secondary BTree on <column> (ORDER BY/LIMIT, rows <= limit+offset). Fallback remains Full Table Scan for nullable columns and projection alias ambiguity.

</spec-entry>

<spec-entry category="coding" keywords="topk,index,types,boolean,date32,timestamptz,interval" date="2026-07-09" title="Secondary BTree ordered Top-K safe type expansion" description="ASC ordered secondary BTree Top-K expanded to safe order-preserving type aliases" source="main@8a12c0f">

### Secondary BTree ordered Top-K safe type expansion

ASC ordered secondary BTree Top-K now supports additional order-preserving NOT NULL single-column BTree types by reusing the central type helpers: BOOL/BOOLEAN, DATE32, TIMESTAMPTZ/TIMESTAMP WITH TIME ZONE/TIMESTAMP(n), DATETIME(n), and INTERVAL qualifiers. The index key decode path now reconstructs Boolean, DATE32, timestamp/datetime aliases, and interval qualifiers for covered/index-only rows. TEXT/VARCHAR, DECIMAL/NUMERIC, FLOAT/DOUBLE, pseudo prefixes such as TIMESTAMPZ, and unsupported time/blob/vector/json-like types remain rejected. Execution and EXPLAIN continue to share Executor::secondary_index_order_type_supported.

</spec-entry>

<spec-entry category="coding" keywords="reverse,storage,memory,topk,desc" date="2026-07-09" title="Memory reverse range scan foundation" description="Memory-backed scan_range_reverse contract and limitations" source="main@8a12c0f">

### Memory reverse range scan foundation

Transaction::scan_range_reverse is now the public storage contract for descending visible-key range reads. The trait default is correctness-only and materializes the forward scan before reversing, so it must not be used as a performance gate for SQL DESC Top-K. MemoryTransaction overrides it with a direct descending BTreeMap merge across committed data and the transaction write buffer; write-buffer tombstones shadow committed rows, write-buffer-only tombstones are skipped without consuming limit, and limit is applied only after visible rows are emitted. Fusion and SSTable still need bounded reverse implementations before DESC SQL/EXPLAIN can be opened.

</spec-entry>

<spec-entry category="coding" keywords="reverse,sstable,storage,topk,mvcc" date="2026-07-09" title="SSTable user-key bounded reverse iterator" description="Bounded reverse SSTable internal iterator contract" source="main@8a12c0f">

### SSTable user-key bounded reverse iterator

SSTable now exposes new_user_key_range_reverse_iterator and _with_options for raw/internal KV reverse scans over decoded user-key half-open bounds [lower, upper). The iterator starts at the last block whose first decoded user key is below the exclusive upper bound, scans blocks backward, filters every entry by decoded user key, stops before lower blocks when aligned block properties prove last_key < lower, preserves no-fill cache and per-iterator file-handle reuse through read_block_at_with_reusable_file, and uses block-prefix properties as fail-open skip hints. It intentionally returns internal entries only; Fusion must still group versions by user key and apply MVCC/tombstone visibility before using scan_range_reverse for SQL DESC Top-K.

</spec-entry>

<spec-entry category="coding" keywords="fusion,reverse,mvcc,storage,topk" date="2026-07-09" title="Fusion reverse visible MVCC merge" description="Fusion scan_range_reverse visible merge contract" source="main@8a12c0f">

### Fusion reverse visible MVCC merge

FusionTransaction now overrides scan_range_reverse with a true descending visible-row merge instead of the Transaction trait's forward-materialize fallback. The implementation snapshots active/immutable memtables and SSTables once, creates per-source reverse cursors, drains all internal versions for each user key inside each source, chooses write-buffer entries first or the newest ts <= read_ts candidate, treats tombstone winners as deleted rows, and consumes limit only after emitting visible PUT rows. last() now delegates to scan_range_reverse(start,end,Some(1)), removing the prior two-block SSTable heuristic that could stop on a high-key tombstone.

</spec-entry>

<spec-entry category="coding" keywords="reverse,storage,planner,capability" date="2026-07-09" title="Transaction bounded reverse scan capability gate" description="Guard against using materializing reverse fallback as fast path" source="main@8a12c0f">

### Transaction bounded reverse scan capability gate

Transaction now exposes supports_bounded_scan_range_reverse(), defaulting to false because the trait scan_range_reverse fallback materializes the forward range before reversing. MemoryTransaction and FusionTransaction return true after their direct reverse merge implementations. Planner and EXPLAIN DESC ordered-index fast paths must require this capability before treating scan_range_reverse as a performance path.

</spec-entry>

<spec-entry category="coding" keywords="topk,desc,index,planner,reverse" date="2026-07-09" title="Secondary BTree DESC ordered Top-K path" description="Bounded DESC secondary BTree ORDER BY/LIMIT execution path" source="main@8a12c0f">

### Secondary BTree DESC ordered Top-K path

SQL execution now enables the existing single-column secondary BTree ORDER BY/LIMIT fast path for DESC when the transaction reports supports_bounded_scan_range_reverse(). scan_secondary_index_range uses txn.scan_range_reverse for order_direction=false, pushes the same limit+offset row goal used by ASC, preserves scan order without materialize-then-reverse, and keeps the existing narrow gate: no shard router, single ORDER BY expression, non-primary single-column BTree, NOT NULL, and order-preserving supported types. Range and BETWEEN secondary index scans also treat DESC as ordered only when bounded reverse capability is present; otherwise they fall back to unordered index candidates plus normal query sorting.

</spec-entry>

<spec-entry category="coding" keywords="distinct,count-distinct,index,btree,explain,benchmark" date="2026-07-09" title="Secondary BTree DISTINCT key stream" description="No-WHERE secondary BTree index-key stream for DISTINCT and COUNT DISTINCT" source="main@8a12c0f">

### Secondary BTree DISTINCT key stream

FusionDB now has a narrowly gated no-WHERE single-table secondary BTree key-stream path for COUNT(DISTINCT col) and SELECT DISTINCT col. COUNT(DISTINCT) accepts non-primary single-column BTree columns whose data type has stable index key encoding, including integer/boolean/date/timestamp/interval/text/decimal, and counts adjacent distinct value keys without reading data rows; NULL remains excluded because secondary BTree entries are not written for NULL. SELECT DISTINCT is narrower: non-sharded executor, non-primary single-column BTree, NOT NULL column, and existing order-preserving key decode types only, then returns values from the index key stream and falls back for nullable/string/decimal/WHERE cases. EXPLAIN reports Access Path: Index Scan using distinct secondary BTree on <col> with DISTINCT key stream or COUNT DISTINCT key stream only under the same no-WHERE safety envelope. benchmark.py adds non-default BENCH_MATRIX=index_distinct / Part 21 to compare full-scan baseline, indexed DISTINCT, indexed COUNT DISTINCT, ORDER LIMIT, and nullable fallback with per-case metrics.

</spec-entry>

<spec-entry category="coding" keywords="groupby,count,index,btree,explain,benchmark" date="2026-07-09" title="Secondary BTree GROUP BY COUNT key stream" description="No-WHERE secondary BTree key-stream run-length GROUP BY COUNT(*)" source="main@8a12c0f">

### Secondary BTree GROUP BY COUNT key stream

FusionDB now extends the secondary BTree index-key stream pattern from DISTINCT/COUNT DISTINCT to low-cardinality SELECT group_col, COUNT(*) FROM table GROUP BY group_col. The execution gate is single table, no WHERE predicate, no join, no HAVING, non-primary single-column secondary BTree, NOT NULL group column, and a group key type that can be reconstructed from the index key. The run-length scan reads the BTree value_key stream with Transaction::scan_prefix_parallel, decodes each new group key from the index key, counts adjacent entries, and falls back to the existing row-scan HashMap path for nullable columns, predicates, unsupported types, primary keys, sharded executors, or malformed keys. EXPLAIN reports Access Path: Index Scan using group secondary BTree on <col> (GROUP BY COUNT key stream) under the same no-WHERE safety envelope. benchmark.py Part 21 / BENCH_MATRIX=index_distinct now includes full-scan, indexed, and nullable fallback GROUP BY COUNT cases alongside DISTINCT and COUNT DISTINCT. This is a tight index scan rather than a true loose/skip scan: the current Transaction API still materializes all index entries for a prefix, so future work should add streaming/visitor index prefix scan and eventually skip-to-next-distinct-key support.

</spec-entry>

<spec-entry category="coding" keywords="distinct,count-distinct,groupby,index,visitor,streaming" date="2026-07-09" title="Streaming routed index prefix visitor for key streams" description="Secondary BTree key-stream scans now use routed prefix visitors instead of materializing scan_prefix_parallel results" source="main@streaming-prefix-2026-07-09">

### Streaming routed index prefix visitor for key streams

DISTINCT, COUNT(DISTINCT), and low-cardinality GROUP BY COUNT secondary BTree key-stream paths now avoid per-prefix Vec materialization. Executor::scan_routed_prefixes_for_each tries Transaction::scan_prefix_parallel_for_each first and falls back to serial scan_prefix_for_each when the storage engine returns None, preserving key order and early-stop semantics. The SQL key-stream functions still keep the existing non-sharded, no-WHERE safety gates and remain tight O(N index entries) scans, not loose skip scans. If an index key cannot be parsed or decoded, the visitor marks fallback_required, stops, returns Ok(None), and discards partial count/rows so the existing full-row scan path produces the result.

</spec-entry>

<spec-entry category="coding" keywords="distinct,count-distinct,index,loose-scan,skip-scan,first,btree" date="2026-07-09" title="Secondary BTree DISTINCT loose key seek" description="Safe order-type DISTINCT/COUNT DISTINCT now skip duplicate index value runs with Transaction::first" source="main@loose-distinct-2026-07-09">

### Secondary BTree DISTINCT loose key seek

COUNT(DISTINCT) and SELECT DISTINCT over safe single-column secondary BTree key types now use a first(start,end)-driven loose key seek instead of tight streaming all index entries. The loop enumerates visible value_key groups by calling Transaction::first over the index prefix range, emits/counts the current value_key, then seeks to prefix + value_key + ':' + 0xFF to skip all row ids for that value. The gate is intentionally narrower than COUNT DISTINCT key-stream support: only secondary_index_order_type_supported types use loose seek because TEXT index keys are not separator-escaped and values like a:b and a:b:c would be skipped incorrectly by prefix + value + ':' + 0xFF. TEXT/DECIMAL COUNT(DISTINCT) remains on the tight streaming index key path, preserving colon correctness and index-only execution. GROUP BY COUNT remains tight run-length streaming because exact per-group counts still require visiting each visible index entry unless a future run-count summary is added. EXPLAIN and benchmark labels now distinguish loose key seek from key stream.

</spec-entry>

<spec-entry category="coding" keywords="metrics,index,distinct,groupby,benchmark" date="2026-07-09" title="Secondary index key scan observability counters" description="Counters and semantics for secondary-index loose seek vs key-stream observability" source="main@8a12c0f">

### Secondary index key scan observability counters

FusionDB exposes monotonic SQL secondary-index scan counters for loose DISTINCT seek and tight key-stream paths. Metrics: index_loose_seek_count counts each Transaction::first probe including terminal misses; index_loose_value_count counts emitted distinct value groups; index_loose_run_skip_count counts advances to the next value run, not duplicate entries skipped; index_key_stream_entry_visit_count counts tight key-stream secondary-index entries visited. Expose the counters in /metrics JSON and /metrics/prometheus, include them in benchmark.py METRIC_COUNTER_KEYS, and derive per-query metadata so benchmark evidence can distinguish O(NDV) loose seek from O(index entries) tight stream.

</spec-entry>

<spec-entry category="coding" keywords="groupby,index,stats,planner,explain" date="2026-07-09" title="Stats-aware GROUP BY COUNT index key-stream gate" description="ANALYZE-backed negative cost gate for GROUP BY COUNT secondary index key-stream" source="main@8a12c0f">

### Stats-aware GROUP BY COUNT index key-stream gate

GROUP BY COUNT secondary BTree key-stream remains a tight O(visible index entries) scan, so FusionDB now uses ANALYZE table stats as a negative performance gate after the existing semantic gate. If table stats are absent or column stats are missing, the path fails open to preserve current index-only behavior and large no-stats benchmark wins. If stats exist, non-null index entries are estimated as row_count - column_stats.null_count, and the index key-stream path is allowed only when that count is at least 65,536 entries; smaller analyzed tables fall back to the row-scan HashMap path. EXPLAIN reuses the same helper so analyzed small tables do not advertise GROUP BY COUNT key stream when execution will fall back. Do not use distinct_count to disable this tight path yet: NDV affects output rows but the path still visits every index entry, and high-NDV full scans still pay row decode plus HashMap cost.

</spec-entry>

<spec-entry category="coding" keywords="groupby,count,summary,fallback,metrics" date="2026-07-09" title="GROUP BY COUNT summary fallback rules" description="Parser, fallback, and metrics rules for GROUP BY COUNT summary scan" source="main@8a12c0f">

### GROUP BY COUNT summary fallback rules

The GROUP BY COUNT summary scan must strip the fixed index_count prefix and treat the remaining suffix as value_key, because TEXT value keys may contain ':'. It must reuse secondary_index_group_value_from_key for decoding, reject non-positive or malformed counts, check scanned total_entries and group_count against v1 metadata, and return Ok(None) on any malformed/missing/legacy summary so existing key-stream or full-row fallback handles correctness. Summary visits are counted with index_group_count_summary_entry_visit_count and must not be mixed into index_key_stream_entry_visit_count.

</spec-entry>

<spec-entry category="coding" keywords="startup,recovery,sstable,timestamp,cache" date="2026-07-09" title="SSTable timestamp recovery cache" description="Per-SSTable timestamp recovery cache and empty memtable flush skip" source="main@8a12c0f">

### SSTable timestamp recovery cache

FusionStorage startup now uses data/sstables/_fusiondb_sstable_ts_cache.json to cache per-SSTable max MVCC timestamp keyed by SSTable id plus file length and mtime fingerprint. Cache hits avoid the previous full data-block iterator scan for timestamp restore; cache misses retain the safe legacy full scan and then persist the cache. New SSTables produced by flush, shutdown flush, and compaction immediately write their cache entry. Empty memtables are skipped during flush/shutdown so clean startup/shutdown no longer creates metadata-only SSTables. The vector index background rebuild now yields startup scheduling briefly before scanning, so it does not run before server bind once storage init returns.

</spec-entry>

<spec-entry category="coding" keywords="startup,sstable,open,parallel,metadata" date="2026-07-09" title="Parallel SSTable open for warm startup" description="Concurrent startup SSTable open and block_properties move" source="main@8a12c0f">

### Parallel SSTable open for warm startup

FusionStorage startup opens existing SSTables concurrently with tokio tasks, then sorts successfully opened handles by SSTable id before registering them. SsTable::open now moves decoded block_properties into the runtime Arc instead of cloning and retaining a duplicate inside meta; opened table tests should inspect table.block_properties for runtime block metadata. This lowers warm startup metadata load overhead without changing data-block reads, timestamp restore semantics, or query behavior.

</spec-entry>

<spec-entry category="coding" keywords="startup,sstable,open,metrics,metadata" date="2026-07-09" title="SSTable open phase metrics" description="Per-phase SSTable open observability counters" source="main@8a12c0f">

### SSTable open phase metrics

SSTable open now records cumulative startup/open counters in monitor metrics: open count, total open microseconds, index/filter/meta bytes, read microseconds, decode microseconds, decoded index entries, and decoded block property count. The counters are exported through /metrics JSON and /metrics/prometheus so startup benches can separate I/O from decode CPU and quantify lazy metadata work.

</spec-entry>

<spec-entry category="coding" keywords="sstable,descriptor-cache,lazy-meta,startup" date="2026-07-09" title="SSTable descriptor cache for lazy startup meta" description="Skip SSTable meta/block_properties decode during warm startup" source="main@8a12c0f">

### SSTable descriptor cache for lazy startup meta

FusionDB SSTable warm startup now uses _fusiondb_sstable_descriptor_cache.json keyed by SSTable id and file fingerprint. On cache hit, SsTable::open_with_descriptor builds meta from first_key/last_key/format_version and skips reading/decoding the large meta block. block_properties are stored behind OnceLock and preloaded asynchronously after startup; before preload, block-level pruning fail-opens for correctness.

</spec-entry>

<spec-entry category="coding" keywords="sstable,index,startup,decode,flat-vector" date="2026-07-09" title="SSTable direct vector index runtime" description="Avoid BTreeMap construction during SSTable index open and write versioned flat index for new files" source="main@8a12c0f">

### SSTable direct vector index runtime

FusionDB SSTable open now decodes legacy bincode map index bytes with a custom serde MapAccess visitor directly into runtime index_keys/index_offsets, avoiding construction of a BTreeMap during startup. SsTable no longer exposes a runtime index map; tests and benches should use index_offset_for(key). New SSTables are written with a versioned FIDX flat-vector index block while legacy BTreeMap index decode remains supported.

</spec-entry>

<spec-entry category="coding" keywords="sstable,index,sidecar,startup,cache,metrics" date="2026-07-09" title="SSTable runtime index sidecar cache" description="Skip canonical index decode on warm startup using validated per-SSTable sidecar cache" source="main@8a12c0f">

### SSTable runtime index sidecar cache

FusionDB SSTable open now treats per-SSTable *.idxcache files as derived runtime-index sidecars. After footer validation, open tries FICX v1 sidecar keyed by file length, mtime, and index_len; a hit materializes index_keys/index_offsets without decoding the legacy SSTable index block. Missing, stale, invalid, or corrupt sidecars fail open to the canonical SSTable index decode and then rewrite the sidecar atomically via same-directory tmp+rename. Compaction obsolete cleanup removes sidecars with their SSTable. Metrics expose hit/miss/stale/invalid/write/write_error counters.

</spec-entry>

<spec-entry category="coding" keywords="sstable,index,sidecar,ficx,checksum,validation" date="2026-07-09" title="SSTable index sidecar FICX v3 validation" description="FICX v3 sidecar checksum and stronger offset/fingerprint validation" source="main@8a12c0f">

### SSTable index sidecar FICX v3 validation

FICX runtime index sidecar cache is now version 3. The fingerprint includes file length, mtime, index_offset, filter_offset, meta_offset, and index_len. The sidecar payload stores a CRC32 over entry_count and key/offset entries; checksum mismatch is invalid and falls back to canonical index decode. Decode rejects oversized payload entry counts, non-increasing keys, non-increasing offsets, and offsets outside the SSTable data region. SSTable open also validates canonical index vectors against the data-region boundary before use. Sidecar persistence writes a same-directory temp file, sync_data, then rename.

</spec-entry>

<spec-entry category="coding" keywords="manifest,flush,compaction,wal,obsolete" date="2026-07-09" title="SSTable manifest install ordering" description="Manifest update order for flush, shutdown, and compaction" source="main@8a12c0f">

### SSTable manifest install ordering

Flush and shutdown paths register new SSTables through register_live_sstable, which updates the in-memory live set and writes MANIFEST/CURRENT; WAL truncation is skipped or errors if the manifest write failed. Compaction builds the next live set, persists MANIFEST/CURRENT first, then swaps the in-memory live set and only then queues old SSTables for obsolete deletion. Manifest writes use same-directory tmp files, file sync, rename, and directory sync. Derived timestamp/descriptor/index caches remain non-authoritative.

</spec-entry>

<spec-entry category="coding" keywords="manifest,metrics,startup,sstable,observability" date="2026-07-09" title="SSTable manifest startup observability" description="Manifest startup metrics exported via /metrics and Prometheus" source="main@8a12c0f">

### SSTable manifest startup observability

FusionDB now exposes manifest startup counters through monitor and HTTP metrics: sstable_manifest_load_count, sstable_manifest_load_total_us, sstable_manifest_load_error_count, sstable_manifest_live_file_count, sstable_manifest_legacy_scan_count, sstable_manifest_legacy_scan_candidate_count, and sstable_manifest_open_error_count. FusionStorage::with_config records successful CURRENT/MANIFEST loads, strict load errors, legacy directory scans, candidate counts, and startup SSTable open errors. These counters make normal manifest startup distinguishable from legacy scan/repair paths.

</spec-entry>

<spec-entry category="coding" keywords="wal,replay,startup,metrics,prometheus" date="2026-07-09" title="WAL replay startup observability" description="WAL replay startup metrics and strict replay failure behavior" source="main@8a12c0f">

### WAL replay startup observability

FusionDB startup WAL replay now records replay count, elapsed microseconds, segment count, replay bytes, entry count, put/delete split, partial-tail count, truncate count, error count, apply count, apply elapsed microseconds, and max replay timestamp. Replay failures during startup return FusionError instead of panicking; partial tails on the latest WAL segment are truncated only if set_len succeeds. Export the counters through HTTP JSON metrics and Prometheus before using them as benchmark gates.

</spec-entry>

<spec-entry category="coding" keywords="wal,replay,cursor,floor,metrics" date="2026-07-09" title="WAL replay cursor foundation" description="Replay summary cursor and valid-byte metrics for future WAL floor" source="main@8a12c0f">

### WAL replay cursor foundation

FusionDB WAL replay now has replay_with_summary(), returning decoded entries plus a WalReplayCursor { segment_id, offset } for the last complete replayed record and WalReplayStats including valid_bytes, last_segment_id, and last_valid_offset. replay() preserves the old API by returning only entries. Metrics export wal_replay_valid_bytes, wal_replay_last_segment_id, and wal_replay_last_valid_offset through JSON/Prometheus and benchmark deltas. This cursor is only a candidate for a future durable WAL floor; it must not drive physical WAL deletion until a synced manifest edit/CURRENT transition publishes it durably.

</spec-entry>

<spec-entry category="coding" keywords="manifest,record,crc32c,framing,recovery" date="2026-07-09" title="Manifest record physical log foundation" description="Manifest v2 physical record writer/reader foundation" source="main@8a12c0f">

### Manifest record physical log foundation

FusionDB now has src/storage/manifest_record.rs as the manifest v2 physical record layer. It implements LevelDB/Pebble-style legacy framing with 32 KiB blocks, 7-byte little-endian masked CRC32C + payload length + type headers, CRC32C Castagnoli over type||payload, FULL/FIRST/MIDDLE/LAST fragmentation, zero padding for 1..6 byte trailers, zero-length FIRST when exactly 7 bytes remain before a non-empty logical record, append-mode block offset initialization from existing_len, explicit Strict vs RecoverTornTail reader modes, structured read_all output with records/valid_bytes/recovered_tail, and no automatic file truncation. This layer is intentionally independent from VersionEdit, SSTable, CURRENT, and the existing WAL entry format.

</spec-entry>

<spec-entry category="coding" keywords="manifest,edit,versionedit,wal_floor,high_watermark" date="2026-07-09" title="Manifest edit binary schema foundation" description="Manifest v2 logical edit schema and replay state foundation" source="main@8a12c0f">

### Manifest edit binary schema foundation

FusionDB now has src/storage/manifest_edit.rs as the manifest v2 logical VersionEdit payload layer over manifest_record. Each logical payload uses stable magic FMED, version 1, explicit edit tag, fixed little-endian numeric fields, u32 length-prefixed bounded strings/byte arrays, and no bincode/Rust enum disk layout. Supported edits: Snapshot, AddSstable, DeleteSstable, Compact, SetNextFileNumber, SetHighWatermark, and SetWalReplayFloor. SSTable metadata includes file id/name, legacy file fingerprint, first_key, last_key, format_version, max_ts, and content_fingerprint. WAL replay floor includes wal_generation, segment_id, and offset to avoid reusing segment 0 after truncate. Replay produces ManifestVersionState with BTreeMap live files and validates unique live files, next_file_number above live ids, monotonic high_watermark, high_watermark covering live SST max_ts, and monotonic WAL floor.

</spec-entry>

<spec-entry category="coding" keywords="manifest,current,atomic,fsync" date="2026-07-09" title="Manifest log CURRENT atomic foundation" description="Binary manifest_log/CURRENT atomic persistence rules" source="main@8a12c0f">

### Manifest log CURRENT atomic foundation

FusionDB manifest_log v2 uses MANIFEST-N files with manifest_record/manifest_edit payloads. Creating a manifest requires a Snapshot first edit, writes a same-directory unique temp file with create_new, flushes the record writer, file.sync_all()s the manifest, renames to the final MANIFEST-N name, then syncs the directory before CURRENT can be installed. CURRENT installation writes a same-directory unique temp CURRENT file, file.sync_all()s it, atomically renames it to CURRENT, and syncs the directory. CURRENT parsing is strict: only one MANIFEST-N line is accepted, with optional LF or CRLF terminator; embedded newlines, path separators, '..', invalid names, and missing target manifest files are errors.

</spec-entry>

<spec-entry category="coding" keywords="manifest,edit,sstable,validation" date="2026-07-09" title="Manifest edit canonical SSTable entry validation" description="Canonical SSTable entry validation for manifest v2" source="main@8a12c0f">

### Manifest edit canonical SSTable entry validation

Manifest v2 now validates ManifestSstableEntry at encode, decode, and state-apply boundaries. The SSTable file_name must be a canonical base name exactly matching '<id>.sst'; empty names, path separators, '..', CR/LF, and id/file-name mismatches are rejected before writing, while reading disk payloads, and while applying manually constructed edits. This mirrors the existing JSON manifest canonical SSTable-name requirement and prevents v2 replay from accepting path traversal or mismatched SSTable descriptors.

</spec-entry>

<spec-entry category="coding" keywords="manifest,append,torn-tail,current,crash" date="2026-07-09" title="Manifest log append repairs recovered torn tail" description="Recovered torn-tail append repair and canonical MANIFEST names" source="main@8a12c0f">

### Manifest log append repairs recovered torn tail

manifest_log append now replays the target MANIFEST before appending. If replay reports recovered_tail, append truncates the file to valid_bytes, file.sync_all()s the repaired prefix, and only then opens append mode with ManifestRecordWriter::new_appending at valid_bytes. Middle corruption still returns an error from replay and is not repaired. manifest file names are also canonicalized: parse/validation accepts only names that round-trip through manifest_file_name(file_number), so MANIFEST-1 and non-canonical zero padding are rejected.

</spec-entry>

<spec-entry category="coding" keywords="manifest,edit,prefix,high-watermark,crash" date="2026-07-09" title="Manifest edit prefix legality enforcement" description="Per-edit prefix legality for manifest v2" source="main@8a12c0f">

### Manifest edit prefix legality enforcement

ManifestVersionState::apply now enforces full invariants after AddSstable and Compact, not only after Snapshot or final replay. AddSstable/Compact failures roll back inserted/removed files and next_file_number so failed applies do not pollute in-memory state. Until FusionDB adds a composite VersionEdit, callers must publish a sufficient SetHighWatermark before any AddSstable/Compact whose output max_ts would exceed the current manifest high_watermark. This guarantees each complete manifest edit prefix replays into a legal state.

</spec-entry>

<spec-entry category="coding" keywords="manifest,versionedit,composite,crash,wal" date="2026-07-09" title="Manifest composite VersionEdit foundation" description="Composite manifest VersionEdit edit foundation" source="main@8a12c0f">

### Manifest composite VersionEdit foundation

Manifest v2 now supports a composite VersionEdit edit tag. The payload contains delete_ids, add_files, optional next_file_number, optional high_watermark, and optional wal_replay_floor. Apply is atomic: it validates add entries, checks high_watermark and WAL floor monotonicity, removes deleted files, inserts added files, applies metadata, then validates full invariants. Any failure restores the previous ManifestVersionState. This gives future flush/compaction integration a single durable record that can advance live file delta, next file number, high watermark, and WAL replay floor together instead of relying on crash-sensitive split edits.

</spec-entry>

<spec-entry category="coding" keywords="manifest,current,rollover,torn-tail,recovery" date="2026-07-09" title="Manifest current recovered-tail rollover" description="CURRENT manifest recovered-tail rollover API" source="main@8a12c0f">

### Manifest current recovered-tail rollover

manifest_log now has recover_current_manifest_with_rollover(). It reads CURRENT, replays only the pointed MANIFEST, and returns unchanged when recovered_tail is false. When recovered_tail is true, it builds a Snapshot from the replayed valid ManifestVersionState, chooses the next available MANIFEST-N after the current file number without using orphan manifests as live state, writes and syncs the new manifest through write_manifest_file(), atomically installs CURRENT through install_current_file(), and returns a clean replay of the new manifest. Missing or corrupt CURRENT/MANIFEST still fails through normal replay and is not repaired by scanning.

</spec-entry>

<spec-entry category="coding" keywords="manifest,replay,snapshot,startup" date="2026-07-09" title="Manifest log requires snapshot-first replay" description="MANIFEST startup replay rejects empty files and non-Snapshot first edits" source="main@8a12c0f">

### Manifest log requires snapshot-first replay

Physical MANIFEST files must be non-empty and the first logical edit accepted by manifest_log::replay_manifest_path must be Snapshot. Keep this validation in the manifest_log layer so generic manifest_edit replay can remain usable for lower-level tests and tooling.

</spec-entry>

<spec-entry category="coding" keywords="manifest,fusionstorage,startup,sstable,current" date="2026-07-09" title="FusionStorage writes manifest v2 snapshots" description="FusionStorage persists SSTable live set through manifest v2 Snapshot/CURRENT" source="main@8a12c0f">

### FusionStorage writes manifest v2 snapshots

FusionStorage SSTable manifest persistence now writes manifest_log v2 Snapshot MANIFEST-N files and atomically installs CURRENT, while startup first tries v2 replay with recovered-tail rollover and then falls back to legacy JSON only for backwards compatibility. Each v2 entry carries canonical SSTable name, metadata fingerprint, first/last key, format_version, cached max_ts when available, and a deterministic metadata-derived content fingerprint.

</spec-entry>

<spec-entry category="coding" keywords="manifest,startup,descriptor,sstable,performance" date="2026-07-09" title="Manifest v2 carries startup SSTable descriptors" description="Startup uses v2 MANIFEST descriptors directly before descriptor cache fallback" source="main@8a12c0f">

### Manifest v2 carries startup SSTable descriptors

FusionStorage startup now represents manifest-selected live SSTables as SstableLiveFile entries with an optional SsTableOpenDescriptor. v2 manifest replay fills the descriptor from ManifestSstableEntry first_key, last_key, and format_version, so SsTable::open_with_descriptor can avoid relying on the derived descriptor cache on warm startup; legacy JSON and directory scan paths still use None and fall back to the existing cache/full-open behavior.

</spec-entry>

<spec-entry category="coding" keywords="manifest,versionedit,fusionstorage,flush,compaction" date="2026-07-09" title="FusionStorage append-only manifest VersionEdit" description="FusionStorage append-only MANIFEST integration rules" source="main@8a12c0f">

### FusionStorage append-only manifest VersionEdit

FusionStorage normal flush and compaction should persist SSTable live-set changes by appending one composite ManifestEdit::VersionEdit to the current v2 MANIFEST. Full Snapshot rewrite is reserved for initial v2 creation, legacy JSON conversion, torn-tail recovery, or future size-based rollover. The durable append must happen before publishing the new in-memory live SSTable set; WAL truncation/floor advancement is intentionally not changed by this integration.

</spec-entry>

<spec-entry category="coding" keywords="sstable,point-get,overlap,metrics,bloom" date="2026-07-09" title="FusionStorage point-get SSTable overlap skip" description="Point reads skip SSTables by user-key min/max before Bloom/find_ge" source="main@8a12c0f">

### FusionStorage point-get SSTable overlap skip

FusionTransaction::get now checks each SSTable's decoded user-key min/max range before counting a point probe, probing the MVCC user-key Bloom filter, or calling find_ge. If the requested user key is outside [sst_min_user_key, sst_max_user_key], it increments sstable_point_overlap_skip_count and skips the SSTable. This is a negative-only optimization and does not affect MVCC visibility because any file whose user-key interval excludes the key cannot contain any version of that key. JSON/Prometheus metrics and benchmark.py now expose sstable_point_overlap_skip_count and derived per-query/ratio fields.

</spec-entry>

<spec-entry category="coding" keywords="composite-index,topk,desc,reverse,order-by" date="2026-07-09" title="Composite BTree DESC ordered scan uses bounded reverse" description="Composite DESC Top-K uses bounded reverse range scan" source="main@8a12c0f">

### Composite BTree DESC ordered scan uses bounded reverse

Composite index ORDER BY/LIMIT execution now gates DESC ordered paths on txn.supports_bounded_scan_range_reverse(). When safe, range and prefix scans in try_composite_index_scan call txn.scan_range_reverse(start,end,remaining) directly, so LIMIT can be applied from the high end of the composite key range instead of scanning forward then reversing. If bounded reverse is unavailable, DESC is not reported as ordered and the caller falls back to normal sorting. Residual-filter safety is preserved: scan_limit is still only pushed when predicates are fully covered by equality/range components.

</spec-entry>

<spec-entry category="coding" keywords="composite-index,topk,explain,orderby,btree" date="2026-07-09" title="Composite ordered Top-K EXPLAIN gate" description="Composite BTree ORDER BY/LIMIT EXPLAIN visibility" source="main@8a12c0f">

### Composite ordered Top-K EXPLAIN gate

EXPLAIN now reports ordered composite BTree ORDER BY/LIMIT for single-table composite indexes only when the same bounded execution gate is met: alias-safe streaming ORDER BY/LIMIT, leading equality prefix, range predicate on the next ordered composite column, fully covered predicates, ordered composite metadata, and DESC requiring supports_bounded_scan_range_reverse(). The plan text includes the index name, order column, ASC/DESC, and rows <= limit+offset. Residual predicates remain fallback and do not advertise bounded Top-K.

</spec-entry>

<spec-entry category="coding" keywords="benchmark,topk,composite-index,orderby,explain" date="2026-07-09" title="Composite Top-K benchmark matrix" description="Part 20 composite Top-K benchmark coverage" source="main@8a12c0f">

### Composite Top-K benchmark matrix

benchmark.py Part 20 / BENCH_MATRIX=index_topk now includes composite BTree ORDER BY/LIMIT cases. Setup creates bench_topk_comp_scan and bench_topk_comp_idx with idx_bench_topk_comp_idx_host_ts(host_id, ts). Cases cover composite full-scan ASC baseline, ordered composite ASC, ordered composite DESC, upper-half range DESC, bounded middle-window DESC, residual fallback, missing-leading-prefix fallback, and mixed ORDER BY fallback. Composite queries rotate host_id per benchmark iteration to reduce query-result cache contamination. Composite report metadata records rows/hosts/range/fallback context plus a sample EXPLAIN access path and booleans for ordered composite BTree and ORDER BY/LIMIT.

</spec-entry>

<spec-entry category="coding" keywords="topk,metrics,ordered,index,benchmark" date="2026-07-09" title="Ordered Top-K observability counters" description="Ordered Top-K scan/visit/reverse/sort fallback metrics" source="main@2026-07-09">

### Ordered Top-K observability counters

FusionDB now exposes SQL ordered Top-K observability counters across monitor, HTTP metrics, and benchmark reports. Metrics are index_ordered_topk_scan_count, index_ordered_topk_entry_visit_count, index_ordered_topk_reverse_scan_count, and query_sort_fallback_count. Secondary and composite ordered BTree paths increment Top-K counters only when ORDER BY/LIMIT is actually satisfied by a bounded ordered index scan. Reverse scans increment the reverse counter, and expression or mixed-order ORDER BY sort fallbacks increment query_sort_fallback_count. Residual composite scans that preserve index order but cannot push a bounded Top-K limit are intentionally excluded from Top-K counters.

</spec-entry>

<spec-entry category="coding" keywords="fusion,topk,reverse,streaming,memtable" date="2026-07-09" title="Fusion reverse Top-K lazy memtable source" description="Lazy memtable source for Fusion reverse Top-K" source="main@2026-07-09,delegate:cdx-110950-05b4">

### Fusion reverse Top-K lazy memtable source

FusionTransaction::merge_visible_range_reverse no longer materializes each memtable reverse range into a VecDeque before heap merge. ReverseSource is lifetime-parameterized and stores a boxed lazy iterator for buffered/write-buffer and memtable sources, so LIMIT-driven visitor early-stop can stop before cloning the rest of a large memtable range. MVCC winner selection, tombstone handling, write-buffer priority, and per-user-key version drain remain centralized in ReverseSource::next_candidate. This is the first storage-side performance slice after ordered Top-K counters; it reduces hidden work that SQL-level index_ordered_topk_entry_visit_count could not see.

</spec-entry>

<spec-entry category="coding" keywords="storage,visitor,topk,range,streaming" date="2026-07-09" title="Transaction range visitor API for Top-K" description="Range visitor API and Top-K execution integration" source="main@2026-07-09,delegate:cdx-112159-3774,delegate:cdx-112159-31d8">

### Transaction range visitor API for Top-K

Transaction now exposes scan_range_for_each and scan_range_reverse_for_each in addition to prefix visitors. The trait defaults preserve compatibility by materializing through scan_range/scan_range_reverse, while MemoryTransaction and FusionTransaction override both methods with streaming visitors over their existing forward/reverse visible merge logic. SQL ordered Top-K execution now consumes secondary and composite index ranges through these visitors, so bounded ORDER BY/LIMIT paths no longer build an intermediate index_entries Vec before row-id extraction. RecordingCompositeTxn tests distinguish visitor calls from Vec scan calls to prevent silent fallback.

</spec-entry>

<spec-entry category="coding" keywords="fusion,reverse,topk,metrics,sstable,benchmark" date="2026-07-09" title="Fusion reverse raw work counters" description="Raw reverse storage counters for Top-K claim gates" source="main@8a12c0f">

### Fusion reverse raw work counters

FusionDB now exposes storage/raw reverse scan counters below SQL ordered Top-K: Fusion reverse scans, source opens, raw internal entries read, visible candidates, visible PUT rows, SSTable reverse iterator opens, reverse block reads, reverse block entry decodes, and reverse block entry yields. Fusion reverse iterator open increments when an overlapping SSTable reverse iterator is created after overlap/prefix gates. Benchmark reports include per-query metadata for these counters, enabling claim-mode gates to detect hidden reverse materialization below SQL-visible LIMIT entries.

</spec-entry>

<spec-entry category="coding" keywords="benchmark,topk,claim-mode,metrics,gate" date="2026-07-09" title="Part 20 BENCH_CLAIM_MODE Top-K gates" description="Top-K benchmark pass/fail gate" source="main@8a12c0f">

### Part 20 BENCH_CLAIM_MODE Top-K gates

benchmark.py now supports BENCH_CLAIM_MODE for Part 20 index_topk. The gate is counter-first, not latency-first: positive ordered Top-K paths must have measured query_count == success_count == planned_iters, expected rows, ordered scan count equal to query_count, ordered entry visits bounded by LIMIT, zero query sort fallback, and correct reverse scan count for ASC vs DESC. DESC positive paths also require Fusion reverse scans equal query_count, visible PUTs bounded by LIMIT, and raw reverse reads bounded by max(3*LIMIT,64) per query. Fallback paths must not report ordered Top-K scans/visits/reverse scans; expression and mixed-order fallbacks must report query sort fallback. Composite positive/fallback paths use EXPLAIN metadata to assert or reject ordered composite BTree ORDER BY/LIMIT.

</spec-entry>

<spec-entry category="coding" keywords="cache,metrics,benchmark,pgwire,http,claim-mode" date="2026-07-09" title="Query-result cache observability counters" description="Query-result cache hit/miss observability" source="main@8a12c0f">

### Query-result cache observability counters

FusionDB now exposes query-result cache counters for benchmark claim integrity: query_result_cache_eligible_count, hit_count, miss_count, stale_count, insert_count, and invalidation_count. HTTP execute_sql and pgwire execute_cached_select both increment eligible/hit/miss/stale/insert consistently, and invalidate_query_result_cache increments invalidation_count. pgwire cache hits now call record_query so query_count includes cached grouped aggregate reads. /metrics JSON, /metrics/prometheus, and benchmark.py metrics_delta/metadata include these counters.

</spec-entry>

<spec-entry category="coding" keywords="benchmark,disclosure,report,reproducibility,privacy" date="2026-07-09" title="Benchmark disclosure report schema" description="Benchmark JSON disclosure schema and collection behavior" source="main@8a12c0f">

### Benchmark disclosure report schema

benchmark.py now writes a top-level disclosure object in JSON reports. The object includes schema_version/status/generated_at_utc, Cargo package metadata, git commit/describe/dirty status, Rust/cargo versions and binary hint/mtime/profile, benchmark client runtime, server endpoints with URL credential redaction, selected benchmark env vars with password/token redaction, CPU/RAM/load/system metadata, explicit BENCH_DISCLOSURE_DATA_DIR or FUSIONDB_DATA_DIR size/file/SSTable/sidebar counts, cache-related env and metric keys, and privacy notes. Data dir is not guessed when no explicit local path is provided.

</spec-entry>

<spec-entry category="coding" keywords="benchmark,topk,sstable,claim-mode,reverse,counters" date="2026-07-09" title="Part 20 SSTable-heavy Top-K claim gate" description="Part 20 persisted SSTable reverse claim implementation" source="main@8a12c0f">

### Part 20 SSTable-heavy Top-K claim gate

benchmark.py Part 20 now has INDEX_TOPK_SSTABLE_CLAIM, enabled by default under BENCH_CLAIM_MODE unless BENCH_INDEX_TOPK_SSTABLE_CLAIM=0. setup_index_topk_tables checkpoints after loading rows and building indexes, records index_topk_sstable_claim_checkpoint_ms/ok in load metadata, and fails claim-mode early if checkpoint cannot run. Ordered DESC Top-K cases are tagged with sstable_heavy_required and checkpoint_after_part20_load. apply_part20_claim_gate now requires tagged DESC paths to observe SSTable reverse iterator opens, reverse block reads, reverse block decodes, and enough reverse yields, while keeping query-result cache counters at zero, ordered scan/reverse counters bounded, sort fallback zero, and Fusion raw/visible counters bounded. This proves persisted SSTable reverse path usage, not cold disk I/O.

</spec-entry>

<spec-entry category="coding" keywords="benchmark,topk,first-pass,sstable,claim-mode,methodology" date="2026-07-09" title="Part 20 first persisted Top-K pass" description="Part 20 first persisted pass report rows" source="main@8a12c0f">

### Part 20 first persisted Top-K pass

benchmark.py now separates first persisted SSTable observation from warm persisted observation for Part 20. BENCH_INDEX_TOPK_FIRST_PERSISTED_PASS defaults on when BENCH_CLAIM_MODE and BENCH_INDEX_TOPK_SSTABLE_CLAIM are enabled. bench_with_phase(first-pass) now supports callable query generators via bench_query_text. Part 20 emits one no-warmup single-iteration [first-pass] row before each ordered DESC warm case, reusing the same finalize_case_result metadata, EXPLAIN annotation, block/cache metric annotation, and claim gate. First-pass metadata states that it proves a first persisted SSTable path after checkpoint, not cold OS-cache behavior, and latency is smoke-only.

</spec-entry>

<spec-entry category="coding" keywords="benchmark,topk,restart,sstable,claim-mode,owned-server" date="2026-07-09" title="Part 23 benchmark-owned Top-K restart phase" description="Benchmark-owned restart phase for persisted Top-K claims" source="main@8a12c0f">

### Part 23 benchmark-owned Top-K restart phase

benchmark.py now registers non-default Part 23 / BENCH_MATRIX=index_topk_restart. The matrix is self-contained: setup() short-circuits without touching external FUSIONDB_URL, then part23 starts a temporary FusionDB process on BENCH_INDEX_TOPK_RESTART_PORT using write_startup_config, loads Part 20 Top-K tables/indexes through the owned server, checkpoints, stops the process, restarts the same data dir, and measures only ordered DESC Top-K cases as restart-first-pass and restart-warm rows. It reuses Part 20 claim gates, block/cache metrics, EXPLAIN metadata, and SSTable reverse counters. Metadata records load_ready_ms, restart_ready_ms, data dir/SSTable counts, process cache reset, and that OS page cache is not controlled. Env knobs: BENCH_INDEX_TOPK_RESTART_BINARY, PORT, WORKDIR, KEEP_WORKDIR, TIMEOUT_SEC.

</spec-entry>

<spec-entry category="coding" keywords="benchmark,disclosure,owned-server,restart,topk" date="2026-07-09" title="Benchmark-owned server disclosure for restart matrices" description="Top-level disclosure now identifies benchmark-owned restart servers" source="main@8a12c0f">

### Benchmark-owned server disclosure for restart matrices

benchmark.py now adds disclosure.server.benchmark_owned. For normal reports it is active=false. For BENCH_MATRIX=index_topk_restart it is active=true and records the owned HTTP query/metrics/checkpoint URLs, port, binary, workdir/data-dir lifecycle, data-dir size/SSTable counts from result metadata, process-cache reset state, and OS page-cache caveat. This prevents top-level disclosure.server.base_url, which is restored external configuration at report time, from being mistaken for the server that served benchmark-owned restart rows.

</spec-entry>

<spec-entry category="coding" keywords="benchmark,cold-cache,drop-caches,restart,disclosure" date="2026-07-09" title="Optional OS drop_caches control for restart benchmark" description="Part 23 can explicitly request OS page cache dropping with structured disclosure" source="main@8a12c0f">

### Optional OS drop_caches control for restart benchmark

benchmark.py now supports BENCH_OS_CACHE_CONTROL=none|drop_caches and BENCH_INDEX_TOPK_RESTART_OS_CACHE_CONTROL for benchmark-owned restart matrices. Default is none and does not touch host caches. In Part 23 the hook runs only after the load process is stopped and before the restart process is started. When drop_caches is requested it runs sync, writes BENCH_OS_DROP_CACHES_VALUE (1/2/3, default 3) to /proc/sys/vm/drop_caches on Linux, records meminfo before/after, effective uid, sync result, and kernel documentation reference. If the explicit drop_caches request fails, Part 23 returns a setup error to avoid false cold-cache claims. Result metadata now includes cache_phase and os_cache_control, and top-level disclosure.server.benchmark_owned carries os_cache_control.

</spec-entry>

<spec-entry category="coding" keywords="benchmark,restart,topk,trial,claim-mode" date="2026-07-09" title="Multi-trial restart benchmark for indexed Top-K" description="Part 23 restart matrix can collect multiple process restart trials" source="main@8a12c0f">

### Multi-trial restart benchmark for indexed Top-K

benchmark.py now supports BENCH_INDEX_TOPK_RESTART_TRIALS for BENCH_MATRIX=index_topk_restart. Default is 1 and preserves existing row names. For N>1, Part 23 loads/checkpoints Part 20 data once, stops the load server, then repeats N process restart trials against the same data dir. Each trial optionally applies BENCH_INDEX_TOPK_RESTART_OS_CACHE_CONTROL, starts a fresh owned FusionDB process, records trial_number/restart_trials_requested/restart_ready_ms/restart_initial_metrics/RSS/cache state, runs all DESC Top-K cases as restart-first-pass and restart-warm, then stops the process before the next trial. Failures in explicit OS cache control or restart readiness are returned as benchmark error rows so claim-mode fails instead of silently dropping a trial. Top-level disclosure.server.benchmark_owned now records restart_trials_requested and a sample trial_number.

</spec-entry>

<spec-entry category="coding" keywords="benchmark,restart,trial-summary,topk,report" date="2026-07-09" title="Restart trial summary aggregation in benchmark reports" description="Reports now include cross-trial aggregation for Part 23 restart rows" source="main@8a12c0f">

### Restart trial summary aggregation in benchmark reports

benchmark.py now writes a top-level trial_summaries object. It currently aggregates only index_topk_restart rows that carry trial_number metadata, grouping by base case name, path, phase, and cache_phase. Each group reports trial_numbers, trial_count, sample_count, error_count, claim_statuses, avg/p50/p90/p95/p99/min/max/stddev/cv, row_count_values, metric_totals, and metric_per_query for METRIC_COUNTER_KEYS. Raw per-trial benchmark rows are unchanged. This supports multi-trial restart-first-pass analysis without requiring downstream tools to reconstruct groups from row names.

</spec-entry>

<spec-entry category="coding" keywords="benchmark,restart,review,claim-mode,workdir" date="2026-07-09" title="Part 23 restart semantics review hardening" description="Part 23 now restarts per case and hardens workdir and claim metric checks" source="main@8a12c0f">

### Part 23 restart semantics review hardening

After review, Part 23 index_topk_restart no longer performs only one restart per trial before running all cases. It now loads/checkpoints once, stops the load process, then for each trial and each DESC Top-K case it optionally applies OS cache control, starts a fresh owned FusionDB process, measures that case's restart-first-pass before case-specific warmup, measures restart-warm, and stops the process before the next case. Metadata records restart_case_policy=restart_before_each_case_per_trial, case_order_in_trial, cases_per_trial, and shared_data_dir_reused_across_trials=true. This makes every restart-first-pass row process-cold for its own case while still disclosing that the same checkpointed data dir is reused across trials. Explicit BENCH_INDEX_TOPK_RESTART_WORKDIR is no longer deleted by default when non-empty; BENCH_INDEX_TOPK_RESTART_RESET_WORKDIR=1 is required to permit deletion. Part 20 claim-mode no longer falls back query_count to success_count and now fails if query_count or core ordered Top-K/reverse/sort claim metrics are missing.

</spec-entry>

<spec-entry category="coding" keywords="lazy-reverse,sstable,topk,fusion" date="2026-07-09" title="Lazy SSTable reverse activation implementation" description="Reverse DESC LIMIT now avoids opening lower-frontier SSTables until needed" source="main@8a12c0f">

### Lazy SSTable reverse activation implementation

Implemented lazy activation for reverse SSTable sources in src/storage/fusion.rs. Eligible SSTables are now pushed into a pending max-heap keyed by decoded sst.meta.last_key user key, while write-buffer and memtables remain eager. Pending SSTables are activated only when no active source exists or pending.frontier_user_key >= active reverse top user key; equality is intentionally inclusive so equal-frontier tombstones/newer versions are visible before emitting a key. ReverseSource carries stable source_order so MVCC/source tie-breaking remains independent of activation order. Added cfg(test) thread-local activation hook only for precise unit verification.

</spec-entry>

<spec-entry category="coding" keywords="lazy-reverse,sstable,correctness,topk" date="2026-07-09" title="Lazy reverse pending-drain correctness fix" description="Lazy reverse scan now drains pending SSTables for full scans" source="main@8a12c0f">

### Lazy reverse pending-drain correctness fix

After implementing lazy SSTable reverse activation, merge_visible_range_reverse must continue while either the active merge heap or the pending SSTable heap is non-empty. A loop conditioned only on active heap emptiness preserves LIMIT 1 performance but can incorrectly stop an unbounded reverse scan after the highest-frontier SSTable drains. The loop now uses !heap.is_empty() || !pending_sstables.is_empty(), allowing lower-frontier SSTables to activate after higher keys are exhausted.

</spec-entry>

<spec-entry category="coding" keywords="lazy-reverse,memtable,topk,performance" date="2026-07-09" title="Reverse scan skips empty memtable sources" description="Avoid fixed empty active-memtable source overhead in reverse scans" source="main@8a12c0f">

### Reverse scan skips empty memtable sources

Fusion reverse merge now skips empty MemTable snapshots before constructing a ReverseSource. Snapshot creation leaves an empty active memtable, and small DESC LIMIT scans should not pay a source-open/candidate-pull cost for it. Non-empty write-buffer/memtables remain eager, and SSTables remain lazily activated by frontier. Added cfg(test) thread-local source-open hook alongside the SSTable activation hook so unit tests can assert exact per-task source counts without relying on global metrics.

</spec-entry>

<spec-entry category="coding" keywords="index-prefix,bloom,sstable,topk,metrics" date="2026-07-09" title="SQL index-prefix SSTable Bloom pruning implementation" description="SQL index-prefix SSTable Bloom pruning landed" source="main@8a12c0f">

### SQL index-prefix SSTable Bloom pruning implementation

Added SSTable filter block version 4 with a separate SQL index-prefix Bloom filter. The extractor is negative-only/fail-open and supports legacy index:<table>:<columns>: keys plus shard:<id>:index:<table>:<columns>: keys. Single-column scans use the column prefix; composite scans use the leading equality component prefix ending at | when the queried range is contained within that prefix. Fusion forward and reverse range scans probe this filter after SSTable overlap and existing table-prefix checks, before iterator open or reverse pending activation. Metrics were added as sstable_index_prefix_filter_* counters and surfaced through JSON metrics, Prometheus, and benchmark metadata.

</spec-entry>

<spec-entry category="coding" keywords="no-fill,block-cache,scan-options,fusion,sql" date="2026-07-09" title="SQL no-fill cache policy plumbing" description="SQL-level no-fill block-cache read policy plumbing" source="main@8a12c0f">

### SQL no-fill cache policy plumbing

Added StorageScanOptions to the Transaction scan API and wired FusionTransaction options overrides through forward, reverse, and parallel visible range merges. Fusion now converts StorageScanOptions to SsTableReadOptions and passes no-fill through SSTable user-key iterators and lazy reverse activation. Executor routed prefix/data scan helpers now have with-options variants; old helpers keep fill-cache defaults. First SQL no-fill users are ANALYZE table stats collection, CREATE INDEX backfill, unbounded SQL full scans, order-TopK fallback full scans, and no-LIMIT primary-key range scans. MemoryTransaction overrides with-options methods to ignore the option while preserving existing streaming visitor behavior instead of trait-default materialization.

</spec-entry>

<spec-entry category="coding" keywords="reverse-block,sstable,topk,rseek,performance" date="2026-07-09" title="Runtime bounded reverse block decode" description="Runtime bounded reverse block materialization before persisted sidecar" source="main@8a12c0f">

### Runtime bounded reverse block decode

SSTable reverse iterators now use a runtime block-entry span pass before materializing reverse entries. SsTable::append_reverse_block_entries_in_bounds parses decoded block entry offsets, applies user-key lower/upper bounds with partition_point, and only copies full key/value bytes for entries in the bounded reverse subrange. This preserves read_block_at_with_reusable_file, CRC/decompression, block-cache, no-fill, block-prefix filter, and Fusion MVCC behavior while reducing sstable_reverse_block_entry_decode_count for narrow bounded reverse scans. This is the fail-open baseline for a future persisted .rseek sidecar: missing or invalid sidecar should fall back to this helper. New test reverse_block_bounds_materialize_only_needed_entries builds 100 entries and verifies [k090,k095) materializes/decodes only 5 entries in descending order.

</spec-entry>

<spec-entry category="coding" keywords="rseek,sidecar,sstable,reverse-block,topk" date="2026-07-09" title="Persisted SSTable reverse seek sidecar" description="Persisted fail-open reverse seek sidecar for SSTable reverse iterators" source="main@8a12c0f">

### Persisted SSTable reverse seek sidecar

SSTable finish now writes a derived .rseek sidecar using magic FRSK v1. The sidecar is keyed by the same file fingerprint shape as FICX (file len, mtime, index/filter/meta offsets, index len) and stores per-block decoded_len, entry_count, decoded_crc32, and decoded-payload entry offsets. SsTable::open does not read .rseek; reverse iterators carry a shared OnceLock and lazy-load the sidecar on first block access. Reverse block reads still go through read_block_at_with_reusable_file, preserving CRC/decompression, block cache, no-fill, and reusable file-handle behavior. A sidecar block is used only when block offset exists and decoded_len/entry_count/decoded_crc32 match; otherwise sstable_reverse_seek_sidecar_fail_open_count increments and the iterator falls back to append_reverse_block_entries_in_bounds. Obsolete SSTable cleanup and compaction install failure cleanup now remove .rseek alongside .sst and .idxcache. Metrics expose hit/miss/stale/invalid/write/write_error/use/fail_open counters through monitor, HTTP JSON/Prometheus, and benchmark.py METRIC_COUNTER_KEYS.

</spec-entry>

<spec-entry category="coding" keywords="rseek,benchmark,topk,claim,sidecar" date="2026-07-09" title="Part 20 persisted rseek sidecar claim gate" description="Part 20/23 .rseek sidecar benchmark gate" source="main@8a12c0f">

### Part 20 persisted rseek sidecar claim gate

benchmark.py Part 20/23 claim gate now records and enforces persisted .rseek sidecar evidence for SSTable-heavy ordered DESC Top-K paths. The gate records claim_reverse_seek_sidecar_status, use/hit/load/path-failure counters, requires use_count > 0, requires sidecar uses to match SSTable reverse block reads, requires miss/stale/invalid/write_error/fail_open all zero, and requires restart-first-pass lazy-load hit > 0. Warm phases do not require hit because sidecars may already be cached; use_count remains the proof. The evidence scope is counter/path only, not a hard latency claim.

</spec-entry>

<spec-entry category="coding" keywords="rseek,counters,sstable,benchmark,topk" date="2026-07-09" title="rseek block-internal work counters" description="Block-internal .rseek evidence counters" source="main@8a12c0f">

### rseek block-internal work counters

FusionDB now exposes block-internal reverse scan counters for persisted .rseek evidence. ReverseBlockScanStats records runtime span-scan blocks, span-scanned entries, runtime span materializations, sidecar indexed entries covered by successful block uses, sidecar materialized entries, and sidecar offset probes. monitor.rs exports the new counters, http_server.rs exposes them in JSON and Prometheus, and benchmark.py records derived fields including zero runtime span-scan proof and sidecar probe/materialize ratios. Part 20/23 SSTable-heavy DESC Top-K claim gate now requires .rseek full coverage to have zero runtime span scans/materializations and sidecar materializations accounting for all reverse block entry decodes.

</spec-entry>

<spec-entry category="coding" keywords="rseek,benchmark,ab,topk,restart" date="2026-07-09" title="Part 23 rseek A/B restart benchmark" description="A/B benchmark for .rseek kept vs removed fallback" source="main@8a12c0f">

### Part 23 rseek A/B restart benchmark

benchmark.py now supports BENCH_MATRIX=index_topk_rseek_ab and BENCH_INDEX_TOPK_RSEEK_AB=1. It reuses Part 23 index_topk_restart but after checkpoint copies the loaded data dir to a rseek_removed variant, deletes only .rseek sidecars there, and restarts a fresh FusionDB process per case/trial/variant. The rseek-kept variant expects persisted sidecar use and zero runtime span scans; the rseek-removed variant expects sidecar use/index/materialize/probe counters to stay zero, restart-first-pass to observe a sidecar miss, and runtime span-scan/materialize counters to prove fallback work while preserving ordered Top-K and result correctness gates. Reports include rseek_ab_enabled, rseek_ab_variants, rseek_ab_fallback removal metadata, rseek_ab_variant, and rseek_sidecar_expectation.

</spec-entry>

<spec-entry category="coding" keywords="rseek,benchmark,ab,checksum,report" date="2026-07-09" title="rseek A/B paired result evidence" description="A/B report now pairs .rseek kept/removed rows with ordered result checksums" source="main@8a12c0f">

### rseek A/B paired result evidence

benchmark.py records a stable sha256-json-v1 checksum for the ordered rows and columns of each captured SELECT result. Warm benchmark rows keep the full measured checksum sequence because some phase/index query functions intentionally vary the predicate per measured iteration. BENCH_MATRIX=index_topk_rseek_ab reports trial_summaries.index_topk_rseek_ab by pairing rseek-kept and rseek-removed rows on trial_number, case_order_in_trial, path, and phase. In BENCH_CLAIM_MODE, each pair must have matching row_count and an identical checksum sequence; failures become benchmark errors. The paired summary also reports latency ratios as smoke-only evidence and counter deltas that show runtime span scans/materializations in the removed-sidecar path versus sidecar use/index/materialize/probe work in the kept path.

</spec-entry>


<spec-entry category="coding" keywords="block-index-prefix,sql-index-prefix,sstable,block-properties,fail-open" date="2026-07-09" title="Block-level SQL index-prefix block properties" description="Per-block SQL index-prefix pruning implementation" source="main@8a12c0f">

### Block-level SQL index-prefix block properties

SSTable meta v4 now carries per-block SQL index-prefix properties. SsTableBuilder extracts prefixes from decoded MVCC user keys during flush_block and stores sql_index_prefixes_complete plus sql_index_prefixes. Forward and reverse SSTable user-key range iterators derive a target SQL index prefix only when the query range is contained in a supported SQL index prefix, then probe block properties before reading the block. Missing/misaligned properties, legacy v3 meta, incomplete extraction, short internal keys, malformed block entry payloads, and absent suffix metadata all fail open and read the block. Negative skips increment sstable_block_index_prefix_filter_skip_count; positives and fail-opens have separate counters exposed via monitor, HTTP JSON metrics, Prometheus metrics, and benchmark.py annotations.

</spec-entry>

<spec-entry category="coding" keywords="benchmark.py,part29,block-index-prefix,metrics,gate" date="2026-07-09" title="Part 29 block index-prefix benchmark wiring" description="Benchmark.py wiring for block SQL index-prefix gate" source="main@8a12c0f">

### Part 29 block index-prefix benchmark wiring

benchmark.py now exposes BENCH_MATRIX=sstable_block_index_prefix as Part 29 and routes it to src/bin/sstable-block-index-prefix-bench.rs without HTTP setup. Environment controls are BENCH_SST_BLOCK_INDEX_PREFIX_SSTABLES, ITERS, PAYLOAD_BYTES, RELEASE, and TIMEOUT_SEC. The result adapter hard-gates row_count == 0, exact SSTable index-prefix check/positive/fail-open counts, exact block-index-prefix check/skip/fail-open counts, and expected block cache misses for optimized/fail_open/incomplete phases. The low-level binary records SSTable-level SQL index-prefix probes explicitly, then opens user-key range iterators so block-level counters come from the actual iterator path.

</spec-entry>

<spec-entry category="coding" keywords="part29,benchmark.py,natural-false-positive,sql-index-prefix,gate" date="2026-07-09" title="Part 29 natural false-positive implementation" description="Implementation details for Part 29 natural phase" source="main@8a12c0f">

### Part 29 natural false-positive implementation

src/bin/sstable-block-index-prefix-bench.rs now reports natural_false_positive alongside optimized, fail_open, and incomplete phases. Config fields/env include BENCH_SST_BLOCK_INDEX_PREFIX_NATURAL_PREFIXES, NATURAL_ITERS, NATURAL_PAYLOAD_BYTES, and NATURAL_CANDIDATES. Candidate target prefixes must be canonical composite SQL index prefixes of the form index:metrics:host_id,ts:i2NNNNNNNN| so probe_sql_index_prefix_filter does not fail open due extractor mismatch. benchmark.py hard-gates natural_false_positive with one SSTable times natural_iters: table checks/positives equal expected, table skips/fail-opens zero, block checks/skips equal expected, block fail-opens zero, block misses zero, row_count zero.

</spec-entry>

<spec-entry category="coding" keywords="part30,sql-block-index-prefix,benchmark,claim-gate" date="2026-07-09" title="Part 30 SQL block index-prefix benchmark wiring 2026-07-09" description="Part 30 public SQL/Fusion block index-prefix benchmark wiring" source="main@8a12c0f">

### Part 30 SQL block index-prefix benchmark wiring 2026-07-09

benchmark.py now registers non-default BENCH_MATRIX=sql_block_index_prefix_prune / Part 30. The setup creates a public SQL table with composite index (host_id, ts), loads real neighboring-prefix decoy SSTables, discovers a natural SQL index-prefix Bloom false-positive host via public SQL probes, then loads matching target rows into a separate SSTable. The Part 30 ASC Top-K case uses composite_block_index_prefix_prune_asc and a claim gate requiring setup false-positive discovery, file-level SQL index-prefix positives, block-level SQL index-prefix skips, zero block/index fail-opens, zero compactions, ordered composite EXPLAIN, stable result checksums, and normal Part 20 cache/sort gates.

</spec-entry>

<spec-entry category="coding" keywords="part30,sql,block-index-prefix,delayed-index,benchmark" date="2026-07-09" title="Part 30 delayed index setup optimization 2026-07-09" description="Part 30 setup delays composite index creation for first decoy while preserving real backfill/checkpoint semantics." source="main@8a12c0f">

### Part 30 delayed index setup optimization 2026-07-09

Part 30 SQL block index-prefix benchmark now defaults BENCH_SQL_BLOCK_INDEX_PREFIX_DELAY_INDEX=1. Setup creates table first, bulk-loads the first decoy rows without per-row composite-index maintenance, then runs CREATE INDEX idx_bench_topk_block_index_prefix_host_ts ON bench_topk_block_index_prefix_idx(host_id, ts) before the decoy checkpoint. This preserves real public SQL DDL/DML/checkpoint/SSTable paths: CREATE INDEX backfills real composite index keys from table rows, checkpoint flushes those keys into SSTables, and discovery still requires natural file-level Bloom false positive plus block-level prefix skip. Later decoy SSTables, if configured, continue through normal DML index maintenance so multi-SSTable benchmark shape is preserved. The timer for load_sql_block_index_prefix_decoys must use the outer decoy Timer; CREATE INDEX timing is recorded separately as load_sql_block_index_prefix_create_index.

</spec-entry>

<spec-entry category="coding" keywords="part30,copy-stdin,bulk-load,benchmark,sql-block-index-prefix" date="2026-07-09" title="Part 30 COPY STDIN loader 2026-07-09" description="Part 30 benchmark setup uses conservative chunked /copy_stdin loader with fallback and claim metadata." source="main@8a12c0f">

### Part 30 COPY STDIN loader 2026-07-09

Part 30 SQL block index-prefix setup now uses public HTTP /copy_stdin by default for HTTP transport via BENCH_SQL_BLOCK_INDEX_PREFIX_COPY_STDIN=1. Python benchmark.py sends CSV payloads to COPY bench_topk_block_index_prefix_idx(id, host_id, ts, payload) FROM STDIN WITH (FORMAT csv), records load_method/copy_format/chunk_rows/rows/batches/bytes/total_ms/max_payload_bytes, and falls back to INSERT VALUES for pg transport or BENCH_SQL_BLOCK_INDEX_PREFIX_COPY_STDIN=0. Default BENCH_SQL_BLOCK_INDEX_PREFIX_COPY_CHUNK_ROWS is 1000 because 100000-row payload hit HTTP 413 and 20000-row COPY transactions were slower. The loader preserves public SQL realism: table DDL, delayed CREATE INDEX backfill, checkpoint boundaries, natural false-positive discovery, and query claim gates remain unchanged. Claim mode now checks COPY row/batch metadata, no INSERT fallback, successful checkpoints, and setup compaction_run_count == 0.

</spec-entry>

<spec-entry category="coding" keywords="part31,zone-map,metrics,storage-scan-options,fail-open" date="2026-07-09" title="Part 31 zone-map observability scaffold 2026-07-09" description="Part 31 adds zone-map metrics and explicit opt-in scan option while keeping pruning disabled." source="main@8a12c0f">

### Part 31 zone-map observability scaffold 2026-07-09

Part 31 SQL block zone-map pruning now has a fail-open observability scaffold before any data-block skip is enabled. src/monitor.rs defines counters and increment helpers for sstable_block_zone_map_filter_check/positive/skip/fail_open, metadata_bytes, mvcc_overlap_fail_open, and schema_fail_open. src/server/http_server.rs exposes the counters in JSON /metrics and Prometheus, with HTTP metrics tests asserting their presence. benchmark.py includes the zone-map counters in METRIC_COUNTER_KEYS and annotate_prefix_filter_metrics-derived metadata so future Part 31 claim gates can capture deltas and per-query ratios. src/storage/mod.rs extends StorageScanOptions with sql_block_zone_map_pruning, default false in both fill_cache and no_fill_cache, plus with_sql_block_zone_map_pruning(enabled). No scan path currently enables or consumes it, so this change does not skip blocks or alter query behavior; future implementation must explicitly opt in and prove MVCC isolation before skip.

</spec-entry>

<spec-entry category="coding" keywords="part31,sstable,zone-map,metadata,v5,wire-format" date="2026-07-09" title="Part 31 SSTable zone-map v5 metadata scaffold 2026-07-09" description="Part 31 v5 SSTable zone-map metadata scaffold with default v4 compatibility" source="main@8a12c0f">

### Part 31 SSTable zone-map v5 metadata scaffold 2026-07-09

Implemented a metadata-only SSTable v5 scaffold in src/storage/sstable.rs. SsTableBlockProperties now carries sql_zone_maps_complete and sql_zone_maps, and SsTableSqlZoneMap records table_prefix, schema_fingerprint, column_index, column_name, type_tag, value_encoding_version, scalar min/max, row/null/non-null/put/tombstone counts, and bounds_valid. Default SSTableBuilder output remains v4 when no zone maps are present; v5 is selected only when zone-map metadata is populated. Explicit v4 wire structs preserve block SQL index-prefix compatibility, and decode_meta_exact rejects trailing bytes before trying fallback formats.

</spec-entry>

<spec-entry category="coding" keywords="part31,sstable,v5,wire-format,metadata,header,bincode" date="2026-07-09" title="Part 31 framed SSTable v5 metadata wire hardening 2026-07-09" description="Part 31 hardens SSTable v5 metadata with explicit wire structs and framed version dispatch" source="main@8a12c0f">

### Part 31 framed SSTable v5 metadata wire hardening 2026-07-09

src/storage/sstable.rs now decouples v5 on-disk metadata from runtime SsTableMeta. Added explicit BlockSqlZoneMapV5, BlockSqlZoneMapsSsTableBlockPropertiesV5, and BlockSqlZoneMapsSsTableMetaV5 wire structs with conversions to runtime structs. New v5 writes are framed with SSTABLE_META_MAGIC FSMT plus a u32 version before the bincode payload; decode_meta dispatches framed metadata strictly by header version and rejects unknown framed versions or trailing payload bytes. Unframed legacy v0-v4 metadata still uses existing fallback decoding, and unframed temporary v5 payloads remain readable for the narrow scaffold window. Default builder output remains v4 when no zone maps are present; builder writes framed v5 only when zone-map metadata is populated.

</spec-entry>

<spec-entry category="coding" keywords="part31,zone-map,producer,sstable,fusion,metadata" date="2026-07-09" title="Part 31 SQL zone-map metadata producer 2026-07-09" description="Part 31 producer-only SQL zone-map metadata generation for SSTable flush/compaction" source="main@8a12c0f">

### Part 31 SQL zone-map metadata producer 2026-07-09

Implemented the first producer-only SQL zone-map step. src/storage/sstable.rs now has block_sql_zone_maps for strict Fusion block parsing, data:/shard:data: table-prefix extraction, deterministic schema fingerprints, supported scalar type tags for integer/bool/date/timestamp/interval, and per-column min/max/null/non-null/put/tombstone counts. SsTableBuilder has enable_sql_zone_map_collection and fills sql_zone_maps during flush_block when a schema snapshot is provided. New v5 writes still happen only when at least one actual zone-map entry exists, so empty/unsupported-only collection keeps v4. src/storage/fusion.rs now builds SSTableBuilder through sstable_builder_with_zone_maps for flush, compaction, and shutdown flush, using a best-effort visible schema: snapshot. No reader skip or pruning behavior is enabled.

</spec-entry>

<spec-entry category="coding" keywords="part31,zone-map,producer,fail-open,compaction" date="2026-07-09" title="Part 31 producer fail-open hardening 2026-07-09" description="Fail-open producer hardening and compaction metadata recomputation coverage" source="main@8a12c0f">

### Part 31 producer fail-open hardening 2026-07-09

Part 31 producer behavior is now locked with safety regressions: malformed values or schema/runtime type mismatch cause block_sql_zone_maps to fail open, leaving sql_zone_maps_complete=false and keeping v4 if no actual maps exist. Unsupported-only schemas also keep v4. Fusion compaction output is verified to recompute zone maps from the output SSTable entries, including tombstone_count, rather than inheriting old input metadata. Reader pruning remains disabled.

</spec-entry>

<spec-entry category="coding" keywords="part31,mvcc,zone-map,storagescanoptions,scanpredicateplan,fusion" date="2026-07-09" title="Part 31 MVCC gate scaffold 2026-07-09" description="Non-skipping SQL zone-map pruning plan scaffold and Fusion options propagation" source="main@8a12c0f">

### Part 31 MVCC gate scaffold 2026-07-09

Added a non-skipping SQL block zone-map pruning scaffold. StorageScanOptions now carries Option<Arc<SqlBlockZoneMapPruningPlan>> while preserving default disabled behavior and legacy with_sql_block_zone_map_pruning(false) clearing semantics. Storage exposes shared schema fingerprint, type tag, scalar encoding helpers used by SSTable producer and executor plan building. Executor builds a narrow plan only for AND-connected Eq/Lt/LtEq/Gt/GtEq, non-negated BETWEEN, and positive IN over integer, boolean, date, timestamp, and interval scalar-compatible values; rejects NotEq, LIKE, OR, NOT IN, NULL, text, float, decimal, and unsupported expressions. Full-scan, streaming filtered scan, top-k scan, and PK range scan attach the plan to scan options. Fusion internal scan/parallel merge now preserves StorageScanOptions to merge_visible_range and derives SsTableReadOptions only when opening SSTable iterators. No block skip or zone-map counter decision is enabled yet.

</spec-entry>

<spec-entry category="coding" keywords="part31,zone-map,decision,evaluator,fail-open" date="2026-07-09" title="Part 31 zone-map decision evaluator scaffold 2026-07-09" description="Storage-layer SQL zone-map block decision evaluator scaffold" source="main@8a12c0f">

### Part 31 zone-map decision evaluator scaffold 2026-07-09

Added a storage-layer SQL block zone-map decision evaluator without wiring it into SSTable block skipping. SqlBlockZoneMapPruningPlan::evaluate_block_zone_maps returns SkipBlock, ReadBlock, or FailOpen(reason). The evaluator validates complete metadata, schema fingerprint, column index/name, type tag, value encoding, valid min/max bounds, zero null/tombstone counts, and row_count == put_count == non_null_count with row_count > 0 before trusting a map. Predicate semantics support Eq/Lt/LtEq/Gt/GtEq and IN against scalar min/max. For AND terms, any trusted no-match term can return SkipBlock; if no trusted term proves no-match and any term is untrusted, the result is FailOpen. This remains a scaffold: no Fusion isolation proof or raw iterator skip consumes SkipBlock yet.

</spec-entry>

<spec-entry category="coding" keywords="part31,sstable,iterator,zone-map,approved-skip-offsets" date="2026-07-09" title="Part 31 approved block skip iterator scaffold 2026-07-09" description="SSTable helper and approved block-skip offset scaffold for future Fusion zone-map pruning" source="main@8a12c0f">

### Part 31 approved block skip iterator scaffold 2026-07-09

Added the next non-behavior-changing Part 31 infrastructure in src/storage/sstable.rs. SsTable now exposes validated_block_properties_for_zone_maps(), returning block properties only when metadata length and offsets align with the current index offsets, plus block_property_user_key_interval() which strips the MVCC suffix and fails open on short or inverted bounds. Forward SsTableIterator now carries an optional approved_block_skip_offsets set. Existing constructors pass None, while new_user_key_range_iterator_with_options_and_block_skips() lets future Fusion MVCC isolation pass pre-approved block offsets. The iterator only consumes approved offsets before reading a block and still does not interpret SQL predicates or zone-map metadata itself. No Fusion path enables skip yet.

</spec-entry>

<spec-entry category="coding" keywords="part31,zone-map,fusion,approved-skip,mvcc" date="2026-07-09" title="Part 31 Fusion approved zone-map skip planner 2026-07-09" description="Fusion plans conservative SQL zone-map approved block skips" source="main@8a12c0f">

### Part 31 Fusion approved zone-map skip planner 2026-07-09

L3: Fusion merge_visible_range now converts safe SQL block-zone-map pruning plans into SSTable approved block-skip offsets for forward scans only. Gates: scan range must map to data:<table>: or shard:<id>:data:<table>:; block properties must be offset-validated; candidate block user-key interval must be fully inside [start,end); adjacent blocks must not split the same user key; write buffer, memtables, and other SSTables must not overlap the candidate interval. Other SSTables that only overlap via broad CDC/schema SSTable meta can be ruled out only when their user-key prefix filter returns NoMatch for the target table prefix; MayMatch and FailOpen still force MVCC fail-open. Storage evaluator counters record one outcome per checked block, and the raw SSTable iterator still receives opaque approved offsets without interpreting SQL predicates.

</spec-entry>

<spec-entry category="coding" keywords="part31,benchmark,owned-server,zone-map,claim" date="2026-07-09" title="Part 31 benchmark-owned zone-map claim runner" description="Part 31 claim runs are now isolated from external server memtable/compaction settings" source="main@8a12c0f">

### Part 31 benchmark-owned zone-map claim runner

benchmark.py now runs BENCH_MATRIX=sql_block_zone_map_prune in BENCH_CLAIM_MODE on a benchmark-owned HTTP FusionDB process by default when Part 31 is the only selected part. The runner writes a fresh startup config with BENCH_SQL_BLOCK_ZONE_MAP_MEMTABLE_FLUSH_MB defaulting to 256, switches global HTTP URLs before setup, records owned-server disclosure and per-row metadata, then stops the process and restores URLs. Explicit BENCH_SQL_BLOCK_ZONE_MAP_OWNED_SERVER=0 disables this path; non-HTTP owned mode is rejected.

</spec-entry>

<spec-entry category="coding" keywords="part31,zone-map,mvcc,metrics,fail-open" date="2026-07-09" title="Part 31 MVCC fail-open reason counters" description="Reason-specific MVCC fail-open counters for safer Part 31 optimization" source="main@8a12c0f">

### Part 31 MVCC fail-open reason counters

Part 31 SQL block zone-map pruning now splits MVCC fail-open observability into four fixed low-cardinality counters while preserving the existing aggregate sstable_block_zone_map_mvcc_overlap_fail_open_count. Reasons are boundary split, write-buffer overlap, memtable overlap, and SSTable overlap. Fusion records exactly one first-match reason per checked block in the same safety order as the previous boolean short-circuit, and raw SSTable iterators still only consume opaque approved block offsets.

</spec-entry>

<spec-entry category="coding" keywords="part31,zone-map,benchmark,control,hint,task-local" date="2026-07-09" title="Part 31 enabled-disabled scoped control gate" description="Part 31 enabled/disabled control gate uses scoped executor hint and pair summaries" source="main@8a12c0f">

### Part 31 enabled-disabled scoped control gate

Part 31 SQL block zone-map benchmark now has enabled-vs-disabled control rows without changing the SQL predicate. HTTP /query recognizes the leading hint /*+ FUSIONDB_DISABLE_SQL_BLOCK_ZONE_MAP_PRUNE */, strips it before authorization/routing/parsing, and executes the clean SQL inside Executor::execute_sql_with_sql_block_zone_map_pruning(false). Executor uses a tokio task-local flag, so the disable scope is per-query and does not affect concurrent queries. scan options return unchanged when the scoped flag is false; otherwise existing zone-map pruning plan attachment remains unchanged. benchmark.py adds BENCH_SQL_BLOCK_ZONE_MAP_DISABLED_CONTROL, disabled-control rows for each Part 31 path, single-row disabled hard gates requiring zero zone-map counters, and pair claims/summaries comparing warm enabled rows against disabled rows by path.

</spec-entry>

<spec-entry category="coding" keywords="composite,topk,index-only,covering,btree" date="2026-07-09" title="Composite ordered Top-K key-column covering" description="Composite ordered Top-K returns key-column covered rows" source="main@8a12c0f">

### Composite ordered Top-K key-column covering

Composite ordered BTree Top-K scans can now return CoveredIndexRows for projections composed of the primary key plus decoded ordered composite index key columns. The implementation decodes ordered index components conservatively, reconstructs PK values using value_to_primary_row_id/primary_key_row_from_id semantics, and fail-opens by omitting covered rows when component decoding or schema mapping is incomplete. This is intentionally narrower than composite INCLUDE payload support.

</spec-entry>

<spec-entry category="coding" keywords="composite,include,topk,covering,index-only,fail-open" date="2026-07-09" title="Composite BTree INCLUDE covering Top-K" description="Implementation contract for composite BTree INCLUDE payload covering ordered Top-K." source="main@8a12c0f">

### Composite BTree INCLUDE covering Top-K

Composite BTree indexes can use a versioned c4 metadata shape to store non-key INCLUDE columns as payload: c4:<table>:<key_columns_csv>:<include_columns_csv>. Legacy v3/u3/v2/s2 metadata must continue decoding with include_columns empty. INCLUDE columns are payload only: they do not affect uniqueness, composite key ordering, leading-prefix/range gating, or residual predicate safety. Ordered composite scans may merge decoded include payloads into CoveredIndexRows only when every returned entry has a complete decodable payload; mixed legacy or malformed payloads must fail open by omitting include columns from coverage so the scan layer fetches base rows. DML maintenance must write include payloads on insert/backfill, rewrite the same composite key on include-only update, and delete/reinsert on key-column update. DROP/RENAME COLUMN dependency checks must include composite key columns and include_columns.

</spec-entry>

<spec-entry category="coding" keywords="topk,metrics,index-only,base-fetch,benchmark,claim" date="2026-07-09" title="Ordered Top-K index-only/base-row-fetch counters" description="Dedicated ordered Top-K row-source counters and strengthened Part 20 claim gate." source="main@8a12c0f">

### Ordered Top-K index-only/base-row-fetch counters

FusionDB now exposes dedicated SQL ordered Top-K row-source counters: index_ordered_topk_index_only_row_count counts rows materialized directly from index keys or INCLUDE payload without base-row lookup; index_ordered_topk_base_row_fetch_count counts rows whose ordered Top-K path had to materialize the base row, including row-cache hits. The counters are gated by IndexScanPlan.ordered_topk_counted so they stay aligned with existing index_ordered_topk_scan_count semantics instead of any ordered index row stream. HTTP /metrics and Prometheus expose both counters. benchmark.py Part 20 captures them, requires ordered index-only + base-fetch rows to equal expected_rows * query_count for ordered paths, requires covering paths to have base-fetch 0 and index-only rows equal returned rows, and requires heap-fetch control paths to have base-fetch rows equal returned rows.

</spec-entry>

<spec-entry category="coding" keywords="composite,include,c5,metadata,wire-format,compatibility" date="2026-07-09" title="Composite INCLUDE c5 length-prefixed metadata" description="Length-prefixed c5 metadata replaces delimiter c4 for composite INCLUDE indexes." source="main@8a12c0f">

### Composite INCLUDE c5 length-prefixed metadata

Composite BTree INCLUDE metadata now writes c5 length-prefixed values instead of delimiter c4. c5 encodes table name, key-column count and names, include-column count and names using byte-length-prefixed UTF-8 string fields, so ':' and ',' in names round-trip without split/trim ambiguity. parse_index_meta tries c5 first, rejects malformed/trailing/zero-count/oversized-count payloads, and keeps c4/v3/u3/v2/legacy read compatibility. New composite INCLUDE writes use c5 only when include_columns is non-empty; no-include composites continue using v3. Table-directory rebuild/drop cleanup now filters scanned index_meta_table entries by parsed metadata table, so table names like 'a' no longer delete directory entries for 'a:b' that share the textual prefix. Remaining separate risk: single-column INCLUDE s2 metadata and broader quoted-column DDL naming still use delimiter/Ident::to_string vs value patterns and should get their own hardening slice.

</spec-entry>

<spec-entry category="coding" keywords="single-column,include,s3,s2,metadata,wire-format,compatibility" date="2026-07-09" title="Single-column INCLUDE s3 length-prefixed metadata" description="Length-prefixed s3 metadata replaces delimiter s2 for single-column INCLUDE indexes." source="main@8a12c0f">

### Single-column INCLUDE s3 length-prefixed metadata

Single-column BTree INCLUDE metadata now writes s3 length-prefixed values instead of delimiter s2. s3 reuses the c5 byte-length-prefixed payload shape: table name, column count, column name, include count, include column names. The s3 parser requires exactly one key column, rejects malformed/trailing/zero-count/oversized payloads via the shared c5 parser, and sets ordered_encoding=false. Legacy s2 metadata remains readable for existing indexes. No-INCLUDE single-column indexes keep the original table:column metadata shape.

</spec-entry>

<spec-entry category="coding" keywords="data-prefix,table,scan,delimiter,collision,limit,correctness" date="2026-07-09" title="Exact table data-prefix scan filtering" description="Exact-table filtering for routed data prefix scans." source="main@8a12c0f">

### Exact table data-prefix scan filtering

Routed table data scans now post-filter raw prefix hits in scan_routed_data_prefixes_for_table_with_options and scan_routed_data_prefixes_for_each_with_options. The filter protects delimiter-style storage keys from table-name prefix collisions such as raw table 'tenant' matching raw key 'data:tenant:archive:<row_id>'. It loads the table schema when available, decodes the primary key column from each row, rebuilds the expected routed data key, and accepts only exact key matches. If no reconstructable primary key exists, it conservatively accepts only suffixes without an extra ':' segment. Limit accounting now counts accepted exact-table rows, not raw prefix hits, so a collision before the first valid row cannot consume LIMIT. This is a compatibility hardening layer over the existing key format; longer-term structured/length-prefixed table/index/FTS key components remain preferable.

</spec-entry>

<spec-entry category="coding" keywords="identifier,column,quoted,canonicalization,include,ddl" date="2026-07-09" title="Column identifier value storage for quoted delimiter columns" description="CREATE/ALTER TABLE column storage now uses Ident.value instead of Display text." source="main@8a12c0f">

### Column identifier value storage for quoted delimiter columns

CREATE TABLE and ALTER TABLE column identity now use sqlparser Ident.value via a local sql_identifier_name helper instead of Ident::to_string(). This prevents quoted column names from being stored with literal quote characters while CREATE INDEX, SELECT, DML, and other resolution paths use ident.value. The slice intentionally applies only to column identity, not table names, because table names are embedded in schema/data/index/FTS/count keyspaces and require a separate keyspace migration. It enables quoted column names containing ':' or ',' to participate in single-column INCLUDE indexes and ALTER ADD/RENAME/DROP flows. Full PostgreSQL-style quote-aware exact lookup and unquoted lowercase folding remain future work; TableSchema::get_column_index still has legacy case-insensitive fallback.

</spec-entry>

<spec-entry category="coding" keywords="row-cache,mvcc,byte-identity,cache" date="2026-07-10" title="Row cache 字节一致性契约(BENCHPROD-463)" source="main@47449f4">

### Row cache 字节一致性契约(BENCHPROD-463)

执行层 row cache 自 4d2fcfc 起为 CachedRow{encoded: Arc<[u8]>, row},命中条件=调用方本次从存储解析出的字节与缓存字节 memcmp 相等(execution/mod.rs row_cache_lookup/row_cache_store)。禁止任何绕过 helper 的 row_cache.get/insert;禁止重新引入 per-key invalidate(正确性不依赖失效,唯一保留 Raft 快照 invalidate_all)。新点查路径顺序:covered → key_only_scan(零存储)→ txn.get → 字节验证 → 解码+store(仅全行)。row_read=每次存储取行(含验证命中)。测试契约:带外改写存储字节必须战胜缓存(*_tracks_storage_bytes/*_storage_truth 系列);投毒与旧快照回归测试在 execution::tests。

</spec-entry>