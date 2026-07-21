# 472 主体设计:块级列式聚合解码(合并稿)

> 2026-07-21。4 只读读取(块格式/聚合消费/MVCC 约束/Arrow 基建)+ 2 竞争设计
> (最小风险 vs 吞吐最大)+ max-effort 对抗合并。全部守卫条件已回代码核实。

## 核心事实(读取实证)

- **行值是行主序 blob 但带 O(1) 列偏移表**:`encode_value` = `[flag:1B][RowEncoder blob]`,
  blob = `[count:u16][off×count][bincode span...]`。块 = `[count:u32][key_len|key|val_len|val]...`。
  ⇒ 块内 N 个独立单行 blob,无物理列存;"列式解码"= 用偏移表跨块每行抽第 idx 列 span
  喂 decode_scalar_span(66a4300),绕开整行 Vec<Value> 物化。**不需要 Arrow/物理转置**。
  写路径 block_sql_zone_maps(sstable.rs:522)已是这个循环的模板。
- **聚合消费逐条**:ColumnAggregateScanVisitor(column_scan.rs:418)从 N 路归并拿单条
  (key,value),decode_column → ColumnAggregateState::update。MVCC 可见性/去重全在上游归并
  (merge_visible_range_entries)完成 —— 故块级 API 不能盲穿归并,必须守卫"块独占其键区间"。
- **前四条守卫已有现成 helper**:sql_zone_map_skip_offsets_for_sstable(fusion.rs:3550)为
  zone-map 块跳过算的正是"块键区间无 write-buffer/memtable/他 SSTable 重叠+无边界共享"——
  反向用途,直接复用。**COUNT/MIN/MAX 已在 SsTableSqlZoneMap 元数据里**(row_count/put_count/
  tombstone_count/min_scalar/max_scalar,sstable.rs:671)——两派都漏了,收入 T4。

## 守卫条件(每条都便宜,任一失败 Ok(None) 回退)

**整段(一次,元数据+小扫描,复用已上线 helper)**:
- G1 恰好一个 SSTable 覆盖 [start,end)(sstable_overlaps_user_key_interval,两次 decode_key)
- G2 无 write-buffer 重叠(线性扫小 buffer;整段检查天然捕获任意内部块边界的 WB 键)
- G3 无 memtable 重叠(每 memtable 一次有界 SkipMap seek)
- G4 validated_block_properties_for_zone_maps 为 Some(仅用于枚举块偏移+区间,不用于折叠正确性)

**逐条(单块窗口走查,归并逐条解决的四义务在存储侧验证)**:
- G5 全可见:ts = u64::MAX - be(key 后缀 8B) <= read_ts(键已切好,近免费)
- G6 仅 PUT:block[value_start]==1(一字节;tombstone 即回退)
- G7 单版本:prev_user_key 跨块串连,任一用户键重复即回退(这是 Design A 漏掉、归并靠去重兜住的洞)

## 票梯(8 票,首增量=T1)

| 票 | 内容 |
|---|---|
| **T1** | 单源洁净窗口直折(绕归并):单前缀+恰一 SSTable+无 memtable/WB 重叠的无谓词裸聚合,attach 在 simple_column_aggregate_scan 谓词分支后、visitor 构建前,纯前置检查 Ok(None) 回退 |
| T2 | 跳过式+逐块回退:孤立 tombstone/超时单条 SKIP 而非整段 abort,只把真脏块(多版本键)流经逐条归并,偏聚合状态可结合合并 |
| T3 | 列式 SoA 类型化 scratch:一趟批量 column_bounds+decode_scalar_span 入 Vec<i64>/Vec<f64>,升序折叠序不变 |
| T4 | 元数据直答 COUNT/MIN/MAX(复用 SsTableSqlZoneMap 既有字段,两派修正) |
| T5 | 多源块内洁净预聚合(归并内复用 skip-offsets 守卫集) |
| T6 | 带谓词裸聚合(zone-map 整块全过/全拒 + 部分块逐条谓词回退) |
| T7 | 多前缀/分片表 + 单组 GROUP BY(顺序折叠保前缀-键序) |
| T8 | 并行列式分区 + 有序结合(升序分区结合,匹配现并行序) |

## T1 首增量(详细)

attach:column_scan.rs simple_column_aggregate_scan(1206),谓词索引扫描尝试后、
column_aggregate_states+visitor 构建前插:
`if predicate.is_none() { if let Some(v)=self.try_columnar_single_source_aggregate(...).await? { return Ok(v); } }`
Ok(None) 时既有 ColumnAggregateScanVisitor 路径逐字不变(纯前置,零风险)。
新 try_columnar_single_source_aggregate:prefixes.len()==1 门;downcast &FusionTransaction
(只读,先例 scan/mod.rs:1636);schema 用 load_schema_for_data_prefix_filter(与 Exact 路径同源,
已核 query_schema_key==schema_key);每块洁净窗口内 user_key=key_user_part(key,TS_SIZE)、
payload=&block[value_start+1..value_end],**仍过 routed_data_entry_belongs_to_table 成员守卫**
(不省——省了就是 Design A 的过计数 bug),decode_column 折入复用 ColumnAggregateState。

## 正确性(逐位一致,逐维验证)

仅当"洁净窗口按升序用户键把每个可见-put-单版本行恰折一次、经同一 fold"才返回 Some,否则
Ok(None) 由原路径答。**浮点求和序=全局升序用户键**:并行路径 bounds.windows(2) 升序、
消费端顺序 drain、每分区归并升序 ⇒ visitor 见全局升序;快路径按块偏移(=键)序、单源单版本
块内条目序==用户键序 ⇒ 同一值序列 ⇒ f64 累加逐位同。**不做逐块偏和结合(会重排)**。
NULL/tombstone/DECIMAL/成员过滤全维度已验。

## 测试计划

①随机差分(主门):随机行数/列型/compaction 态,同表同时跑快路径与强制回退(测试钩子禁用),
断言 finalized Vec<Value> 逐字节等(SUM/AVG 比 f64::to_bits 锁浮点序);矩阵覆盖 7 种聚合 +
跨 [start,end) 边界 + 同 SSTable/边界块的邻表。②成员回归(钉死 Design A 的洞):前缀内植入
含冒号 row-id 的非 PK 行 + 不等于 routed_data_key 的 PK 键,断言快路径排除,与原路径一致——
去掉成员守卫此测必红。③守卫拒绝矩阵:每条 G1-G7 各造一个违反样本,断言 Ok(None) 回退且结果正确。

## 性能预期(诚实)

消除:每行 BinaryHeap push/pop + VisibleMergeItem Ord、每条双 decode_key、mpsc send +
per-partition spawn、一层 dyn ScanVisitor。**不消除**:逐列 decode_column 不变;成员守卫
保留 ⇒ PK 表仍付第二次 decode_column(pk)。**估计:有意义但非数量级**(执行层削减,存储侧
逐条目读仍在)。真正数量级要 T3(SoA 批量)+ T4(元数据直答 COUNT/MIN/MAX)。

## Open questions

- JOIN / Revenue profile — does the ladder reach it? NO, by construction. This is a single-table, single-routed-prefix, unpredicated, BARE-COLUMN-argument accelerator: simple_column_aggregate_projection (column_scan.rs:1118-1191) rejects expression args via column_arg_index (so SUM(l_extendedprice*(1-l_discount)) never enters) and rejects DISTINCT; and there is no join operator anywhere on this path. The Revenue profile's per-row cost lives in the hash/merge-join probe + per-row expression evaluation, which this merge-bypass never touches. Reaching it needs a SEPARATE initiative (vectorized join probe + expression evaluation over columnar batches); the ladder tops out at T8 predicated/parallel single-table aggregates. Recommend stating this explicitly so the campaign does not expect JOIN wins from this line of work.
- BOTH DESIGNS MISSED that SsTableSqlZoneMap (sstable.rs:671) already carries put_count + tombstone_count (+ row_count/null_count/non_null_count/min_scalar/max_scalar/bounds_valid) per block per table-prefix. T4/T5 must REUSE these and add ONLY max-real-ts + single_version — not re-add tombstone metadata. Confirm sql_zone_maps_complete + bounds_valid semantics before trusting them for metadata-only answers.
- Abort granularity for T1: whole-range fallback (chosen — simplest/safest, and correct since threaded G7 still catches multi-version) vs skip-through tombstone/invisible singletons (pulled into T2). Confirm benchmark tables are compacted/quiescent enough that whole-range abort on a stray tombstone/invisible is rare; if not, pull T2 forward.
- Membership schema source: VERIFIED query_schema_key_for_table == schema_key_for_table (both "schema:"+name), so reusing the planning schema for membership is bit-identical today. Add a guard/test so that if these keys ever diverge, membership follows load_schema_for_data_prefix_filter (mod.rs:1534), not the planning-time argument.
- Overlap-helper boundary semantics: the helpers are INCLUSIVE ([first,last]) but the scan is half-open [start,end); passing end as the inclusive last over-declines only at a key exactly==end (next table) — safe. Confirm this is acceptable vs threading a half-open variant (minor coverage at the exact boundary).
- txn downcast reach: confirm the &mut dyn Transaction in simple_column_aggregate_scan is a bare FusionTransaction in all routing modes (shadow-v2 / sharded wrappers). A wrapper => downcast miss => Ok(None) => safe fallback but silent loss of the optimization — decide whether wrappers should forward.
- StorageScanOptions / block-cache: the fast path passes fill_cache(); confirm interaction with sql_bulk_scan_no_fill so the columnar block reads do not thrash or fill differently from the current iterator path.
- Visibility bumps (BlockEntrySpan, decoded_block_entry_spans, read_block wrapper to pub(crate)) — confirm none are part of a serialized/public API contract before widening; all are currently module-private.
- routed_data_prefixes_for_table cardinality in the benchmark/prod bare-aggregate tables: T1 fires only at len()==1. If sharded single-table aggregates are common, prioritize T7 so T1 actually fires.
