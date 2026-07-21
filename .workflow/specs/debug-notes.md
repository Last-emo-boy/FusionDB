---
title: "Debug Notes"
readMode: optional
priority: medium
category: debug
keywords:
  - debug
  - issue
  - workaround
  - root-cause
  - gotcha
---

# Debug Notes

## Entries



<spec-entry category="debug" keywords="startup,benchmark,server,data" date="2026-07-09" title="Benchmark server clean cwd startup" description="Avoid stale repo-root data recovery when starting benchmark server" source="main@8a12c0f">

### Benchmark server clean cwd startup

The fusiondb binary always loads fusiondb.toml from the current working directory and uses storage.data_dir relative to that cwd. Starting from repo root on 2026-07-09 printed only the early config lines and did not listen within 20s because the repo-root data directory contained about 698 MB of previous SSTables. Starting /root/FusionDB/target/debug/fusiondb from an empty /tmp/fusiondb_part21_smoke cwd used default empty data/ and immediately printed FusionDB HTTP Server running on http://127.0.0.1:8091. For short benchmark smoke runs, use a clean cwd or an isolated config/data dir.

</spec-entry>

<spec-entry category="debug" keywords="fusion,shutdown,flush,race,sstable" date="2026-07-09" title="Fusion shutdown flush double-writer race" description="Prevents background and synchronous flush from writing the same SSTable" source="main@2026-07-09">

### Fusion shutdown flush double-writer race

A parallel test run exposed an early EOF failure opening a freshly flushed SSTable during snapshot/shutdown paths. Root cause: rotate_memtable notifies the background flush loop, while create_snapshot_now/shutdown immediately call flush_all_immutable_memtables; both paths could write the same memtable id to the same SSTable file concurrently. FusionStorage now owns flush_lock: Arc<AsyncMutex<()>>. The background flush_loop and flush_all_immutable_memtables share this lock, preventing double writers while still allowing the loser to observe that the memtable was already marked flushed.

</spec-entry>

<spec-entry category="debug" keywords="unique,phantom,occ,sentinel,benchprod-464" date="2026-07-10" title="单列 UNIQUE 并发幻读洞已核实,复合 PK 无洞(BENCHPROD-464 范围修正)" source="main@47449f4">

### 单列 UNIQUE 并发幻读洞已核实,复合 PK 无洞(BENCHPROD-464 范围修正)

调研 review 声称复合 UNIQUE 有 OCC 幻读洞——核实后修正:复合 PK 安全(row_id_for_insert 对 _pkey 返回 value_key 作 row_id,并发同值写相同 data key,exact-key OCC 碰撞);单列 PK 同理安全。真洞:①单列 UNIQUE 非 PK(insert.rs:480 scan 检查 + index:t:col:value:row_id 后缀键)并发同值双提交;②非 PK 复合 UNIQUE 索引根本未校验(composite_index.rs:514 loader 只认 _pkey)——独立功能缺口;③UPDATE 可能完全不检查单列 UNIQUE(待核)。464=sentinel 键堵①;②③单独开票。

</spec-entry>

<spec-entry category="debug" keywords="flaky,sstable,frsk,reverse-seek" date="2026-07-10" title="flaky 测试定名:reverse_iterator_uses_persisted_reverse_seek_sidecar" source="main@47449f4">

### flaky 测试定名:reverse_iterator_uses_persisted_reverse_seek_sidecar

多次 --all-targets 高负载运行中偶发失败的 lib 测试终于捕获名字:storage::sstable::tests::reverse_iterator_uses_persisted_reverse_seek_sidecar(FRSK 反向 seek sidecar)。单独运行稳定通过,仅在全量并行+高系统负载下偶发,疑似时序/文件系统竞态。与 BENCHPROD-463/464 改动无关(未触 sstable)。待专项修复:审查该测试的 sidecar 持久化等待逻辑。

</spec-entry>

<spec-entry category="debug" keywords="bloom,saturation,xlarge,metrics-delta" date="2026-07-10" title="bloom 定容塌方与定位方法(BENCHPROD-468)" source="main@27935da">

### bloom 定容塌方与定位方法(BENCHPROD-468)

xlarge 装载 O(n)/批塌方根因:SsTableBuilder 四个 bloom 硬编码 expected_items(100k),32MB memtable flush 携带 30-50 万键,饱和后不存在键 100% 假阳性,PK 查重每 SSTable 真实块读。修复=set_expected_filter_items 按 flush(memtable len)/compaction(输入 entry_count 和)定容。定位方法论:单批 /metrics 增量(filter positive==check 即铁证)>> 猜测;隔离微重现(单表千行批推到 50 万行看曲线)使二分不必要。中小规模永远暴露不了装不满 memtable 的缺陷——容量类改动必须过 xlarge。

</spec-entry>

<spec-entry category="debug" keywords="range-limit,469,cpu,zone-map" date="2026-07-10" title="469 解剖中间态:Range LIMIT 29s 纯 CPU,LIMIT 未生效(2026-07-10)" source="main@a1462d2">

### 469 解剖中间态:Range LIMIT 29s 纯 CPU,LIMIT 未生效(2026-07-10)

xlarge 数据集上 SELECT * FROM bench WHERE id>250000 LIMIT 100:warm 29.3s 且存储 I/O≈0(块读 1.2KB)——纯执行层 CPU;LIMIT 未到达 scan_range(bulk fill_skip 路径证实 remaining=None);zone-map 检查 13,890 次、fail-open 50%;单行 ~58µs 反常(基准内 parallel merge ~0.4µs/行)。push_down_limit 守卫(query/mod.rs:2643)文本上应命中该查询——怀疑 selection_for_scan 形态或 primary_key_range_scan_plan 返回 None 落入 full-scan 兜底且该兜底对 Gt 谓词不走 FilteredScanVisitor 早停。复测环境:重启后 WAL 全量重放进 memtable、compaction 重写 1.84GB 后沉降。下一步:无 WHERE 的 LIMIT 对照、分支计数器定位、zone-map fail-open 成因、每行 CPU 剖析。

</spec-entry>

<spec-entry category="debug" keywords="469,limit-pushdown,index-scan,xlarge" date="2026-07-10" title="469 病灶切分完成:两个独立缺陷(2026-07-10)" source="main@a1462d2">

### 469 病灶切分完成:两个独立缺陷(2026-07-10)

对照实验(xlarge bench 表 50 万行):LIMIT 100 无 WHERE=281ms 正常;id>499900 LIMIT 100(百行区间)=31ms 正常;id>250000 LIMIT 100=29s——limit=None 到达 PK 区间分支,整个 25 万行区间物化后截断(区间边界正常、limit 丢失);val=42 LIMIT 100(val 有 idx_bench_val 索引)=6.2s——索引等值路径未命中或探测爆炸,独立病灶。push_down_limit 守卫(query/mod.rs:2643)文本应命中①——查三个 scan_single_table 调用点(1919/1932/2831)哪条路径实际执行、其 limit 实参;②先看 EXPLAIN val=42 与 try_index_scan 在 xlarge 的 stats_guided_index_probe_limit(65536 上限)是否放弃索引。另:29s/25万行=116µs/行的物化本身也反常(第三层问题,或与 zone-map fail-open 相关)。

</spec-entry>

<spec-entry category="debug" keywords="469,correction,index-persistence" date="2026-07-10" title="469 修正:索引持久性无恙,缺陷②撤销(2026-07-10)" source="main@5a18cb1">

### 469 修正:索引持久性无恙,缺陷②撤销(2026-07-10)

更正先前判断:bench.val 从未有索引——benchmark.py 设计了双表(bench 无索引测全扫/bench_idx 带索引测索引扫,4648/4763-4765),post-restart 的 val=42 6.2s 查错了表,与报告 Full scan 6.0s 一致即全扫本身。索引跨重启持久性经两轮重现验证无恙(小规模干净重启✓、checkpoint+SIGKILL✓,EXPLAIN 走索引✓)。469 剩两个真问题:①Range id>N LIMIT 未下推(整区间物化,29s,已隔离);③全扫单行成本 12µs/行 vs 活动记录的 parallel-merge 0.4µs/行地板 =30×(HTTP 路径,需扣除 JSON 序列化后定位)。

</spec-entry>

<spec-entry category="debug" keywords="469,zone-map,limit,root-cause" date="2026-07-10" title="469 根因与修复:zone-map 预计算击穿 LIMIT 早停(2026-07-10)" source="main@5a18cb1">

### 469 根因与修复:zone-map 预计算击穿 LIMIT 早停(2026-07-10)

根因:merge_visible_range 为每个 SSTable 调 sql_zone_map_skip_offsets_for_sstable,对区间内每块预评估 zone map 且每块做遍历全 memtable 的 MVCC fail-open 校验——O(区间块数×memtable 探测),LIMIT 早停被击穿(LIMIT 100 先付 25 万行区间全款)。修复:PK 区间分支仅无 limit 时挂剪枝计划(scan/mod.rs)。实测 50 万行:2715ms→2.2ms warm(~1200×),正确性验证通过。途中撤销两个错误假设:索引跨重启丢失(查错表,bench/bench_idx 双表)、limit 未下推(守卫链完好)。方法论沉淀:visits 计数+耗时打点一击定位 setup-vs-loop;'预计算 vs 早停'是一类模式——全扫 zone-map 挂载点(444 visitor 自停)同类待测。

</spec-entry>

<spec-entry category="debug" keywords="470,zone-map,fail-open,no-fill,fullscan" date="2026-07-10" title="全扫地板解剖:zone-map fail-open 风暴与 no-fill 重读(2026-07-10)" source="main@fb1393a">

### 全扫地板解剖:zone-map fail-open 风暴与 no-fill 重读(2026-07-10)

Full scan val=42(50 万行,532 命中)warm 3.1s 画像:zone-map 检查 111,120 次/fail-open 97,237(87.5%)/真跳过仅 2.6%——收益近零成本全付且每次 warm 重付;no-fill 策略(sql_bulk_scan_no_fill 默认 true)每查重读+解压 11-32k 块(14-39MB)。checkpoint 后 fail-open 不变(97,253)⟹ 与 memtable 重叠无关,结构性原因待查:读 SqlBlockZoneMapFailOpenReason(storage/mod.rs:80)与 sql_zone_map_skip_offsets_for_sstable 的 fail-open 分支分类 87%。候选修复方向:①fail-open 占比阈值熔断(某表/某谓词 fail-open>50% 时本查询放弃 zone-map);②按 reason 修根因;③no-fill 与重复扫描的权衡需要 scan-result cache 或放宽 fill 策略(部分填充)。7.4µs/行 vs 0.37µs/行地板的 20 倍差主要由这两项+27 个 SSTable 的迭代器开销构成。BENCHPROD-470 主题定为全扫地板恢复。

</spec-entry>

<spec-entry category="debug" keywords="470,fail-open,incomplete-metadata" date="2026-07-10" title="470 收窄:fail-open 全部为第三类原因(2026-07-10)" source="main@fb1393a">

### 470 收窄:fail-open 全部为第三类原因(2026-07-10)

细分计数器(schema_fail_open/mvcc_overlap 四子项)对 val=42 全扫单查增量全为零 ⟹ 97,237 次 fail-open 全部来自只进聚合计数的第三类:IncompleteMetadata/InvalidBounds/NullOrTombstone/CountMismatch(fusion.rs record_sql_zone_map_fail_open 的非 schema 分支)。头号嫌疑 IncompleteMetadata=块属性 sql_zone_maps_complete=false(采集不全:compaction 产出块跨表?builder schema 快照时机?)。下一步:evaluate_block_zone_maps 加临时 reason 直方图 eprintln→本地 50 万行重现(fdb-idx-repro 模式)→按 reason 修根因。若为采集缺陷,修 builder 侧一次性解决;若结构性,加 fail-open 占比熔断。

</spec-entry>

<spec-entry category="debug" keywords="470,seven-eighths,fail-open,handoff" date="2026-07-10" title="470 深挖状态:7/8 fail-open 之谜(2026-07-10)" source="main@b1062cf">

### 470 深挖状态:7/8 fail-open 之谜(2026-07-10)

最新事实链:①构建侧正常——snapshot 非空(1 表),12 个 kill 点零触发 ⟹ block_sql_zone_maps 返回 complete=true;②writer finish() 逻辑正确(有 zone map 走 V6,V6 From 保留 complete 字段);③但读侧 87.5%(恰 7/8)的检查落在 complete=false 块上(本地 120k 行重现:26,680 检查/23,352 fail-open/218 positive/3,110 skip,与 xlarge 同比例);④之前 snap 重现'零 fail-open'是直方图 5 万阈值假象,/metrics 增量揭穿。7/8 结构比例线索:疑与块族构成有关(data 块 vs index-key 块)或 flush 产 vs compaction 产 SSTable 差异。下一步打点:finish() 按 SSTable 打印 complete/incomplete 块计数 + skip_offsets 评估时打印 sstable id——区分哪类 SSTable 写了 false。临时打点现存 3 处(fail-open 读侧直方图/构建 kill 点/snapshot 大小),修复后一并移除。重现环境:/root/fdb-idx-repro 120k 行 + checkpoint。

</spec-entry>

<spec-entry category="debug" keywords="470,table-prefix-ranges,root-cause" date="2026-07-10" title="470 真凶定位:table_prefix_ranges 不完整,非 zone_maps(2026-07-10)" source="main@7586041">

### 470 真凶定位:table_prefix_ranges 不完整,非 zone_maps(2026-07-10)

决定性反转:读侧 IncompleteMetadata 来自 sql_zone_map_skip_offsets_for_sstable 的第三分支——block_property_table_prefix_interval(property, prefix) 返回 None(即块的 table_prefix_ranges_complete=false 或 ranges 缺失)时逐块记 IncompleteMetadata。盘上 sql_zone_maps_complete=100%(finish 普查 8559/8559)但查错了字段;真正不完整的是 table_prefix_ranges(V6 的另一字段,块级表前缀区间,zone map 评估的前置)。数字自洽:23,352 fail(区间缺失块)/3,110 skip(区间存在且不含目标表)/218 positive。下一步:读 block_table_prefix_ranges(builder 侧)的 incomplete 条件,7/8 比例应从其结构解释;修复目标=让区间采集完整或评估侧对区间缺失退化为 first/last key 粗判而非 fail-open。

</spec-entry>

<spec-entry category="debug" keywords="470,slice,fail-open,fixed" date="2026-07-10" title="470 破案与修复:并行切片误分类,(K-1)/K 假 fail-open(2026-07-10)" source="main@7586041">

### 470 破案与修复:并行切片误分类,(K-1)/K 假 fail-open(2026-07-10)

7/8 之谜闭式破案:26,680=8×3,335——并行全扫切 K=8 片,sql_zone_map_skip_offsets_for_sstable 每片遍历 SSTable 全部块,把其他 7 片的块在范围边界检查(fusion.rs 原 3251)误记 IncompleteMetadata,每块每片白付区间 Vec 克隆+计数。修复:完全在 [start,end) 外的块在计数前静默 continue,仅真边界部分重叠块 fail-open。本地验证:检查 26,680→3,342、fail-open 23,352→14、剪枝决策逐一相同(skip 3,110/positive 218)、块读 16k→232。重要正面发现:zone map 本身在 val=X 上 93% 跳过率——功能优秀,被噪声淹没。解剖方法论:闭式数字吻合(87.5%=7/8)是切片类 bug 的指纹;四次假设撤销全程 maestro 留痕。

</spec-entry>

<spec-entry category="debug" keywords="471,index-misplan,selectivity,cbo" date="2026-07-10" title="471 立案:低选择率索引误选型,10 万点查 44s(2026-07-10)" source="main@876d8d8">

### 471 立案:低选择率索引误选型,10 万点查 44s(2026-07-10)

Avg order value(SELECT AVG(total) FROM orders WHERE status='delivered')warm 44s 画像:EXPLAIN 显示 Index Scan using status,但 status 选择率 ~50%(200k 行中 ~100k 命中)→ 10 万次逐行 txn.get 点查(user_key_filter_check 100,001、positive 34,013、block_cache_hit 34,528),每次 ~440µs。顺序全扫同表 ~1s。stats_guided_index_probe_limit(cap 65536,scan/mod.rs:523)未拦:疑无 ANALYZE 统计时默认走索引。修复方向:①用已维护的 index count summary(低基数列免费精确计数)或 count_prefix 流式预判命中数,>表占比阈值(或概率上限)退回全扫;②同类受害者:Revenue by category 10s(category 低基数)、Never-ordered 8.5s(NOT IN)、Subquery IN 24s 待逐一画像。方法照旧:单查 metrics 增量。

</spec-entry>

<spec-entry category="debug" keywords="471,fix-design,probe-cost" date="2026-07-10" title="471 修复设计定稿(2026-07-10)" source="main@876d8d8">

### 471 修复设计定稿(2026-07-10)

gate 缺口双因:①load_table_stats 为 None(无 ANALYZE)直接 Ok(None)=无界探测;②成本模型 index_cost=log2(rows)+est_rows 把点查按顺序行等价计价,实际点查 ~100-400µs vs 顺序 ~2-7µs/行(50-100×),有统计也会误选(100k est<200k rows 仍走索引 65536 探测≈28s)。修复:①统计无关兜底——try_index_scan Eq/IN 路径收集 row_ids 后计数免费,len>STATS_INDEX_PROBE_LIMIT_MAX(65536)即放弃索引退全扫(与既有 cap 语义一致);②成本模型点查加权 PROBE_COST_FACTOR(取 16 保守):index_cost=log2+est×16 vs table_rows。小命中索引路径不受影响;xlarge orders 实测目标 44s→~2s。

</spec-entry>

<spec-entry category="debug" keywords="471,column-scan,aggregate,correction" date="2026-07-10" title="471 再修正:肇事路径是二级索引聚合扫描,非 try_index_scan(2026-07-10)" source="main@876d8d8">

### 471 再修正:肇事路径是二级索引聚合扫描,非 try_index_scan(2026-07-10)

try_index_scan 有完备弃用机制(index_candidate_cap(None,None)=1024,should_use_index_plan 超限即弃)——100k 探测不可能来自它。AVG(total) WHERE status='x' 为纯聚合投影,走 column_scan.rs 的 secondary-index aggregate scan(~1293,7 月功能):扫 status 索引条目后逐行取数聚合,无候选上限。Revenue by category 10s 应为其 group_by 变体(group_by_count/aggregate_column_scan)。修复点改为:该家族入口加候选计数闸门(复用 STATS_INDEX_PROBE_LIMIT_MAX 或 index_candidate_cap 语义),超限退回全扫聚合(simple_column_aggregate_scan 批式全扫已高效);统计存在时用 stats 预判少付一次索引条目扫描。教训:同一逻辑查询在不同投影形态下走完全不同的执行家族,gate 必须逐家族审计。

</spec-entry>

<spec-entry category="debug" keywords="473,subquery-in,sstable-reopen" date="2026-07-10" title="473 立案:Subquery IN 31s,查询内重复 SSTable 开销(2026-07-10)" source="main@6d572d2">

### 473 立案:Subquery IN 31s,查询内重复 SSTable 开销(2026-07-10)

SELECT * FROM users WHERE id IN (SELECT DISTINCT user_id FROM orders WHERE total>500) LIMIT 20:warm 31s(应 ~1.5s:orders 扫描+DISTINCT 物化+users IN 集合扫描)。两轮测量均被重启后 compaction 波次污染(每轮数百 MB 混入),但查询自身增量含决定性异常:sstable_open_meta_bytes 67MB/查 + open_filter 39MB + open_total_us 297ms——单查询内反复 OPEN/解码 SSTable meta;另见首轮 762k 次 point_overlap_skip(IN 成员判定疑逐行点查而非哈希集)。嫌疑:①deferred IN-membership(subquery.rs:661-838 per-value cache)对每候选行做存储点查;②物化路径重复重扫 orders;③no-fill 下 meta/filter 不缓存导致每次子扫描重开。Never-ordered(NOT IN)26.5s 同族。解剖方法:待系统安静(compaction 彻底完成)后单查增量+subquery.rs 路径读码;或本地 fdb-idx-repro 造 orders/users 双表干净重现。

</spec-entry>
<spec-entry category="debug" keywords="upsert,unique,conflict-target,on-conflict,insert" date="2026-07-12" title="UPSERT 顺序缺陷与 conflict_target 被忽略(单列 UNIQUE 调查立案)" description="UPSERT 对 unique 列的顺序缺陷与 conflict_target 忽略,含 INSERT..SELECT/UPDATE 残留 O(N²)" source="main@c24e8b4">

### UPSERT 顺序缺陷与 conflict_target 被忽略(单列 UNIQUE 调查立案)

对抗验证过的两处正确性缺陷,待独立修复:①单列 UNIQUE 查重先于 ON CONFLICT 分支执行(insert.rs 两条路径),对自身持有相同 unique 值的已存在行执行 INSERT ... ON CONFLICT DO UPDATE/DO NOTHING 会先报 UNIQUE constraint violated 而非执行冲突动作;②oc.conflict_target 全文无引用,冲突只由数据键 txn.get 判定——非 PK unique 列上的真冲突(不同 PK、相同 unique 值)不触发 upsert 动作,与 PostgreSQL 语义不符。另:INSERT..SELECT 与 UPDATE 仍走 validate_unique_columns_for_update 每行全表扫描(批量 O(N²)),可复用本轮 UniqueColumnValueSets 机制(dml/mod.rs);完整根治方向=把复合 UNIQUE 的 sentinel-first + authoritative marker 模式移植到单列。

</spec-entry>

<spec-entry category="debug" keywords="flaky,全局计数器,测试污染,反向扫描,metrics" date="2026-07-12" title="flaky 根因关闭:全局 metrics 精确差值断言禁用,同类模式警示" description="reverse_seek_sidecar flaky 根因=并行测试污染共享计数器,已改迭代器本地统计;fusion.rs 同类全局差值测试待排查" source="main@c24e8b4">

### flaky 根因关闭:全局 metrics 精确差值断言禁用,同类模式警示

reverse_iterator_uses_persisted_reverse_seek_sidecar flaky 根因确认并修复(对抗验证):GLOBAL_METRICS 为进程级共享原子计数器,lib 测试并行于同一进程,对其做 ==0/==N 精确差值断言必然被并发兄弟测试污染(至少 15 个反向迭代测试碰同族计数器);仅单调 >= 差值断言污染免疫。修复=SsTableReverseIterator 累计自身 ReverseBlockScanStats,精确断言全部本地化。规则:测试中禁止对 GLOBAL_METRICS 写 ==精确差值断言,一律用迭代器/组件本地统计;全局只留 >= 接线检查。已知同类待排查:fusion.rs 多处测试用全局差值模式(5739/5985/6147/6204/6617/6738/6837 附近),若再现 flaky 按同法迁移。H2(sidecar 文件系统竞态)已排除:finish() 内 fsync+原子改名,fingerprint 取自 sync 后稳定元数据。

</spec-entry>

<spec-entry category="debug" keywords="upsert,conflict-target,unique,闭环,insert" date="2026-07-12" title="UPSERT/UNIQUE 立案三项全部闭环(e9cce62/0cc1926/7cdf50f)" description="UPSERT 顺序缺陷、conflict_target 忽略、UPDATE/INSERT..SELECT O(N²) 均已修复;遗留 DO UPDATE 改 PK 列问题" source="main@7cdf50f">

### UPSERT/UNIQUE 立案三项全部闭环(e9cce62/0cc1926/7cdf50f)

2026-07-12 立案的三项全部落地:①UPSERT 顺序缺陷(e9cce62)——单列 UNIQUE 查重移到 ON CONFLICT 分支后,自冲突 upsert 不再误报;②conflict_target(0cc1926)——单列非 PK UNIQUE 目标按 值→owner row_id 映射解析并对 owner 行执行 DO UPDATE/DO NOTHING,复合 PK 全集目标=数据键冲突,其余目标与 DO UPDATE ... WHERE 一律 loud 报错(取代静默忽略),两路径 DO UPDATE 主体收敛为 apply_on_conflict_do_update;③UPDATE/INSERT..SELECT/upsert 批量 O(N²)(7cdf50f)——四个调用点统一到 UniqueColumnValueSets(自排除=owner row_id 比较),旧扫描验证器删除,UPDATE 106×/INSERT..SELECT 52×(2000 行)。契约:单列 UNIQUE 查重只能走 check_unique_columns_for_insert/for_update,不得再写内联扫描或逐行验证器。遗留(未立案):DO UPDATE 赋值改 PK 列时行仍写在旧 row_id 键下(数据键与 PK 值不一致,PK 冲突与 unique 冲突路径同病),需要 row_id 迁移或 loud 拒绝,待独立票。

</spec-entry>

<spec-entry category="debug" keywords="pk-immutable,row_id,upsert,distinct-decimal,fanout" date="2026-07-12" title="PK 不可变守卫 + DISTINCT DECIMAL 合并闭环(1990671/1f49bce)" description="UPDATE/DO UPDATE 改 PK 的行键错位腐蚀已 loud 拒绝;462 遗留的 DISTINCT over DECIMAL 合并已修" source="main@1f49bce">

### PK 不可变守卫 + DISTINCT DECIMAL 合并闭环(1990671/1f49bce)

①PK 不可变(1990671):重现证实 UPDATE SET id=N 或 unique 目标 upsert SET id=EXCLUDED.id 会把行搁浅在旧 row_id 键下——全表扫描直接丢行、新旧 PK 点查双双失真(比预期严重)。reject_primary_key_change 覆盖单列 is_primary 与复合 _pkey 身份索引列,UPDATE 主循环与 apply_on_conflict_do_update 赋值后立即拒绝;同值赋值放行。两个 FK 测试原依赖父键变更报 FOREIGN KEY,现被 PK 守卫先截获(断言已更新;无子行时旧行为是静默腐蚀)。row_id 迁移(完整 PK 变更支持)仍待设计票。②DISTINCT DECIMAL(1f49bce):sum_distinct/avg_distinct 门放行 DECIMAL 但合并端对 JSON 字符串形态的原始值报 non-numeric——http/pg 两端合并循环先经 fanout_decimal_f64/forward_decimal_f64 映射,已双端修复+双节点集成测试。fusion.rs 全局差值断言测试群维持"再现 flaky 时按 sstable 同法迁移"的观察策略(需先给 FusionStorage 扫描 API 加本地统计管道,==0 断言不可安全弱化为 >=)。

</spec-entry>

<spec-entry category="debug" keywords="outer-join,谓词下推,null-padding,反连接,join" date="2026-07-12" title="外连接谓词下推两族静默错误闭环(613d440)" description="ON 保留侧谓词丢保留行 + NULL 侧 WHERE 被吞;按 join 类型门控提取点,残余走逐对求值/后置过滤" source="main@613d440">

### 外连接谓词下推两族静默错误闭环(613d440)

memory 挂账已久的"outer-join predicate-pushdown caveat"重现属实且双族:①apply_join_step 把 ON 中仅引用保留侧的合取项无条件下推为扫描过滤(LEFT/FULL 左、RIGHT/FULL 右被丢行);②NULL 侧 WHERE 合取项被 take_* 提取吞掉后不再后置应用(NULL 填充行错误存活,IS NULL 反连接错乱)。修复原则:ON 保留侧合取项留在 join 谓词走 residual_expr 逐对求值(probe/expr-hash/hash/nested-loop 全路径已支持);链含 RIGHT/FULL 则整链禁 WHERE 下推(含首表与 comma 对谓词,后续步可 NULL 填充任何早先关系列),本步 LEFT/FULL 则右侧 WHERE 不提取——execute_join 尾部后置过滤兜底。教训:下推安全性 = f(谓词来源 ON/WHERE, 侧别, 本步与后续步的保留性),四维都要判;纯 INNER 链不受影响。遗留权衡:comma+RIGHT/FULL 混链的对谓词不再提取会退化为笛卡尔物化后过滤(极罕见,正确性优先);FULL JOIN ORDER BY 的 NULL 排序为 NULLS FIRST(asc)。

</spec-entry>

<spec-entry category="debug" keywords="data-v2,phase,fence,p10-2-1,评审事故" date="2026-07-20" title="P10-2.1 落地:phase record + commit fence,评审抓 1 blocker + 6 项(df21565)" description="持久相位取代 flag;评审抓到同事务混排/superuser 门绕过/fence 非单调等;评审 agent 误删未提交工作" source="main@df21565">

### P10-2.1 落地:phase record + commit fence,评审抓 1 blocker + 6 项(df21565)

持久相位 record(Catalog identifier key,18B 定长严格 decode)取代进程本地 flag,fence 三层:启动灌好并随 invalidate_storage_caches 失效(一行同覆盖 raft apply 与 snapshot install 两调用点)、单机 commit_lock 内等值校验且 advance 同临界区发布、分布式转 precondition + apply 单调守卫。**对抗评审 19 findings 修 7 项**:①blocker 同事务混排——`BEGIN;INSERT;CALL advance;COMMIT` 会把按旧相求值的行与新相 record 原子共提交(commit 的 pin 校验只看已发布 fence,本事务 staged 的 record 尚未发布,二者互不感知;standalone 规则只拦单字符串多语句),修复=staged record 与 fence pin 并存即 loud abort;②superuser 门 `starts_with("CALL ")` 裸前缀被 `/*注释*/`、TAB 绕过且 statement_permissions(Call) 为空 ⇒ 非 superuser 可改相位,门移到 authorize_statement 按解析语句判定;③resolve_with 无单调性,陈旧 MVCC 快照覆盖已发布新 fence 后写以旧相干净提交;④raft evaluate 路径缺 standalone;⑤install 在 commit_lock 内改 record 但 invalidate 延迟到上层的陈旧窗口;⑥空 write_buffer 早退跳过 pin 校验;⑦无 record 时 flag 来源从 Executor 漂到 FusionStorage。**契约**:data-family 写只能经 write_routed_data_row/delete_routed_data_row/delete_structured_data_shadows_for_table 三个 helper(它们打 fence);相位推进必须独占事务。**基准方法论教训**:medium harness 噪声底噪极大(baseline-vs-baseline 逐查询 p90 |delta| 36.2%、装载基线跨时段自漂移 +14%),首轮 n=4 的"装载 +9.5% 回归"假设**已撤回**(n=8 合并基线 +2.4%/t=0.63);判噪内部对照=只读扫描也"可复现变慢",而写路径改动不可能影响纯 SELECT。**过程事故**:评审 subagent 写 in-tree probe 测试后用 `git checkout src/execution/mod.rs` 清理,抹掉该文件约 600 行未提交改动(其余 6 文件完好),靠 cargo check 报"刚写的函数找不到"发现并重做——**规则:评审 agent 必须只读,多 agent 评审未提交工作前先 git stash create 快照**。

</spec-entry>

<spec-entry category="debug" keywords="data-v2,cleanup,skip-scan,p10-2-2,只读评审" date="2026-07-20" title="P10-2.2:v2 影子清理 route 跳扫有界化(1a7cf5c),只读评审 0 confirmed" description="全命名空间扫描改每 route 一次探针 seek;计数测试证明 2002 行→4 键;只读评审机制生效" source="main@1a7cf5c">

### P10-2.2:v2 影子清理 route 跳扫有界化(1a7cf5c),只读评审 0 confirmed

`delete_structured_data_shadows_for_table` 原为 `scan_prefix(data_namespace_prefix())` 物化整个 v2 namespace 的 (k,v) 后逐键 parse —— backfill(2.3)灌满 namespace 后每次 DROP/TRUNCATE 退化 O(全部影子行),故为 2.3 前置。改 route 区跳扫:`scan_range(cursor, ns_end, Some(1))` 取探针键(FusionStorage visitor 首行 false 早停)→ `parse_data_key_exact().route()` → 只扫 `(route,表)` 自己区间(收键不收值)→ 游标 `prefix_end(encode_data_route_prefix(route))` 跳过整个 route 区。**前缀安全性根因**:表名 4 字节 BE 长度前缀编码 ⇒ `orders` 与 `orders:archive`/`orders_2` 区间天然不嵌套,冒号邻表不可能误删(这也是原实现能安全按表名匹配的同一性质)。**终止性**:探针键 ≥ 游标且以 route 前缀开头 ⇒ `prefix_end(route_prefix) > probe ≥ cursor` 严格递增;`Shard(u64::MAX)` 进位落到 route tag(0x01→0x02)仍在 namespace 内,下轮扫空即止。**语义差(设计稿有意裁定)**:malformed-key loud 检测范围由"整个 namespace"收窄为"本次触碰范围"。**有界性证明方法**:`CountingTransaction` 装饰器计数扫描交还的键,2002 行 namespace → 触碰 4 键(2 探针 + 2 目标行);关键是该断言对旧实现会失败(旧实现 2002),否则测试空过——**证明有界性要用对旧实现会红的计数断言,不要用时延**(本 harness 噪声 p90 36% 根本测不出)。**只读评审机制生效**:agentType=Explore + 提示词禁写 + 评审前 `git stash create` 快照,17 findings 全驳回且工作区完好(对照 2.1 的评审 agent `git checkout` 删档事故)。仍采纳四条质量改进:测试替身静默丢弃 fence(3 agent 独立提出,替身必须委托 inner 而非继承 trait 默认)、`prefix_end` None 分支静默跳过改 loud、文档不再宣称干净 O(routes+rows)(FusionStorage 每次扫描合并写缓冲,按 route 数付费)、补 FusionStorage 真引擎与"只存在于写缓冲的 route"测试。

</spec-entry>

<spec-entry category="debug" keywords="data-v2,backfill,cursor,cte,fence,p10-2-3" date="2026-07-20" title="P10-2.3 backfill 引擎落地(3e81baa):游标毒化 blocker + CTE 守卫误伤" description="分块续跑 backfill;评审抓到游标可写成读不回来的记录(波及全库 DROP)与 apply 守卫误伤 CTE" source="main@3e81baa">

### P10-2.3 backfill 引擎落地(3e81baa):游标毒化 blocker + CTE 守卫误伤

分块 backfill:一事务 = 若干行 v2 影子写 + 游标更新(同生共死),256 行/1 MiB 双封顶(每键在 commit_lock 内一次 latest_committed_timestamp 探测)。**枚举必须走物理键序两段区间而非 ShardRouter**(router 只枚举当前 shard_count 且启用后不含 unsharded 前缀)。**表身份识别**:预加载 schema 目录用已知表名切分,多候选用该行主键消歧,孤儿跳过——注意 `get_primary_key_index` 按 `is_primary` 搜索,**主键不一定在第 0 列**,按第 0 列硬解是错的(自查改掉)。**DDL 冲突点必须无条件写**状态记录:首个 chunk 才是创建者,条件式重写恰好漏掉它。**相位闸门 MAX_SUPPORTED 与 MAX_ADVANCE_TARGET 必须同时升**,只升后者会砸库(数据写全拒/拒开/apply 停机)。

**blocker 游标毒化**:游标取自任意访问过的键,而 `shard:N:index:`/`unique:` 键嵌入无上限列值,可超过 decode 上界;当时 `encode` **无**校验 ⇒ durable 写出一条自己读不回来的记录。**波及面远超 backfill**:`touch_backfill_state_for_ddl` 也读它,于是全库每次 DROP/TRUNCATE 永久失败,且 Catalog 键无 SQL 途径修复。评审补充:shard 区内族序 `data<fts<index<unique`,扫尽时末键**通常**就是 unique 哨兵 ⇒ 毒化是常规终态而非奇葩路径。**规则:任何 durable 记录的 encode 与 decode 必须校验同一组约束,"可编码必可解码"要有测试钉死**;修复=上界 4 MiB + encode 侧同样校验(超界写时 loud,绝不产出不可读记录)。

**major CTE 误伤 + 守卫假想敌不可达**:`is_data_family_key` 匹配 `data:{cte}:{row}`,而 CTE 材料化不打 fence ⇒ 集群到 Backfill 后,外层选出 0 行的 `INSERT ... WITH ...` 批次里只有 CTE 写、无 phase precondition,被新守卫拒绝并附赠误导性"提议节点需升级"。评审进一步证明该守卫的**假想敌不可达**:旧二进制在 advance→Backfill 那条 entry 上就先停机,不可能之后当 leader 提议未 fence 的写 ⇒ **假阳性可达、真阳性不可达**。修复取根因:**凡写 `data:` 命名空间者都必须遵守 P10-2.1 fence 不变量**,给 CTE 材料化补 fence,守卫随之健全。

**撤回**:2.1/2.2 工单曾写"backfill 必须排除 CTE 键"——CTE 行在同一事务内 put 后即 clear(`handle_query` 尾部无条件清理),MVCC 只在提交时发布,并发事务永远看不到活的 CTE 行,枚举层面无需排除(本票给 CTE 补 fence 是为 raft 守卫,与枚举无关)。

**未结**:曾有一次 `--lib` 报 672/2 failed 但未捕获测试名,此后 7 次运行(含与 --all-targets 并发压测)全绿无法复现;疑为既有 GLOBAL_METRICS 高负载 flaky 家族但**未经证实**,再现须第一时间捕获名字。

</spec-entry>

<spec-entry category="debug" keywords="regression,order-by,topk,early-stop,benchmark,port" date="2026-07-20" title="性能回归实证:ORDER BY PK LIMIT 丢失早停(b0bd059 起),伴随基准装置端口缺陷" description="同机新旧二进制对照证实中位 3.75x 回归;根因定位到有序 top-K 早停失效;基准脚本不校验端口归属曾污染多轮测量" source="main@3d08f0a">

### 性能回归实证:ORDER BY PK LIMIT 丢失早停(b0bd059 起),伴随基准装置端口缺陷

**测量装置缺陷(先修这个,否则一切数据不可信)**:基准脚本只用 `curl 8091/health` 判就绪,且**从不校验自己启动的服务端是否真的持有客户端要连的 8092**。本机 8091 被 docker-proxy 常驻占用,且残留旧会话的 fusiondb 进程会占 8092/8093;端口被占时 FusionDB 的 pgwire 监听线程 panic 但**进程继续运行**(HTTP 自增到 8096),于是 benchmark 一路连到别的服务端(不同构建/不同数据集),表现为大量 "Table X not found" 与最终 Connection refused。**规则:基准脚本必须在启动前检查端口空闲、启动后用 `ss -ltnp` 确认本进程 pid 持有该端口、日志含 panicked 一律 loud 退出**。今日多轮 A/B(含 P10-2.1 记录的"噪声底噪 p90 36%")均受此污染,该噪声数字作废。

**回归实证(同机同盘、同一份 benchmark.py、large+pg)**:6/29 二进制(5be78c9)vs HEAD 中位 **3.75x 慢**,103 条中 60 条 >3x。机器因子仅 **1.25x**(旧二进制本机 vs 6/29 归档),故非硬件。既有表行数完全相同(多出 4 万行来自新增 join_reorder part)。`sql_bulk_scan_no_fill`(6/29 配置中不存在,今默认 true)仅解释约 10%(开 fill 后中位 0.91x)。**方向是分化的**:重扫描真实变快(LIKE 330→85ms、IN list 310→85、Full scan 314→126,来自 P9-6 并行归并 + P9-7 jemalloc + 469/470),但毫秒级快路径与写入慢 10–100x(ORDER BY id LIMIT 50 0.26→22ms、Total bank balance 0.18→9.7、AND filter 1.28→25、单行写 0.2→1.4-2.7、装载 9.8→14.6s)。

**根因定位(计数器差值,绕开结果缓存用唯一查询文本)**:`SELECT * FROM bench ORDER BY id LIMIT n`——6/29 二进制 0.4-1.8ms 且**存储计数器全零**(有序 PK 扫描取够即停);HEAD 13-54ms、`block_cache_miss=1263`、`sstable_block_read_bytes=1.66MB`、**`query_sort_fallback_count=1`**(物化后显式排序)。回归在 `b0bd059`(7/10 大检查点,4.4 万行)时已完整存在。入口条件 `order_by_allows_streaming_topk`(order.rs:349,该提交引入)对 `ORDER BY id` 应返回 true、`streaming_order_limit` 应为 Some(50),**即入口满足但流式 top-K 实际未生效**——修复票应从 `scan_single_table` 是否兑现 streaming_order_limit 查起。这是 memory 已记载的 **"预计算 vs 早停" PATTERN** 的新实例(469 修过 PK 区间扫描同类问题,并明确留有未审计挂载点)。

**附带发现**:`src/server/pg_server.rs:9216` 在 pgwire 热路径对每条查询 `eprintln!` 完整 SQL(一次 Part 1 运行写 7.6MB,批量 INSERT 的 500 行 VALUES 被整条打印)。自 2026-01-06 初始提交即存在,**非本次回归主因**(新旧二进制都有),但应单独清理。

</spec-entry>

<spec-entry category="debug" keywords="order-by,topk,early-stop,pk,regression,scan-limit" date="2026-07-20" title="ORDER BY PK LIMIT 回归二次收窄:主键这个最优情形反而丢了早停" description="逐形态实测锁定只有 PK 有序回归;streaming top-K 的 limit.is_none() 门是诱因但放开它更慢;真正的洞在 scan_limit 未兑现" source="main@d58c488">

### ORDER BY PK LIMIT 回归二次收窄:主键这个最优情形反而丢了早停

同数据集逐形态实测(HEAD):`SELECT * ORDER BY id LIMIT n`(主键)65ms/962 块未命中/1.26MB;`ORDER BY val LIMIT n`(非主键)150ms/**0 块**;`SELECT id ORDER BY id LIMIT n`(仅键)12ms/**0 块**;`SELECT * LIMIT n`(无 ORDER BY)21ms/**0 块**。**即:普通限量扫描的早停是好的,非主键 ORDER BY 的流式 top-K 早停也是好的,唯独主键这个本该最快的情形回归**。

诱因定位:`src/execution/scan/mod.rs` 约 2276 行,流式 top-K 入口条件含 **`limit.is_none()`**;而主键 ORDER BY 恰恰让 `primary_key_order_scan_limit` 把 `limit` 置为 `Some(n)`,于是该路径被跳过。控制流随后到达普通路径,那里 `scan_limit = effective_limit` 本应为 `Some(n)`,但实测仍读 962 块 —— **limit 在 `effective_limit` 到存储扫描之间丢失或未被兑现**,这是下一步要插桩的确切位置。

**已验证的错误修法(勿重复)**:直接去掉 `limit.is_none()` 门 —— 排序回退确实消失、结果正确(升序 0-4/降序 49999-49995),但**更慢**(65→144ms),因为 top-K visitor 为给任意列排名必须扫全表。主键升序下数据本已按键有序,正解是让既有的 `scan_limit` 路径真正早停(取够 n 行即止),而非改走 top-K。已回退,工作区干净。


<spec-entry category="debug" keywords="regression,write-path,fsync,durability,baseline" date="2026-07-20" title="写路径"回归"实为持久化代价:6/29 基线的 WAL 根本不 fsync" description="旧 wal.rs 零个 sync_data/sync_all;单行写 0.21ms 是无持久化的假快;写侧对比基线无效" source="main@5945031">

### 写路径"回归"实为持久化代价:6/29 基线的 WAL 根本不 fsync

追剩余回归时对写路径做计数器差值:HEAD 单行 INSERT 只有 5 次点探 + 1 次块读 + 796B WAL 写入,**CPU 侧没有异常工作量**,3.5ms 的量级只能是 fsync 绑定。而基线的 0.21ms 对 ext4 上一次 fsync 而言**物理上太快**。核实:`git show 5be78c9:src/storage/wal.rs`(781 行)中 `sync_data|sync_all|fsync|durab` 命中数 **= 0**,只有 `BufWriter::flush()`;当前 wal.rs(2160 行)有完整同步纪律,929 行由 `b0bd059` 一次性加入(即 ROADMAP 的 P1-3"分段 WAL/同步文件与目录持久化"落地)。

**结论:单行写慢 6-13x、批量装载慢 1.5x 不是缺陷,是把"提交后掉电会丢数据"换成了真正的持久化。** 拿 6/29 二进制做写侧基线是无效对照——那是一个不 fsync 的数据库。

**由此必须修正此前的结论**:"同机中位慢 3.04x"这个数字里,凡涉及写入的项都在拿持久 vs 非持久对比,不能计入回归。真正需要继续追的只剩**纯读路径**的差距。方法学教训:跨版本基准对比前,必须先确认两侧的**持久化语义相同**;语义变了就不是同一个可比对象(与 tmpfs 陷阱同源——那次是介质免除 fsync,这次是代码根本不调用 fsync)。

</spec-entry>

<spec-entry category="debug" keywords="regression,parallel-scan,integer_pk_range_splits,reverse-scan,small-table" date="2026-07-20" title="小查询固定开销定位:并行切分探测对每次扫描做一次完整反向扫描" description="integer_pk_range_splits 调 last() 触发反向迭代器+前沿探测,小表上该探测即全部耗时,且结果因低于阈值被丢弃" source="main@c2c5f71">

### 小查询固定开销定位:并行切分探测对每次扫描做一次完整反向扫描

追纯读路径剩余回归(按写入拆分后:纯读 84 条中位 2.53x、35 条 >3x;含写 19 条中位 6.46x 已证实为持久化代价)。500 行 `accounts` 表上 `COUNT(*)` 与 `SUM(balance)` 的存储计数器**逐项完全相同**(含 `fusion_reverse_scan_count=1`、`sstable_reverse_block_span_scan_entry_count=35`),块缓存 21 命中 0 未命中却耗时 2.1-2.7ms——不同查询给出相同计数器 ⇒ 该工作不属于查询本身。空对照(两次 /metrics 间不跑查询)计数器全零,排除探针自身。

在 `fusion.rs` 反向扫描入口打 backtrace,调用链坐实:
`count_routed_data_prefixes_for_table → scan_prefix_parallel_for_each_with_options → scan_range_parallel_for_each_with_options → integer_pk_range_splits → txn.last() → merge_visible_range_reverse`。

**即:每次可并行扫描都先做一次完整反向扫描探最大键(开全部 SSTable 反向迭代器 + 前沿探测/收紧 + 物化 35 条 block span)来计算并行切分,随后因表行数低于 `PARALLEL_SCAN_MIN_ROWS=8192` 把切分丢弃。** 探测成本在大表被摊薄,在小表即全部耗时——正是"亚毫秒查询变 10-40ms"的固定开销来源。

**候选修法(未实施,择一或组合)**:①用 manifest 里已有的 SSTable first/last key 描述符估算键区间,避免实时反向扫描(启动期已加载,零 I/O);②把 (表前缀 → min/max) 的探测结果按事务或按扫描缓存,避免同查询多前缀重复探;③先用便宜信号(如 count summary / 统计器行数估计)判定是否值得并行,不值得就跳过探测。注意 ①最彻底但需确认描述符在 memtable 有新写入时的正确性(区间只需覆盖,不需精确)。


<spec-entry category="debug" keywords="parallel-scan,gate,reverse-scan,fixed-cost,query_total_us" date="2026-07-20" title="并行切分探测门落地(2657029);剩余固定耗时转向执行层" description="小表反向扫描归零(计数器证据);墙钟收益噪声内不可分辨;下一线索=存储计数器近零仍 1.5ms 的服务端内耗时" source="main@2657029">

### 并行切分探测门落地(2657029);剩余固定耗时转向执行层

`range_entry_upper_bound_capped`:先累加重叠 SSTable 块条目数(预载元数据,大表几块即超阈值早停),不足再对 memtable 做封顶区间走查;上界 < `PARALLEL_SCAN_MIN_ROWS` 直接跳过 first()+last() 探测。**顺序要点**:SSTable 块元数据在前(近零成本),memtable 走查在后 —— 反过来会让大表每次扫描白走 8192 次 skip-map 跳(初版犯过,提交前已正)。**门是纯启发式**:切分边界仍由真实键计算,估偏大=照旧探测,估偏小=该查询少并行,两向都不碰正确性 —— 这也是它不需要 verifier 级验证的原因。块属性未载入按 cap 处理(照旧探测),不丢并行。

**验证**:小表连续查询 `fusion_reverse_scan_count` 归零(修复前每查询 1);大表保留(=1)。1217 全绿。**诚实披露**:pgwire 墙钟收益在单轮噪声内不可分辨,收益证据以计数器为准 —— 该探测 I/O 全部命中块缓存时本就只占墙钟的小头,SSTable 多/缓存冷时才是大头。

**下一线索(比本项更大)**:小查询仍有 ~1.5ms **服务端内**固定耗时(`query_total_us=1511` 对 500 行 SUM),而其存储计数器近零(21 次缓存命中,无 miss,现无反向扫描)——即耗时在执行层 CPU/分配,不在存储。更极端的先例:`SELECT * FROM bench LIMIT 82` 曾测得 21.5ms 且存储计数器全零。**候查方向**:聚合路径的逐行 Value 物化/装箱、HTTP/pg 结果序列化计入 query_total_us 的范围、schema/统计加载、tokio 调度。工具:在 execute_in_transaction_with_params 关键段加 FDB_TRACE 分段计时,或 perf record 采样对比同查询在 6/29 二进制的火焰图。


<spec-entry category="debug" keywords="bounded-scan,serial,parallel,limit,phase-trace" date="2026-07-21" title="有界扫描误入并行机器修复(445f7fa):回归因果链补全" description="limit=Some 时并行分区全表扫全部白跑;串行早停恰好只读所需;ORDER BY PK LIMIT 回归=两刀之和" source="main@445f7fa">

### 有界扫描误入并行机器修复(445f7fa):回归因果链补全

分段计时(FDB_PHASE_TRACE 临时插桩)定位:`ORDER BY id LIMIT 60` 的 scan 占 exec 95%(6-7.8ms)却只读 9 块 —— 耗时在 CPU 不在 I/O。根因:`64afb98` 让限量扫描经 `scan_prefix_parallel_for_each_with_options` 走流式,但 **driver 层 limit=None(靠 visitor 自停)⇒ 并行门照过、first/last 探测照做(last=反向扫描)、8 分区全表扫描照生成**,消费端取够行数后兄弟分区已白跑(只读评审当时的"分区无背压"警告在此兑现)。修复:`scan_routed_data_prefixes_for_each_with_options` 在 `limit.is_some()` 时直接走串行 for-each。**规则:有界扫描的正解是串行早停(只读所需);并行分区只配无限量扫描 —— visitor 自停不能替代 driver 级 limit 判定。**实测 scan 6-7.8ms → 0.4-0.9ms(~10x),1217 全绿。

**回归因果链就此补全**:`ORDER BY id LIMIT 50` 基线 0.26ms → 22ms = ①limit 未达存储层(64afb98)+ ②有界扫描误入并行机器(本票)。**诊断方法沉淀**:计数器差值找 I/O 形态异常,分段计时找 CPU 形态异常;"块读少但耗时高"= CPU 侧,直接上 PHASE 插桩比猜快。

**剩余待查**:裸聚合(SUM/COUNT over 小表)不经该 scan 位点仍有 ~1.6ms 执行层耗时(无 scan= 输出证实走了别的分支)——下一轮先找聚合分支的实际路径再插桩;并行分区"无背压先跑满"问题对无限量扫描仍存在,属独立优化票。


<spec-entry category="debug" keywords="aggregate,sstable,read-path,per-entry,memtable,floor" date="2026-07-21" title="裸聚合剩余耗时定性:SSTable 读路径逐条目成本,非版本垃圾非文件数" description="三判定:memtable 驻留 150-250µs vs SSTable 驻留 1.2-4ms;仅 2 文件;VACUUM 无效 —— 读路径地板,接 472 族批量解码" source="main@d8b4d2e">

### 裸聚合剩余耗时定性:SSTable 读路径逐条目成本,非版本垃圾非文件数

三个判定实验(FDB_PHASE_TRACE 插桩 agg_scan,已移除):①同一二进制 + 全新目录(500 行仅在 memtable):**150-250µs**;②基准目录(数据落 SSTable):**1.2-4ms**;③磁盘仅 2 个 SSTable(72MB)排除文件数,**VACUUM 后仍 2-4ms 排除 MVCC 版本垃圾**。⇒ 剩余耗时是 **SSTable 前缀扫描的逐条目读路径成本**(块内逐条目 offset 迭代 + 内部键解码 + 前缀比较,~2-4µs/行),属引擎地板,与 6/29 差距的读侧残余同源。**接架构票 472 族**:批量列式块解码(CRDB 同构证明 70x micro)是正解;短线可查块内条目迭代的逐条开销(每条目 CRC/解码重复工作)。

**环境教训(两次踩坑)**:server 从仓库根启动会打开仓库根 ./data(698MB 历史数据集,spec debug-notes-001 早有警告);cd 失败后 `cat server_port.txt` 读到残留文件连错服务端(sum 值对不上才发现)。**规则:每次测量前必须校验连接目标 —— 用只有该目录才有的数据特征(如行数/求和值)做身份断言,别只看端口通不通。**


<spec-entry category="debug" keywords="reverse,desc,limit,early-stop,cpu,pre-existing" date="2026-07-21" title="新立案:DESC LIMIT 反向路径 ~100-160ms 纯 CPU(既有,非零拷贝回归)" description="同目录新旧二进制对照:pre-slice 同样 104-160ms;零 I/O 全 CPU;疑反向候选机器缺 LIMIT 早停" source="main@ef8af5d">

### 新立案:DESC LIMIT 反向路径 ~100-160ms 纯 CPU(既有,非零拷贝回归)

三刀集成后身份断言基准发现 `ORDER BY id DESC LIMIT n`(5 万行表)中位 ~115-155ms,计数器:`reverse_scan=1`、`block_miss=0`、`read_bytes=0`、`sort_fallback=0` —— **零 I/O 纯 CPU 在反向归并内**。同目录跑改动前二进制(ca88745):104-160ms 相同 ⇒ **既有病灶,与零拷贝四/五刀无关**(判定实验先于修复,遵守"先归因再动手")。

疑点:反向候选机器(merge_visible_range_reverse)对 LIMIT 查询似乎在整段范围上做逐键候选解析而非取够 n 键即停 —— 对照正向路径的教训(445f7fa:有界扫描早停),反向可能同样需要"visitor 取够即整体停"的贯通;也可能是 DESC 计划根本没把 limit 传给反向扫描(对照 64afb98 的 limit 丢失模式)。**下一步**:对 DESC LIMIT 查询插桩反向归并的 per-key 候选计数,确认是"全范围候选"还是"limit 未下传";两个先例模式(限量下传/早停贯通)都有现成修法。


<spec-entry category="debug" keywords="zero-copy,472,integration,worktree,slices" date="2026-07-21" title="零拷贝三刀并行实现+串行集成闭环(b36c344/7bae904/ef8af5d)" description="worktree 三片并发实现,按风险串行集成各过全量门;合并评审 0 confirmed;DESC 病灶另案" source="main@ef8af5d">

### 零拷贝三刀并行实现+串行集成闭环(b36c344/7bae904/ef8af5d)

三片在隔离 worktree 并发实现(compaction 视图直通 builder / 反向迭代器+归并视图化 / 并行分区 channel 传视图),同基 ca88745,各自 lib 全绿后按风险从低到高串行 cherry-pick 集成,每片过独立全量门(1217/1216/1216,-1 为随 VecDeque 装填删除的预分配测试)。合并 diff 只读评审(compaction 内存钉扎与出错路径/反向逐位等价与统计精确/并行 channel 协议)3 findings 全驳回。**流程要点**:worktree 基点可能陈旧(本次三个 worktree 均基于过期 032e052,agent 各自 ff 到 main tip 才动工)——启动 worktree 批实现前应显式给定基提交;集成用 cherry-pick -n + 自写提交信息,保留战役风格。**至此 SSTable 读/写路径的逐条目物化全部消除**(前向串行/并行/反向/compaction 四路),剩余拷贝仅在必要边界(visitor 保留数据自拷/builder 落盘缓冲/堆键小拷贝换比较器不变)。基准:SUM/COUNT/全扫不劣;DESC LIMIT ~115-155ms 经同目录旧二进制对照证实为既有病灶,已另案(77060dd)。


<spec-entry category="debug" keywords="backpressure,parallel,lookahead,negative-result,staged-spawn" date="2026-07-21" title="已验证的错误修法:并行分区滑动窗口 spawn(lookahead=2)全扫慢 2x,已回退" description="背压问题在有界扫描串行化后已基本消解;砍并发生产者=砍真实并行度;正确设计需批量有界 channel 或异步 visitor" source="main@313a2db">

### 已验证的错误修法:并行分区滑动窗口 spawn(lookahead=2)全扫慢 2x,已回退

尝试给并行分区扫描加背压:消费端按分区序消费,把"一次性 spawn 全部分区"改为滑动窗口(lookahead=2)。**实测全扫中位 52-59ms → 120ms(2x 回归)**——并发生产者 8→3,归并/解码的真实并行度被砍。已回退,勿重试同型方案。

**重新定性(为何背压优先级应降)**:reviewer 当初担心的"早停后兄弟分区超跑"在 445f7fa(有界扫描全走串行)与 313a2db(PK DESC 反向早停)后已基本不可达——并行路径只剩全量消费场景,队列深度被表大小天然封顶,与旧 owned-copy 行为同量级(视图钉块字节 ≈ 表块字节)。剩余风险仅"消费端 visitor 极慢"的病态场景。

**若未来真要做**:正解是(a)生产端按批(如 256 条/批)发送 + 有界批 channel(减少 send 次数使 blocking 语义可行),或(b)把 merge 的 visitor 契约改异步以支持有界 send.await —— 两者都别用"砍并发生产者"凑数。

