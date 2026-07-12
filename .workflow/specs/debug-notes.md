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
