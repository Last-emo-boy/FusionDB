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