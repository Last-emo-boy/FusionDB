# BENCHPROD-468: SSTable bloom 按真实条目数定容(xlarge 装载塌方)

用户要求跑 xlarge 全量基准 → 装载爬行(~330 行/s vs 基线 20,403 行/s),
批耗时随总量线性增长。单批 metrics 增量给出铁证:
`user_key_filter_positive == check == 3003`——**不存在的键 100% 假阳性**,
每行 PK 查重在每个 SSTable 都退化为真实块读(O(SSTable 数)/get ⟹ O(n)/批)。

根因:所有 SsTableBuilder bloom(whole-key/prefix/user-key v3/sql-index-prefix v4)
硬编码 `expected_items(100_000)`,而 32MB memtable 一次 flush 携带 30-50 万内部键,
3-5 倍超载使过滤器饱和(单测实测 3 倍超载 FP 37%,生产 ~100%)。
7 月 9-10 filter 工作(检查点)以来的潜伏缺陷;中小规模装不满 memtable 从未暴露。
**与 463-466d 无关**(HEAD 隔离重现 + metrics 机制定位,二分停用)。

修复:`set_expected_filter_items(n)` 重建/定容四个过滤器;flush 传
`mem.map.len()`,compaction 传输入 SSTable 的 `estimated_entry_count()` 之和。

验证:微重现 0→50 万行,HEAD 38→682ms 线性增长,修复后全程 ~60ms 平坦
(50 万行处 11×);FP 3003→13(0.43%),块读 6007→37。单测锁定契约。
披露:磁盘上既有的饱和过滤器随 compaction 重写自愈,无需强制重建。
