# BENCHPROD-469: zone-map skip 预计算击穿 LIMIT 早停(PK 区间扫描)

## 症状与解剖链

xlarge 报告 `Range id>N LIMIT 100` 12-29 秒。解剖:①单查 metrics 增量证明
limit 已达存储、visitor 恰在 100 次访问即停,但 2.7s 花在访问之外 ⟹ O(区间)
的 setup;②打点定位;③`merge_visible_range` 为每个 SSTable 调
`sql_zone_map_skip_offsets_for_sstable`,对区间内**每个块**预评估 zone map,
且每块做一次遍历全部 memtable 的 MVCC fail-open 校验——LIMIT 100 先付
25 万行区间的全款。

途中修正两个错误假设(诚实记录):"索引跨重启丢失"系查错表(基准的
bench/bench_idx 双表设计);"limit 未下推"系误判(守卫链逐层验证完好)。

## 修复

PK 区间分支仅在**无 limit** 时挂 zone-map 剪枝计划(scan/mod.rs)。剪枝的
收益模型只对大扫描成立;早停扫描本来只读少数块。无限扫描路径不变
(zone-map 矩阵基准覆盖)。

## 验证

本地 50 万行:2715ms → cold 19.8ms / warm 2.2ms(**~1200×**);
正确性:区间 COUNT=249,999、LIMIT 恰返 100 行;门禁全绿。

## 后续

同类"预计算 vs 早停"存在于全扫 zone-map 挂载点(444 visitor 自停时)——
先测再动;`AND 1=1` 变体修复前仍有 ~570ms 的次级 O(区间) 成本待复测;
xlarge parts 1/4 复跑确认 Range/Analytics 家族恢复。
