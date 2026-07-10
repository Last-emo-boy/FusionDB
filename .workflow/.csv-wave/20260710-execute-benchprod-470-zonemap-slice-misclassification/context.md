# BENCHPROD-470: zone-map 切片误分类——(K-1)/K 的评估是假 fail-open

## 解剖链(含四次诚实撤销的假设,均记 maestro)

xlarge 全扫 warm 每查 11.1 万次 zone-map 检查、87.5% fail-open。排除:
memtable 重叠(checkpoint 不变)→ 构建侧(12 个 kill 点静默;盘上普查
sql_zone_maps 与 table_prefix_ranges 双双 100% complete;裸字节验证 framed V6)
→ 解码链(V6 From 完整)→ 区间缺失分支(打点静默)。

**真凶**:`sql_zone_map_skip_offsets_for_sstable` 对每次(子)扫描遍历 SSTable
全部块;并行全扫把键空间切 K=8 片,每片把其他 7 片的块(恰 7/8=87.5%!)
在范围边界检查处误记为 IncompleteMetadata——每块每片白付一次区间 Vec 克隆
+ 计数。26,680 = 8 × 3,335 数据块,闭式吻合。

## 修复

完全在 [start,end) 外的块在计数**之前**静默 continue(无关,非元数据失败);
仅真正跨边界的部分重叠块保留 fail-open(每片 ~2 块)。

## 验证(本地 12 万行)

检查 26,680→3,342;fail-open 23,352→**14**;跳过/命中决策逐一相同
(3,110/218,剪枝正确性保持);块读 ~16k→232;结果行数精确。
zone map 本身工作良好(val=X 上 93% 跳过率)——被误分类噪声淹没了。

## 后续

xlarge parts 1 复跑量化 Full scan/BETWEEN/IN 家族恢复;no-fill 重读成本
是下一个地板成分。
