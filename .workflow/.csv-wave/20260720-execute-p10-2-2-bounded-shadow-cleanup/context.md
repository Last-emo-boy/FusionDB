# P10-2.2: DROP/TRUNCATE 的 Data V2 影子清理有界化(route skip-scan)

阶梯设计见 `.csv-wave/20260720-design-p10-2-data-v2-migration-ladder/design.md`(9 票,本票为 2.2)。
前置于 P10-2.3 backfill:backfill 一旦灌满 v2 namespace,原实现会让每次 DROP/TRUNCATE 退化为 O(全库)。

## 问题

`delete_structured_data_shadows_for_table` 原为
`txn.scan_prefix(data_namespace_prefix(), None)` —— **物化整个 Data V2 namespace 的
全部 (key, value)**,逐键 parse 后删表名匹配者。当前 namespace 近乎为空(影子写默认关)
所以无感,但这是 backfill 的前置阻塞项:2.3 落地后每次 DROP/TRUNCATE 都要扫全部影子行,
且把全部值一并读进内存。

## 实现

改为 route 区跳扫。`data_namespace_prefix()`..`prefix_end(...)` 内循环:

1. `scan_range(cursor, namespace_end, Some(1))` 取游标处第一个键(**一次 seek,一行**;
   FusionStorage 的 visitor 在第 1 行返回 false 早停,已核实);
2. `parse_data_key_exact(probe).route()` 得知当前所处 route 区;
3. 对 `[encode_data_prefix(route, table), prefix_end(...))` 做流式扫描,只收键(不收值);
4. `cursor = prefix_end(encode_data_route_prefix(route))` —— 跳过整个 route 区,
   该区其余表与本次清理无关;
5. 走完后统一 `txn.delete`(单事务原子性不变)。

keyspace 配套:`prefix_end` 与 `ParsedDataKey::route()` 从 `#[cfg(test)]` 转生产,
新增 `encode_data_route_prefix`(header + route tag [+ shard id],表名无关)。

**为何前缀安全**:表名以 4 字节 BE 长度前缀编码,故 `orders`(14B 头)与
`orders:archive`/`orders_2`(不同长度头)的前缀区间天然不嵌套——冒号邻表不可能被误删。

**终止性**:探针键 ≥ cursor 且以 route 前缀开头,故
`prefix_end(route_prefix) > probe ≥ cursor`,游标严格递增;`Shard(u64::MAX)` 时
进位落到 route tag(0x01→0x02)仍在 namespace 内,下一轮扫空即 break(已测)。

## 与旧实现的语义差

**malformed-key loud 检测范围收窄**(设计稿明列的有意决定):原来 namespace 内任何
畸形键都会让**任意表**的 DROP/TRUNCATE 报错;现在只对本次触碰的范围(每 route 的探针键 +
目标表自己的区间)loud。这正是"不再扫全库"的代价,已在设计稿裁定。

## 验证

- `cargo test --locked --all-targets`:**1200 通过**(2.1 后为 1197,本票 +3),fmt 干净。
- **既有 4 个 shadow 测试零修改通过**,含 Shard(99) 历史孤儿与 `shadow_cleanup:archive` 冒号邻表。
- **有界性实测(本票核心主张)**:新增 `CountingTransaction` 装饰器直接计数扫描交还的键。
  namespace 2002 行(2 route × (1 目标行 + 40 表 × 25 行))时,清理只触碰 **4 个键**
  (2 次 route 探针 + 2 个目标行),断言 ≤8;旧实现会全扫 2002 行 ⇒ 该断言确有判别力。
  同时断言 2000 行噪声全部存活、目标行两 route 皆删净。
- 多 route 发现:Unsharded / Shard(0) / Shard(7) / Shard(u64::MAX) 四路由 × 两前缀相邻表。
- **FusionStorage 真引擎路径**:committed 三 route + 只存在于**本事务写缓冲**的 Shard(42) route
  + 已提交 route 中的 staged 行,全部被发现并删除(证明 route 发现经 MVCC merge 能看见未提交 route)。

## 基准

**本票不作基准主张**。理由:`delete_structured_data_shadows_for_table` 的调用点只有
DROP TABLE 与 TRUNCATE(已逐一核实),不在任何 benchmark part 的热路径上;而本 harness 的
噪声底噪(逐查询 p90 |delta| 36.2%,见 P10-2.1 记录)远大于任何可能的信号,跑标准 parts
只会产出噪声。有界性由 in-tree 计数测试证明(2002 行 → 4 键),比基准更直接且可回归。

## 只读对抗评审(17 findings,0 confirmed)

本次评审严格只读:agentType=`Explore` + 提示词显式禁止一切写工具与 git 变更命令,
且评审前先 `git stash create` 快照(`715e480`)——P10-2.1 的评审 agent 误删事故未重演,
评审结束后 `git status` 确认工作区完好。

17 条全部被独立验证者驳回。其中四条虽非缺陷,但按本项目 doctrine 采纳为质量改进:

1. **测试替身静默丢弃 fence**(3 个 agent 独立提出):`CountingTransaction` 未覆写
   `fence_data_migration_phase`/`data_migration_phase_pin`/`as_any`,走 trait 默认空实现。
   当前不影响该测试结论,但一旦后续票让 fence 在此路径承重,测试会静默不再覆盖 ⇒ 改为委托 inner。
2. **`prefix_end` 返回 None 时静默跳过该 route 的行**:结构化键以 `\0` magic 开头,
   表前缀不可能全 0xff,该分支不可达;但"不可达即静默跳过"违背 loud-over-silent ⇒ 改为 loud 报错。
3. **文档overclaim**:原写"streaming scan"却把键收进 Vec ⇒ 改为如实描述(收键不收值,
   删除在走完后统一执行),并显式注明 FusionStorage 每次扫描会合并写缓冲,
   故 DROP 若已 staged 大量删除,该合并按 route 数付费——不再宣称干净的 O(routes + rows)。
4. **测试只覆盖 MemoryStorage** + **未覆盖只存在于写缓冲的 route** ⇒ 补 FusionStorage 真引擎测试,
   含写缓冲独有 route 与已提交 route 中的 staged 行。

## 后续

- P10-2.3(下一票):backfill 引擎(幂等 chunk + durable checkpoint + DDL 冲突点 + 解锁 advance→Backfill)。
  本票是其前置,现已解除。
- 2.3 范围提醒(承 2.1):CTE 材料化直写 `data:{cte}:{row}` 键(既有行为),
  backfill 枚举 legacy `data:` 前缀时须排除,否则把临时表灌进 v2。
- 2.3 需注意:DROP/TRUNCATE 的 v2 清理只删其快照已存在的键,与并发 chunk 的新 put 零键重叠,
  必须靠 backfill-state record 制造 write-write 冲突点(设计稿已裁定)。
