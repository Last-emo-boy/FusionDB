# BENCHPROD-463: Row cache 字节一致性验证(P0 正确性)

## 问题

执行层 row cache(`Cache<String, Vec<Value>>`)完全没有版本或校验信息,存在两类真实错误:

1. **永久陈旧竞态**:写事务在 staging 阶段 per-key invalidate,并发读者在 invalidate 之后、
   commit 之前把**旧值**重新写回缓存;正常路径上不存在任何 commit 后失效
   (`invalidate_all` 只在 Raft 快照安装时调用)→ 陈旧行被**无限期**提供给之后的所有读者,
   包括普通 autocommit 查询。
2. **快照违规**:pgwire 显式事务固定在旧 read_ts,却可能命中其他会话缓存的**更新版本**。

此外 DML(UPDATE 基底行、UPSERT existing 行、UNIQUE 预检、DELETE 索引清理)也经由缓存读旧行,
陈旧命中会造成错误的索引维护与约束判断。

## 设计:字节一致性(淘汰掉失效协议本身)

`CachedRow { encoded: Arc<[u8]>, row: Vec<Value> }`,命中条件 = 调用方**本次从存储解析出的
字节**与缓存字节 memcmp 相等(`row_cache_lookup`/`row_cache_store`)。相同字节 ⟹ 相同解码行,
错误结果在结构上不可能;因此**删除了全部 ~14 处 per-key invalidate**——正确性不再依赖任何
失效时序。快照正确性自动成立:比较对象就是本事务快照解析出的字节。

否决的替代方案:

- **表级 epoch(commit 后 bump)**:publish→bump 窗口内新开始的读者可接受 pre-commit 条目,
  不可关闭;双 bump 变体同样留洞。
- **存储层版本化 get**:仍需 epoch/失效来发现"更新版本已存在",trait 改动无健全性收益。

代价(权衡点):原"纯缓存命中零存储访问"的点查路径(PK 点查、HNSW 取行、有序 Top-K 基行、
join PK 探测)改为先 `txn.get` 再验证,缓存只省**解码**;`key_only_scan` 分支前移保持零存储访问。

## 验证

- fmt / clippy correctness / lib 559 全绿;`--all-targets` 31 个测试二进制全绿。
- 新增 2 个 lib 回归测试:投毒条目被忽略;FusionStorage 旧快照事务不读缓存新版本(旧代码下两者都失败)。
- **10 个集成测试原本把旧的不健全行为当契约测**(带外破坏存储字节、断言缓存掩盖),
  全部改写为新契约(带外合法改写必须战胜缓存;UNIQUE 双向判存储真值)并改名。
- 对抗性 review workflow(4 维 × find→verify,26 agents):判定 MVCC-sound、失效删除全覆盖;
  抓到当时未迁移的 6 个测试与 row_read 口径不一致(均已修);否决 5 个误报。
- 基准 A/B(medium parts=1 pgwire,**同盘 ext4**;首轮作废——基线误跑在 tmpfs 上,
  fsync 免费使单行 DML 虚假回归 5-7×):扫描/过滤/聚合 ±9% 噪声带内;
  PK 点查 0.10→0.16ms(绝对 +60µs,base 本身跨环境 0.10-0.19 波动);
  单行 DML fsync 方差主导、方向混杂(UPDATE -17%,INSERT +32%),无一致回归。

## 披露(保守声明)

- 点查命中不再跳过存储探测,只跳过解码;PK 最坏 +60µs@pgwire,µs 级真值需专用微基准。
- HNSW 取行路径无任何基准覆盖,该处回归今日不可度量。
- CachedRow 使单条目内存约 ×2(moka 按条目数计容)。
- `row_reads_per_query` 跨此边界不可比(命中现在也计 row_read)。

## 后续

- BENCHPROD-464:trigram/HNSW 旁路写不在事务内 + 复合 UNIQUE 幻读洞。
- 可选:128-bit 哈希替代完整字节以减半条目内存(需评估碰撞面)。
