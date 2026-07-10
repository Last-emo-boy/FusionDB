# BENCHPROD-464: UNIQUE sentinel key 进 OCC 写集(并发同值双提交修复)

## 问题(自行核实后的准确范围)

单列 UNIQUE(非 PK)有四个洞:

1. **并发幻读**:重复检查是全表扫描,看不见对方未提交的行;索引键带 row_id 后缀,
   exact-key OCC 永不碰撞 → 两个并发同值 INSERT 都能提交。
2. **UPDATE 完全不校验 UNIQUE**(顺序执行即可改出重复)。
3. **UPSERT DO UPDATE 不校验赋值后的行**。
4. **INSERT..SELECT 完全没有单列 UNIQUE 校验**(连顺序重复都能插入)。

范围修正(纠正初始 review 的判断):复合 PK 与单列 PK **本来就安全**——
`row_id_for_insert` 使并发同值写相同 data key,OCC 必然碰撞;非 PK 复合 UNIQUE
索引今天根本未被校验(独立功能缺口,另行开票)。

## 设计(调研定型,CRDB/TiDB unique-key 形态)

**不含 row_id 的 sentinel 键**(`unique:<table>:<col>:<value>`,分片变体按**值**哈希路由,
保证同值必然同键)在每次写 UNIQUE 值时 `txn.put` 进 OCC 写集——并发同值写者在
commit 校验时确定性碰撞,后提交者 abort。**分工**:sentinel 只堵并发窗口;
扫描检查继续负责已提交重复(历史行无 sentinel → 零迁移零回填)。

值编码:`value_to_index_string`,不支持的类型(FLOAT/BLOB/VECTOR...)fallback 到
bincode 上的稳定 FNV-1a-64——哈希碰撞只可能造成保守误杀,不可能漏杀;
`-0.0` 规范化为 `+0.0`(f64 相等但 bits 不同)。

生命周期:INSERT×3 站点 + UPSERT DO UPDATE×2 staging/迁移;UPDATE 主循环 + PK
快路径校验+迁移;DELETE 主循环墓碑(快路径门禁扩展排除 UNIQUE 列);
TRUNCATE/DROP TABLE 前缀清扫。

## 对抗性 review(4 维 × find→verify,21 agents,0 误报)战果

- **[major] FLOAT 等类型拿不到 sentinel**(实测 DOUBLE UNIQUE 双提交)→ 哈希 fallback 修复 + 竞态测试。
- **[major] INSERT..SELECT 无校验** → 复用共享验证器(`old_row=&[]`)修复 + 测试。
- **[major] O(N×列) 列循环内扫描**(实测 2000 行 bulk UPDATE 490×)→ 提为每行单次扫描。
- **[major] migrate 路径无测试锁定** → 新增 UPDATE-vs-INSERT 竞态测试。
- 确认为良性:ALTER 孤儿 sentinel(值从不被读、旧提交不会 abort 新事务)、
  列名含 ':' 的键歧义(仅保守误杀,与现有索引键同构)。

## 验证

fmt / clippy correctness / lib 563(新增 4 个 fusion 后端竞态测试)/
sql_view_show_constraints 20(UPDATE/UPSERT/INSERT..SELECT/批内重复全部拒绝)/
--all-targets 全量。基准:注册工作负载的表均无非 PK UNIQUE 列,sentinel 成本为零;
medium part1 pg 抽查确认无回归。

## 披露

- 真多节点跨机唯一性仍是 best-effort(与修复前一致);MemoryStorage 无 OCC(仅测试后端)。
- 变更 UNIQUE 值的 UPDATE 每行一次全表扫描(与 INSERT 侧既有形态相同)。
- NaN 永不冲突(NaN≠NaN,与扫描检查语义一致)。

## 后续

BENCHPROD-465(side-index 事务化)、非 PK 复合 UNIQUE 校验缺口、
O(1) sentinel 读检查替代 O(N) 扫描(需回填故事)。
