# P10-2.3: Data V2 backfill 引擎(可续跑分块 + DDL 冲突点 + 解锁 advance→Backfill)

阶梯设计见 `.csv-wave/20260720-design-p10-2-data-v2-migration-ladder/design.md`(9 票,本票为 2.3)。
前置 2.2(清理有界化)已于 `1a7cf5c` 落地。

## 实现

**状态记录**(`data-v2-backfill-state`,Catalog 中 phase record 的兄弟键):40 字节定长头
(version / complete / shard_count 存在位 + 值 / chunks_done / rows_done / updated_at / cursor 位 + 长度)
+ 有界游标尾;严格 decode(尾字节、标志位与值一致性、游标上界全校验)。
**shard_count 用存在位而非 u64::MAX 哨兵**——任何 u64 都是合法分片数,哨兵会吞掉一个真值。

**分块步进**:一个事务 = 若干行的 v2 影子写 + 游标更新,进度与数据永远同生共死。
上限同时按行数(256)与字节数(1 MiB)封顶——每个写入键在全局 commit_lock 内要做一次
`latest_committed_timestamp` 探测,chunk 越大越拖慢全库提交。

**枚举**:直接走物理键序两段区间 `["data:","data;")` 与 `["shard:","shard;")`,
**不经 ShardRouter**(已核实 router 只枚举当前 shard_count,且启用后完全不含 unsharded 前缀,
历史分片与 router 前的行会被静默跳过)。区间内非基行键(`shard:N:index:` 等)跳过。

**表身份识别**:先一次性加载 `schema:` 目录,再用已知表名集合切分 `{table}:{row_id}`;
多候选(如 `b_ident` 与 `b_ident:archive`)时用该行自己的主键消歧;无候选=孤儿行,跳过不复制。
**注意**:主键不一定是第 0 列(`get_primary_key_index` 按 `is_primary` 搜索),
最初按第 0 列硬解的写法在评审前自查中已改掉。

**DDL 冲突点**:phase ≥ Backfill 时 DROP/TRUNCATE 也**无条件**重写状态记录,与在飞 chunk
强制写写冲突。无条件是关键:首个 chunk 才是创建该记录的人,条件式重写恰好漏掉它。

**操作面**:`CALL fusiondb_data_backfill_step()` / `fusiondb_data_backfill_status()`,
沿用 2.1 的全套管道(superuser 门按解析语句、standalone 规则、raft 路由分类);
status 为只读,不进 raft 写路径。

**相位闸门**:`MAX_SUPPORTED_PHASE` 与 `MAX_ADVANCE_TARGET_PHASE` **同时**升到 Backfill
(只升后者会直接砸库:观察点拒所有数据写、with_config 拒开、apply 停机)。

## 有意决定:backfill 本票单机限定

分布式路径上 `RecordingTransaction` 为**每个写入键**记录一条 precondition;一个 chunk 数百个
影子键 ⇒ 数百条 precondition。leader 求值到 apply 之间只要有一个并发 DML 碰到其中任一键,
就是 precondition 失配,而失配语义是 **fail-closed 停机**(store.rs 测试钉死)。
即"一次例行 DML 竞态 → 副本停机"。故 chunk 步进在 raft 路径 fail closed
(加入 `statement_is_unsafe_to_stage`),与 pgwire 写在分布式模式 fail closed 同一先例。
分布式驱动需要一种不放大 precondition 的提案形态,另立票。

## 只读对抗评审(5 维 / 28 agent / 168 万 token):1 blocker + 1 major 确认

**blocker(游标毒化)**:游标取自任意访问过的键,而 `shard:N:index:` / `unique:` 等键
包含未设上限的列值,可超出 decode 的游标上界;`encode` 当时**无**校验 ⇒ 写出一条自己
读不回来的记录。后果不止 backfill 卡死:`touch_backfill_state_for_ddl` 也要读它,
于是**全库每次 DROP/TRUNCATE 永久失败**,且该 Catalog 键无任何 SQL 途径可修复。
评审还指出区间内 `data` < `fts` < `index` < `unique`,扫尽时的末键**通常**就是 unique 哨兵,
即毒化是常规终态而非奇葩路径。修复:游标上界提到 4 MiB(高于引擎可存的任何键)且
**encode 侧同样校验**——超界即写时 loud 报错,绝不产出不可读记录。

**major(CTE 误伤)**:`is_data_family_key` 匹配 `data:{cte}:{row}`,而 CTE 材料化不打 fence,
于是集群到 Backfill 后,一条普通 `INSERT ... WITH ...`(外层选出 0 行时批次里只有 CTE 写)
会被新守卫拒绝,还附赠一句误导性的"提议节点需升级"。评审进一步指出:该守卫的假想敌
(旧二进制)**根本不可达**——旧二进制在 advance 到 Backfill 那条 entry 上就先停机了,
所以假阳性可达而真阳性不可达。修复取根因:**CTE 写入也走 fence**
(凡写 `data:` 命名空间者都应遵守 P10-2.1 不变量),守卫随之恢复健全。

评审同时列出大量测试缺口,已按条补齐(见验证)。

## 验证

- `cargo test --locked --all-targets`:**1216 通过**(2.2 后 1200,本票 +16),fmt 干净。
- 引擎行为:跨 chunk 续跑、重开后从游标续跑(非重跑)、影子值与 legacy **逐字节相等**、
  完成后再步进为幂等 no-op。
- 收敛性:chunk 与并发 UPDATE 撞同一 v2 键 ⇒ 写写冲突 loud abort,重试后影子等于最新值。
- DDL 竞态:DROP 先提交 ⇒ 在飞 chunk loud abort,被删表零残留影子行。
- 身份识别:主键在第二列 + 行 id 含冒号 + 引号表名 + **合成的真冒号表名**(走多候选消歧)
  + 孤儿行跳过。
- 路由覆盖:unsharded 与历史 `shard:7:` 行都被复制,同区间的 `shard:7:index:` 键跳过。
- 闸门:相位不足拒步进、拓扑变更拒续跑、参数/standalone/superuser 四类前缀写法全拒。
- 状态编解码单测:全字段往返(含 `Some(u64::MAX)` 分片数)、四类标志位篡改、尾字节、
  标志与值不一致、**超界游标写时即拒且"可编码必可解码"**。
- 分布式:raft 路径拒 backfill step、未 fence 的三种物理形态数据写在 Backfill 相被
  优雅确定性拒绝(节点继续服务)、已 fence 的写正常 apply、**CTE 批次携带 phase
  precondition 且在 Backfill 相正常 apply**。

**一处未结观察(如实记录)**:曾有一次 `cargo test --lib` 报 672 passed / 2 failed,
但当时输出未捕获测试名;此后 7 次运行(3 次单跑 + 1 次与 --all-targets 并发压测 + 3 次
捕获输出)全部 674 全绿,无法复现。本仓库有记录在案的高负载 flaky 家族
(fusion.rs 的 GLOBAL_METRICS 精确差值断言,`d44ccc2` 起标记为"再现即按 sstable 同法迁移"),
疑为同族,但**未经证实**。下次若再现须第一时间捕获测试名。

## 基准

**本票不作基准主张**。backfill 是操作员显式驱动的离线路径,不在任何 benchmark part 上;
写路径新增开销仅 CTE 材料化多一次 fence 观察(命中缓存的同步快路径)。
本 harness 噪声底噪(逐查询 p90 |delta| 36.2%,见 P10-2.1)远大于该量级。

## 后续

- **P10-2.4(下一票)**:exact verifier + verify-token,解锁 advance→Validated。
- 分布式 backfill 驱动:需要不放大 precondition 的提案形态(独立票)。
- 已知局限:单个超过 4 MiB 的 legacy 键(仅可能来自超大索引列值)会让 chunk 在该键上
  loud 卡住;安全但需人工介入,记录备查。
- **撤回先前记载**:2.1/2.2 工单曾写"backfill 必须排除 CTE 键"——CTE 行在同一事务内
  put 后即 clear(`handle_query` 尾部无条件清理),MVCC 只在提交时发布,故并发事务
  永远看不到活的 CTE 行,枚举层面无需排除。但本票另因 raft 守卫给 CTE 写补了 fence。
