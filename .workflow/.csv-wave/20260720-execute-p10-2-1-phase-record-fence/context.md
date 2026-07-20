# P10-2.1: 持久 Data V2 迁移 phase record + 全 data-writer commit fence

阶梯设计见 `.csv-wave/20260720-design-p10-2-data-v2-migration-ladder/design.md`(9 票,本票为 T1)。

## 问题

Data V2 影子写此前由进程本地配置 `structured_data_shadow_v2` 单独决定:存储状态无法
自证影子完整性,集群各节点可按各自配置发散,迁移无任何 fence/相位/回滚语义。
P10-2 后续所有票(backfill/verifier/切读/GC)都必须站在一个持久、单调、被全体
writer 强制的相位之上。

## 实现

**Record**(`src/storage/data_migration.rs`,新):Catalog namespace identifier key
`data-v2-migration-phase`;值为 18 字节定长 v1 `[version][phase][phase_seq:u64BE][updated_at_ms:u64BE]`,
严格 decode(长度/版本/相位序数/seq>0 任一不符即 loud)。相位阶梯 delete-only(1) →
write-delete-shadow(2) → backfill → validated → v2-readable → v2-only → legacy-gc(7);
序数 0(legacy)保留不物化(flag=off 也无条件写 v2 墓碑,现实状态恒 ≥ delete-only)。
双常量 `MAX_SUPPORTED_PHASE` / `MAX_ADVANCE_TARGET_PHASE` 本票均 = write-delete-shadow。

**Fence 三层**:
1. `DataMigrationFence` 挂 FusionStorage,启动时(WAL 重放后、side-index 重建前)读 record 灌好;
   `invalidate_storage_caches` 追加一行 invalidate,一次覆盖 raft apply 与 snapshot install 两个既有调用点。
2. 单机:三个 data-writer helper 打 pin,`FusionTransaction::commit` 在 `commit_lock` 内等值校验,
   advance 的 fence 发布在同一临界区(与 current_ts 同点)⇒ 序于 advance 之后提交的旧 pin 事务必 loud abort。
   pg 交互事务在 COMMIT 时提交真实 FusionTransaction,天然覆盖;P5-4 未收口的绕 raft 本地写路径同样被兜住。
3. 分布式:`RecordingTransaction` 把 fence 转成 phase 键 precondition(expected 取 leader 已应用状态,
   不用缓存);apply 侧单调守卫(删除/畸形/首建非 seq1/跳级/降级 → `Ok(Response::error)` 优雅确定性拒绝,
   状态不变、重放同判、节点不停机;唯超 MAX_SUPPORTED → StorageError 停机);
   `normalize_snapshot_payload` 拒装超限相位载荷。

**操作面**:`CALL fusiondb_data_migration_init()` / `advance('<phase>')`(单步、幂等重试、
越级/降级/超 gate 一律 loud)、`SHOW DATA MIGRATION PHASE`(字符串与解析两种形态,
后者供 pgwire);两个 CALL 经 `statement_may_change_query_results` 自动进 raft 写路由与
leader 转发,superuser 门基于解析后语句(非字符串前缀)。

## 对抗评审(19 findings)与修复

评审抓到 1 个 blocker + 6 个真问题,全部修复且各配回归测试:

- **blocker 同事务混排**:`BEGIN; INSERT; CALL advance; COMMIT` 会把"按旧相求值的行"与
  "新相 record"原子共提交(commit 的 pin 校验只看已发布 fence,本事务自己 staged 的 record 尚未发布,
  二者互不感知);standalone 规则只拦单字符串多语句,拦不住会话事务逐条执行。
  修复:`staged_migration_phase_record` 同时存在 staged record 与 fence pin ⇒ loud abort。
- **major superuser 门被绕**:`upper.starts_with("CALL ")` 裸前缀,`/*注释*/ CALL`、`CALL\tfn()` 均绕过,
  `statement_permissions(Call)` 为空 ⇒ 非 superuser 可改持久相位。修复:门移到 `authorize_statement` 按解析语句判定。
- **major resolve_with 无单调性**:陈旧 MVCC 快照可覆盖已发布的新 fence,后续写以旧相行为干净提交。
  修复:低 seq 永不覆盖高 seq。
- **major raft 路径缺 standalone**:`evaluate_sql_to_request` 只拦 VACUUM/COPY TO/HNSW,advance 可与 DML 同批复制。
- **major install fence 窗口**:`replace_visible_entries_for_snapshot` 在 commit_lock 内改写持久 record,
  但 invalidate 延迟到上层 ⇒ 窗口内提交按旧相校验通过。修复:invalidate 移入同一临界区。
- **minor** 空 write_buffer 早退跳过 pin 校验;`SHOW DATA MIGRATION PHASE` 仅 HTTP 可用
  (pgwire 解析成 `ShowVariable` 报不支持);无 record 时的 flag 来源从 Executor 漂到 FusionStorage
  (两者配置不同则行为变化)⇒ 新增 `CachedFenceState` 三态,由 Executor 提供自己的 flag 默认值。

**事故(过程教训,已入 memory)**:评审 subagent 为证明 finding 往仓库写 probe 测试,
清理时 `git checkout src/execution/mod.rs`,抹掉该文件约 600 行未提交改动(其余 6 文件完好),
靠 `cargo check` 报"刚写的函数找不到"发现并重做。**规则:评审 agent 必须只读;
多 agent 评审未提交工作前先 `git stash create` 快照。**

## 验证

- `cargo test --locked --all-targets`:**1197 通过**(基线 1153,新增 44),fmt 干净。
- 既有 4 个 shadow 测试(execution/mod.rs 三个 + fusion.rs CDC 抑制)零修改通过;无 record 行为继承 flag。
- crash 矩阵:WAL AfterWrite 故障注入回滚(record 不落、fence 不前进)、torn tail 重放回到前一相、
  超 MAX_SUPPORTED 拒开、畸形/删除拒提交。
- fencing race:在飞事务×并发 INIT → COMMIT loud abort 后重试成功;同事务混排 → abort 且两侧效果均未落;
  并发双 advance 恰一胜出,败者重试撞幂等分支;superuser 门四种前缀写法全拒。
- 分布式:apply 守卫矩阵(删除/畸形/非首建/跳级/降级 → 优雅拒绝且节点继续服务;超限 → 停机)、
  precondition 失配 fail-closed、快照 build→install 往返携带 record、install 超限拒装、
  幂等重放不双推 seq、DML 批自动携带 phase precondition、raft 求值拒同批 advance+DML。

## 基准(诚实披露)

同盘 ext4、`BENCH_PROTO=pg`、Parts 1/2/3/5、medium、基线取 worktree@7de8634 独立二进制。

**结论:未检出写路径回归。** 逐查询 delta 全部落在本 harness 噪声内,批量装载
(6 万行,写路径最高信噪比指标)候选与基线不可区分。

- 噪声底噪实测(baseline-vs-baseline):逐查询 p90 |delta| **36.2%**,最大 86%;
  批量装载基线在两个测量时段间自漂移 **+14%**(2591ms → 2950ms)。
- **已撤回的假设**:首轮 n=4 曾测得装载 +9.5%(t≈2.7)疑似回归;补测至 n=8 合并基线后
  v1(逐行 fence)+2.4%(t=0.63)、v2(pin 复用)−0.3%(t=−0.05),证明先前信号是基线漂移。
- **内部对照(判噪关键)**:多条**只读扫描**也"可复现地变慢"(Full scan narrow +25%/+23%,
  BETWEEN +22%/+39%)——本改动只给写路径加开销,不可能影响纯 SELECT,故噪声主导成立。
- 采纳评审 minor("fence 观察逐行而非逐语句"):新增同步无分配的
  `Transaction::data_migration_phase_pin`,首行之后跳过共享锁与装箱 async 调用。

## 后续

- P10-2.2(下一票):DROP/TRUNCATE v2 清理有界化(route skip-scan),backfill 前置。
- 记入 2.3 范围:CTE 材料化直写 `data:{cte}:{row}` 键(既有行为,非本次回归),
  backfill 枚举 `data:` 前缀时须排除,否则把临时表灌进 v2。
- 未决(设计稿 open questions):pre-T1 二进制降级无法在存储层拦截(manifest feature-bit 提案);
  CDC 对 phase record put 产生二进制键事件的去留须在 2.6 前定。
