# P10-2 设计:Data V2 迁移阶梯(持久 phase record → legacy-gc 全程拆票)

> 2026-07-20。产出方式:4 个代码状态读取 agent(codec/catalog/key families/测试机)
> → 2 份独立设计(最小风险派 vs 故障模式派)→ 对抗评审合并(分歧点逐一回代码实证)。
> 依据:docs/PRODUCTION_HARDENING.md P0 相位阶梯 legacy → delete-only → write-delete-shadow
> → backfill → validated → v2-readable → v2-only → legacy-gc。

## 现状边界(读取已核实)

- v2 实际产出仅两种:Data namespace 影子行键(`structured_data_shadow_v2` 默认 off,
  execution/mod.rs:813-831)与 Catalog 复合索引目录标记(composite_index.rs:66-78)。
  读路径 100% legacy。**无任何 phase/fence/backfill 代码**,flag 是进程本地配置,
  存储状态无法自证影子完整性。
- flag=off ≠ 惰性:影子墓碑与 DROP/TRUNCATE 全 namespace 清扫无条件执行
  (mod.rs:833-863)——现实状态恒 ≥ delete-only,故 phase 序数 0(Legacy)只保留、不物化。
- DROP/TRUNCATE 清理是 O(整个 v2 Data namespace) 的 scan_prefix 且全量物化 (k,v)。
- 关键危险(families 读取):ShardRouter 只枚举当前 shard_count 且开 router 后
  **不含 unsharded 前缀**(mod.rs:878-889)——backfill 枚举绝不能经 router,
  必须 raw 前缀 + skip-scan 发现历史 route。
- `unique:` 哨兵按 VALUE 路由,且 KeyNamespace 枚举(keyspace.rs:16-24)**没有为它保留位**。
- 分布式:catalog KV 写走 MutationBatch 完整复制;precondition 失配 = fail-closed 停机
  (store.rs:1561 测试钉死),优雅确定性拒绝的既有通道是 `Ok(Response::error)`
  (store.rs:263-267);apply 后(:336)与 snapshot install 后(:384)都调
  invalidate_storage_caches。

## 票梯(9 票,严格单一关注点)

| 票 | 内容 | 解锁 |
|---|---|---|
| **P10-2.1** | 持久 phase record + 全 data-writer commit-fence + CALL/SHOW 操作面(record 取代 flag) | advance ≤ write-delete-shadow |
| **P10-2.2** | DROP/TRUNCATE v2 清理有界化:route skip-scan + 流式 scan_range_for_each(backfill 灌满 namespace 前必须先落) | — |
| **P10-2.3** | Backfill 引擎:512 行幂等 chunk + durable checkpoint + DDL 冲突点 + 无 precondition data-batch 拒绝守卫 | advance → Backfill |
| **P10-2.4** | Exact verifier(同 snapshot 双游标 merge、三类差异分类)+ verify-token(盖 phase_seq) | advance → Validated |
| **P10-2.5** | v2 读基础设施 dark launch:SSTable 剪枝抽取器认 v2 键、点/范围读实现、side-index v2 分支(零行为切换) | — |
| **P10-2.6** | 读切换 + CDC 同相翻转(单机 scope,分布式 CDC 归 P10-7) | advance → V2Readable |
| **P10-2.7** | v2-only 写翻转:停写 legacy Data family(不可回退点,legacy 键集冻结为不变式) | advance → V2Only |
| **P10-2.8** | legacy-gc 分块可恢复清扫 + flag 退役(no-op + warn)+ 全家族键清点终验 | advance → LegacyGc |
| **P10-2.9** | 剩余 key families 收口:unique namespace 补位优先;index/fts/count-summary 评估 rebuild-based vs 平行阶梯,裁决写回 ROADMAP(不许静默缩水 charter) | — |

排序理由:2.2 前置是因为 backfill 之后每次 DROP/TRUNCATE 都是 O(全库);2.5 先 dark
再 2.6 切读,避免切换票同时动"读实现+剪枝+CDC"三件事;2.9 殿后但属 charter
(ROADMAP.md P10-2 明列 remaining key families)。

## P10-2.1 详细设计(下一执行票)

### Phase record

- **Key** = `encode_identifier_key(Catalog, ["data-v2-migration-phase"])`。
  依据:`\0` 开头对一切 legacy 字符串前缀扫描不可见;非 Data namespace 不进
  DROP/TRUNCATE 的 hard-error 清扫(codec 本就禁止 Data 走 identifier 编码,
  keyspace.rs:249);与 composite-index-directory 首组件互异;
  `{family}-v2-migration-phase` 命名为 2.9 的 unique/index 阶梯与 codec v3 留平行空间。
- **Value** = 18 字节定长 v1:`[version:u8=1][phase:u8∈1..=7][phase_seq:u64BE][updated_at_ms:u64BE]`。
  严格 decode(len==18 && version==1 && 1<=phase<=7,违者 loud Err)。
  弃 bincode 双层:定长可判定、CAS 期望值字节稳定、benchmark.py 一行复刻。
  **phase_seq 是实质能力**:apply 单调守卫自描述、2.4 verify-token 盖戳、审计。
- **Crash 原子性**:单键 put 融入事务 write_buffer → 单 CRC 帧进 WAL → torn tail
  丢整批,无中间态;可见性水位在 WAL 持久后推进(fusion.rs:4791-4793)。
- **复制**:普通 KV。leader 求值捕获进 MutationBatch,put 自动携带 pre-image
  precondition(api.rs:133-141/161-168,expected=None 支持首建 CAS);快照全量导出
  自动携带;**绝不落 raft 节点本地文件**。

### 双常量

- `MAX_SUPPORTED_PHASE = WriteDeleteShadow`:本二进制完整实现(写+读+CDC 契约)的
  最高相,实现票逐票上调恰一格。拒绝超前宣称(T1 二进制不含 CDC 翻转/v2 读,
  在 V2Readable 相运行会按节点版本分叉)。
- `MAX_ADVANCE_TARGET = WriteDeleteShadow`:本二进制允许推进到的目标,gate 票上调
  (与 SUPPORTED 分离,支持 dark-launch 排序:2.5 升代码不升承诺)。

### Fence(三层)

**核心不变量**:任何含 data-family 写的事务,其行为所依据的 phase 必须等于提交时
(单机)/求值与应用时(分布式)的持久 phase。读事务不设 fence,跨相读安全靠
"唯一改读语义的翻转 Validated→V2Readable 发生在 verifier 已证等价的两族之间"。

1. **观察缓存**:`FenceCell = RwLock<FenceState{Unloaded|Missing{flag 推导}|Loaded(Arc)}>`
   挂 FusionStorage;`invalidate_storage_caches`(execution/mod.rs:3782)追加一行
   `FenceCell.invalidate()` → **自动覆盖 raft apply(store.rs:336)与 snapshot
   install(store.rs:384)两个既有调用点**(仅 apply 侧刷新会让 install 后 fence
   永久陈旧,已实证是真实缺口)。
2. **单机 commit 等值**:三个 data-writer helper 打 fence(FusionTransaction 记
   (phase,seq),同事务二次不同值立即报错);commit 在 commit_lock 内
   (fusion.rs:4694 起,串行化全部提交)等值校验,advance 的 fence 发布与
   current_ts 同临界区 → 完整全序,skew=0。**pg 交互事务在 COMMIT 时提交真实
   FusionTransaction(pg_server.rs:9321/9339)→ 天然全覆盖**,无需会话插桩/epoch
   drain/超时。P5-4 未收口的绕 raft 本地写路径同样被本层兜住,不留静默窗口。
   成本:每 fenced commit 一次 RwLock 读 + u8 比较;只读/纯 catalog 事务零开销。
3. **分布式**:(a) 优雅仲裁在求值层——RAFT_WRITE_EVALUATION_LOCK(api.rs:589)
   串行化 advance 与一切写求值,并发 advance 由 target==next/幂等/上限规则以
   loud SQL 错误裁决;稳定 leader 上 precondition 结构性不可触发。
   (b) RecordingTransaction 覆写 fence → `record_precondition(phase_key)`,
   expected 取 inner.get 的 leader 已应用状态(**不用缓存值当 expected——缓存陈旧
   会制造 spurious 失配,而失配=fail-closed 停机,等于把良性竞态放大成停机**);
   唯一职能 = 跨 leader 变更的陈旧提议确定性截停。
   (c) apply 单调守卫:phase 键 Delete 拒绝;Put 必须首建(seq==1,phase∈{1,2})
   或严格 next(seq==old+1 && phase==old.next()),违反 → rollback +
   **`Ok(Response::error)`(确定性拒绝、状态不变、重放同判、节点不停机——
   两份初设计都误用了停机或重试语义,此处必须用 store.rs:263-267 的既有优雅通道)**;
   唯 phase > MAX_SUPPORTED → StorageError 停机(halt-don't-diverge)。
   (d) **install 门(两份初设计共同遗漏)**:快照载荷内 phase record decode 后
   > MAX_SUPPORTED → snapshot_read_error 拒装。
   (e) 预埋(2.3 启用):record>=Backfill 时,含 data-family 写而无 phase
   precondition 的 batch → 确定性拒绝——pre-T1 leader 的批次天然无该 precondition,
   其治下写全停=响亮故障,取代纯运维纪律。

### 操作面

- `CALL fusiondb_data_migration_init()`:幂等;无 record → put {v1, flag?wds:delete-only, seq=1}。
  **绝不自动 INIT**(避免 follower 本地写复制状态机 + 未经操作员同意的持久状态)。
- `CALL fusiondb_data_migration_advance('<name>')`:target==current 幂等零写
  (崩溃重试安全);target != current.next() → loud(降级/越级);
  > MAX_ADVANCE_TARGET → loud "not supported by this build"。
- CALL 走 sqlparser 原生解析(sqlparser-0.60 自身 parser/mod.rs:595 Keyword::CALL
  → ast/mod.rs:3223 Statement::Call;我们的 56 行 wrapper 免费获得,executor
  match 加 `Statement::Call` 臂,其余 CALL 名保持 loud 不支持)。
  statement_may_change_query_results 对这两个函数名返回 true → 自动进
  sql_requires_raft_write 路由 + leader 自动转发(api.rs:574-584),
  非 leader 报错问题整体消失。VACUUM standalone 规则镜像(禁同批混排)。
- `SHOW DATA MIGRATION PHASE`(本地只读):record 三元组或 "no record (config-derived: X)"。

### 触碰文件(8 个)

1. **新 src/storage/data_migration.rs**:枚举/codec/常量/FenceCell(必须在 storage 层,
   fusion.rs 与 store.rs 都要用类型)。
2. **src/storage/mod.rs**:Transaction trait 加默认 no-op
   `fence_data_migration_phase(phase,seq)`(MemoryStorage 零改动)。
3. **src/storage/fusion.rs**:FenceCell 字段;with_config 在 current_ts restore 后、
   rebuild_side_indexes 前读 record——decode 失败或 phase>MAX_SUPPORTED →
   **直接 Err 拒开(单一咽喉覆盖 server/store/tests)**;record 与 flag 矛盾 →
   一次 warn,record 恒胜;FusionTransaction fence 字段 + commit 等值 + 临界区发布。
4. **src/execution/mod.rs**:observe_and_fence(Loaded 快路径;Unloaded → txn.get
   穿 RecordingTransaction 读 leader 已应用状态,与 precondition expected 同源自洽);
   write_routed_data_row 影子 put 条件 → `phase>=WriteDeleteShadow`;两个删除
   helper 行为不变但**同样打 fence**(2.3 守卫要求删除型 batch 也携带 precondition,
   否则 T1 自己的 DELETE 会被误拒;2.7 删除变相时 fence 已就位);CALL 两臂 + SHOW;
   invalidate_storage_caches 追加 FenceCell.invalidate()。
5. **src/distributed/api.rs**:RecordingTransaction 覆写 fence → record_precondition
   + 转发。evaluate_sql_to_request 零修改。
6. **src/distributed/store.rs**:apply 单调守卫 + install 门(如上)。
7. **src/config.rs / fusiondb.toml**:仅注释(INIT 后 record 永久取代 flag)。
8. **src/main.rs**:零改动(门在 with_config)。

### 语义总表(T1 可达)

| 状态 | shadow put | 盲墓碑/清扫 | 说明 |
|---|---|---|---|
| 无 record + flag off | 否 | 是 | == 今日 off |
| 无 record + flag on | 是 | 是 | == 今日 on |
| record=delete-only | 否 | 是 | flag 失效 + 一次 warn |
| record=write-delete-shadow | 是 | 是 | flag 失效,集群一致 |
| record=backfill..legacy-gc | — | — | 拒开 / apply 停机 / install 拒装 |

读路径零改动:任何 T1 相只读 legacy,resurrection 结构性排除。
向后兼容:无 record(=全部现存库)打开零写入、行为逐字节继承 flag;
4 个既有 shadow 测试(execution/mod.rs:6249/6347/6459、fusion.rs:5313)零修改通过
是硬验收线。

## 测试计划(要点)

- **A. crash-at-transition 矩阵**(T1 建骨架,后续每票扩自己的转换,8 相全覆盖=P10-2 收尾门):
  WalFaultPoint one-shot × torn-tail 制造 × 同 dir 重开三件套;
  {无→INIT(两态), delete-only→wds} × {提交前, torn, 提交后发布前, 正常};
  断言 record 恰为新旧之一、写行为与 record 严格一致、oracle 双族等值。
  每个崩溃点独立 fixture(one-shot 不复用,store.rs 矩阵纪律)。
- **B. fencing race**:在飞事务×advance(loud abort→重试得新相);
  **pg 交互事务 BEGIN..写..advance..COMMIT → COMMIT loud abort(两份初设计的
  分歧点,必须有测试钉住)**;并发双 advance 恰一胜 + 重试撞幂等;伪造 record
  矩阵(malformed/version=2/phase=0/99/超限);flag 四象限被 record 压制。
- **C. 分布式**:advance batch 含 precondition 断言;失配 = fail-closed 语义
  (镜像 store.rs:1561,明确不是重试);apply 守卫矩阵(跳 seq/跳级/降级/Delete/
  malformed → 优雅拒绝不停机;超 MAX_SUPPORTED → 停机);install 超限拒装;
  幂等重放不双推 seq。
- **F. 基准门**:同盘 ext4(严禁 tmpfs)、BENCH_PROTO=pg(亚毫秒点写对每提交
  开销最敏感)、Parts 1/2/3/5 + bulk、medium+xlarge 双档(memtable 容量类只在
  xlarge 现形)。T1 预算 = 每事务一次 RwLock 读 + u8 比较,应没入噪声,
  任何 unexplained regression 即 blocker。

## Open questions(截录,完整见评审输出)

1. 2.9 各族路线:index/fts/count-summary 可由 base row 重导出,rebuild-based
   可能免整套 backfill+verifier;裁决必须写回 ROADMAP,不许静默缩水。
2. pre-T1 二进制不认识 record,单机降级拦不住 → 查 manifest v2 对未知
   feature-bit 是否 loud 拒绝;若是,INIT 置 required-bit 变机器强制
   (独立小票,须赶在 2.3 解锁 Backfill 前)。
3. CDC 对 phase record put 产生一条二进制键事件(composite marker 同先例)——
   审计信号 vs 2.6 统一压制,须在 2.6 前定,避免 CDC 契约改两次。
4. 回滚政策:V2Only 起 legacy 不完整;Validated 前是否给 supervised force-set;
   恢复 pre-advance 备份会静默回退 phase(record 随备份走)→ P10-5 备份票文档化。
5. advance 到 >=V2Readable 前是否加集群能力握手(查全部 voter 的 MAX_SUPPORTED),
   把"全节点升级后再 advance"变机器强制;当前靠 apply 停机门兜底。
6. phase/seq 进 /metrics 与 SHOW ALL(建议 2.3 随 backfill 进度一并做)。
