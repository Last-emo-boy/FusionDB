# BENCHPROD-466a 设计:确定性 KvBatch 复制(替代 raw SQL 重执行)

## 问题(调研与代码核实均已完成)

`apply_sql`(distributed/store.rs:57)在每个副本重新执行 SQL:
1. **非确定性发散**:无 PK INSERT 的 UUIDv4 row_id、NOW()/CURRENT_TIMESTAMP、
   SERIAL 默认值(基于本地扫描的 max+1)在各副本产生不同物理键/值。
2. **错误被吞**:每语句错误折叠进 Response 字符串,状态机永不失败 →
   副本可静默分叉。
3. **双写路径**:pgwire 本地 DML 完全绕过 Raft(466b 范畴)。

业界对照(调研 distributed-placement 报告):无生产系统复制 raw SQL;
rqlite 式改写无法钉住执行器内部生成的值;正解是 proposer-evaluated
物理变更(缩小版 CRDB/TiKV)。

## 目标模型(小系统适配)

**全量复制 + 计算分区**(调研裁决:数据超单机前不做 multi-raft/PD):
- 单 Raft 组复制 **OCC 写集**(物化的 puts/deletes + side-index deltas);
- fan-out 变为"复制数据上的计算分区"(disjoint key-range 切片,
  ShardRouter 降级为性能提示),消除 owner-local 与全复制的矛盾(466 系列后步)。

## 机制:capture → propose → deterministic-apply

1. **Capture(leader)**:执行 SQL 到 staged 状态——复用现有
   `execute_in_transaction` + FusionTransaction,但不 commit:
   提取 `(write_buffer, side_index_deltas, read_ts)` 后 rollback。
   需要 FusionTransaction 增加 `take_staged_state()`(消费式提取,
   与 465 的 delta buffer 同源)。
2. **Propose**:Raft entry 从 `Request { sql }` 演进为枚举:
   ```rust
   pub enum Request {
       Sql { sql: String },                    // 兼容旧 entry(内存日志,单版本集群假设,披露)
       KvBatch {
           puts: Vec<(Vec<u8>, Vec<u8>)>,
           deletes: Vec<Vec<u8>>,
           side_deltas: Vec<SideIndexDeltaWire>, // trigram/vector 的 serde 化镜像
           captured_read_ts: u64,
       },
   }
   ```
3. **Apply(每副本,含 leader)**:按日志序对 KvBatch 做
   **确定性 OCC 重验证**——对每个 key 检查
   `latest_committed_timestamp > captured_read_ts` ⟹ 确定性 CONFLICT
   (所有副本看相同日志序与相同已应用状态,验证结果必然一致);
   通过则走物理提交路径(复用 fusion 的 entries 提交机制,
   参考 replace_visible_entries_for_snapshot 的写入骨架)+ 应用
   side deltas;冲突则 entry 结果=Conflict(响应给客户端重试),
   **绝不静默吞错**——apply 内部 I/O 失败必须 panic/halt(fail-stop)
   而非继续(分叉比宕机糟)。
4. **读**:leader 本地读(现状);follower 读继续 stale-read(现状,披露)。

## 正确性论证要点

- 确定性:apply 只依赖 (日志序, 已应用状态, entry 内容) 三者,副本间全部相同。
- capture 与 propose 之间的竞争:leader 本地并发写可能使 captured_read_ts
  过期 → apply 时确定性 CONFLICT → 客户端重试;安全(无脏写),活性由重试保证。
- 幂等/恢复:Raft 元数据仍在内存(466c 之前),重启丢日志=现状缺陷不加剧;
  466c(vote/log 持久化到 FusionStorage 保留键)后天然获得重放幂等
  (相同 commit_ts 的相同 entries 重放 = MVCC 同版本覆盖)。

## 分片票据切分

- 466a:Request 枚举 + capture API + KvBatch apply 路径 + HTTP /raft/write 切换(单节点+2 节点 pg_integration 测试)。
- 466b:pgwire DML → submit_raft_write。
- 466c:vote/log/membership 持久化。
- 466e:fan-out 改计算分区(依赖 a)。

## 风险与测试

- 大事务 entry 体积(批量 INSERT 的物化写集)→ 上限保护 + 披露。
- 测试:两节点(既有 pg_integration 2-node 模式)UUID INSERT 后两副本
  SELECT 一致;并发写 capture 过期 → Conflict 重试成功;apply I/O 失败 fail-stop。
