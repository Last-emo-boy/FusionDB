# BENCHPROD-465: side-index(trigram/HNSW)提交耦合延迟应用

## 问题

trigram 与 HNSW 内存索引在 DML 执行期(OCC 验证**前**)被直接修改;
abort/rollback 的事务留下幻影条目,无补偿路径。

## 设计(调研定型,InnoDB FTS 延迟模式)

`FusionTransaction.side_index_deltas: Mutex<Vec<SideIndexDelta>>`
(TrigramAdd/TrigramRemove/VectorInsert/VectorDelete)。Executor helper 在 Fusion
后端只缓冲 delta;`commit` 在 commit_lock 内、OCC + WAL 持久 + memtable 发布**后**、
`current_ts` 可见性水位**前**统一应用;rollback 丢弃缓冲;空写集提交也应用 delta。
与 464 的分工:约束键进 OCC 写集,搜索索引 delta 不进——调研裁定的区分显式落地。

**defer 时维度校验**:`VectorIndex::validate_insert_dimensions` 在缓冲前校验,
错维 EMBEDDING() 在提交前大声失败(否则 post-commit apply 只能记日志吞错)。

迁移站点:3 个 trigram helper + 8 个 HNSW 写点(含一个 rustfmt 断行导致单行 grep
漏检的站点——教训:多行调用形态要用结构化搜索)。

## 中途回退的越范围改动(重要教训)

曾为让 SQL 字面量喂 HNSW 加了 Array→Vector coercion,对抗性 review **实测复现**
其影响面后回退:pgwire 线格式/OID 翻转(FLOAT8_ARRAY→TEXT Debug)、JSON f32 放大
噪声、COPY FROM 与 pgwire 文本参数硬失败、比较查询整体报错、历史 Array 行无迁移、
错维行重启毒化整列向量搜索。字面量摄入需要独立设计票(BENCHPROD-467)。

## 对抗性 review(4 维 × find→verify,21 agents,16 确认/1 否决)

- 否决的 1 项(空写集提交丢 delta)= review 进行中已抢先修复 ✓。
- coercion 相关 6 项 → 回退解决;维度吞错 → defer 时校验解决;崩溃恢复注释过度
  声明 → 改写(进程内不变量 + checkpoint 粒度披露)。
- 确认为设计代价并披露:事务内 RYOW 收窄(未提交文本/向量对本事务的 LIKE/向量
  搜索不可见,直到提交);旧快照仍可见的行的索引条目在提交时移除(候选重验证
  兜底,不产生错误行)。
- 确认为 pre-existing 另票:Raft 快照安装只重建 vector 不重建 trigram;
  HNSW 读路径不接受字面量查询;历史 Array 行不可索引。

## 验证

fmt/clippy/diff 干净;lib 563;sql_index_cache +3(trigram 延迟三态、EMBEDDING()
向量延迟三态、OCC abort 只留胜者 postings);全量 --all-targets 绿。
基准:trigram/HNSW 无注册基准工作负载(既有披露),写路径仅增 Vec push,
延迟应用在 commit 内一次性完成。

## 后续

BENCHPROD-467(向量字面量端到端)、466 系列吸收 Raft 快照 trigram 重建、
搜索侧合并未提交 delta 恢复事务内 RYOW。
