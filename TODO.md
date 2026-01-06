# FusionDB 开发计划 (Roadmap)

FusionDB 旨在打造一个**极致性能**、**多模态融合**、**AI 原生**的嵌入式数据库。以下是详尽的工程迭代路线图。

## Phase 1-5: 基础核心 (Completed)
- [x] **Core**: 项目初始化, 类型系统, 错误处理.
- [x] **Storage**: Sled 引擎集成, 原子事务基础.
- [x] **Query**: Parser, Create/Insert/Select, Where, Order By, Limit/Offset.
- [x] **Multi-Model**: JSON 支持 (`->`), Vector 支持 (Distance, Sort).
- [x] **Index**: B+Tree 索引, 自动维护, 等值查询优化.
- [x] **DML/DDL**: Update, Delete, Drop Table.
- [x] **Aggregation**: Count(*).

---

## Phase 6: 高级查询与执行引擎 (Advanced Query & Execution)
- [x] **Projection (投影)**:
    - [x] 支持指定列查询 `SELECT col1, col2`.
    - [x] 支持别名 `SELECT col1 AS alias`.
    - [ ] 支持表达式 `SELECT price * 0.9`.
- [ ] **Aggregation (聚合)**:
    - [x] 实现 `GROUP BY` 分组.
    - [x] 实现 `HAVING` 过滤.
    - [x] 支持 `SUM`, `AVG`, `MIN`, `MAX`.
- [ ] **Joins (连接)**:
    - [ ] 实现 `Nested Loop Join`.
    - [ ] 实现 `Hash Join` (优化).
    - [ ] 支持 `INNER`, `LEFT`, `RIGHT`, `CROSS` JOIN.
- [ ] **Advanced Features**:
    - [ ] 子查询 (Subqueries) `WHERE id IN (SELECT ...)`.
    - [ ] CTE (Common Table Expressions) `WITH t AS (...)`.
    - [ ] 视图 (Views).
    - [ ] 窗口函数 (Window Functions) `ROW_NUMBER() OVER (...)`.

## Phase 7: 深度多模态集成 (Advanced Multi-Model)
- [ ] **Full Text Search (FTS)**:
    - [ ] 实现倒排索引 (Inverted Index).
    - [ ] 支持分词 (Tokenization) 和 BM25 相关性评分.
    - [ ] **Hybrid Search**: 融合向量搜索 (Dense) 和全文检索 (Sparse) 的结果 (RRF - Reciprocal Rank Fusion).
- [ ] **Graph Database Capabilities**:
    - [ ] 支持递归查询 (Recursive CTE) 进行图遍历.
    - [ ] 实现最短路径算法 (Shortest Path).
- [ ] **Geo-Spatial**:
    - [ ] 支持 GeoJSON 数据类型.
    - [ ] 实现 R-Tree 索引或 Geohash.
    - [ ] 支持 `ST_Distance`, `ST_Contains` 等地理函数.

## Phase 8: AI 与 LLM 集成 (AI-Native Integration)
- [ ] **In-Database Embedding**:
    - [ ] 集成 `ort` (ONNX Runtime) 或 `candle`，直接在数据库内运行 Embedding 模型 (如 `all-MiniLM-L6-v2`).
    - [ ] 自动生成向量: `INSERT INTO docs (content) VALUES ('hello')` -> 自动填充 embedding 列.
- [ ] **RAG Primitives**:
    - [ ] 内置文档切片 (Chunking) 函数.
    - [ ] 语义去重 (Semantic Deduplication).
- [ ] **Model Serving**:
    - [ ] 允许加载轻量级 LLM (如 Llama-3-8B-Quantized) 进行简单的推理或重排序 (Re-ranking).

## Phase 9: 协议与接口 (Server & Protocol)
- [ ] **Wire Protocol**:
    - [ ] **PostgreSQL Protocol Compatible**: 兼容 pgwire，支持 psql, JDBC, Go/Python PG drivers 直连.
- [ ] **Server Optimization**:
    - [ ] Connection Pooling.
    - [ ] Async IO (Tokio-uring).
    - [ ] Streaming Response (避免大结果集爆内存).

## Phase 10: 事务与并发 (Transaction & Concurrency)
- [ ] **Concurrency Control**:
    - [ ] MVCC (Multi-Version Concurrency Control) 实现.
    - [ ] 隔离级别: Read Committed, Snapshot Isolation, Serializable.
    - [ ] 死锁检测 (Deadlock Detection).
- [ ] **Transaction Management**:
    - [ ] 显式事务 `BEGIN`, `COMMIT`, `ROLLBACK`.
    - [ ] Savepoints.

## Phase 11: 极致性能优化 (Extreme Performance)
- [ ] **Vector Acceleration**:
    - [ ] **SIMD**: 使用 AVX2/AVX-512 指令集加速向量距离计算 (`f32` dot product / euclidean).
    - [ ] **Quantization**: 支持 Scalar Quantization (SQ8) 和 Product Quantization (PQ) 压缩向量，减少内存占用并加速计算.
- [ ] **Execution Engine**:
    - [ ] **Vectorized Execution**: 从火山模型 (Volcano) 升级为向量化执行 (Vectorized Batch Processing)，一次处理一批数据 (e.g., Arrow RecordBatch).
    - [ ] **JIT Compilation**: 使用 Cranelift 或 LLVM 动态编译查询表达式，消除解释执行开销.
    - [ ] **Parallel Query**: 多线程并行扫描与聚合 (Parallel Scan/Agg).
- [ ] **Memory Management**:
    - [ ] **Zero-Copy**: 使用 `rkyv` 替代 `serde_json` 实现零拷贝反序列化.
    - [ ] **Custom Allocator**: 集成 `mimalloc` 或 `jemalloc`.

## Phase 12: 存储引擎内核 (Storage Engine Internals)
- [ ] **Pluggable Storage**:
    - [ ] 抽象底层存储接口，解耦 Sled.
    - [ ] 适配 RocksDB 或 MDBX (作为可选后端).
- [ ] **Data Organization**:
    - [ ] 列式存储优化 (Columnar Storage for Analytics).
    - [ ] 数据压缩 (LZ4/Zstd).
- [ ] **Reliability**:
    - [ ] WAL (Write Ahead Log) 显式管理.
    - [ ] Checkpoint 机制.
    - [ ] Crash Recovery 测试.

## Phase 13: 智能优化器 (Optimizer & Indexing)
- [ ] **Query Optimizer**:
    - [ ] Rule-based Optimizer (RBO): 谓词下推, 常量折叠.
    - [ ] Cost-based Optimizer (CBO): 基于统计信息选择最优计划.
    - [ ] 统计信息收集 (Histogram, Distinct Count).
- [ ] **Advanced Indexing**:
    - [ ] **Vector Index**: 引入 HNSW / IVFFlat 算法 (替换暴力扫描).
    - [ ] **Composite Index**: 复合索引 `(col1, col2)`.
    - [ ] **JSON Index**: 对 JSON 路径建索引.

## Phase 14: 分布式架构 (Distributed System)
- [ ] **Consensus**:
    - [ ] 集成 Raft (OpenRaft) 实现多副本一致性.
- [ ] **Replication**:
    - [ ] Leader-Follower 架构 (High Availability).
    - [ ] Read Replica (读写分离).
- [ ] **Sharding**:
    - [ ] Hash Sharding / Range Sharding.
    - [ ] 分布式事务 (2PC / Percolator).

## Phase 15: 安全与生态 (Security & Ecosystem)
- [ ] **Security**:
    - [ ] Authentication (SCRAM-SHA-256).
    - [ ] RBAC (Role-Based Access Control).
    - [ ] Encryption at Rest / TLS.
- [ ] **Clients & Tools**:
    - [ ] Python/JS/Rust SDKs.
    - [ ] CLI & Web Dashboard.
    - [ ] LangChain/LlamaIndex Integration.
