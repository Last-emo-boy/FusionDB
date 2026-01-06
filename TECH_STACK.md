# FusionDB 技术栈 (Tech Stack)

## 核心语言
- **Rust**: 2021 edition
  - 选择理由: 内存安全、高性能、零成本抽象，适合构建数据库内核。

## 关键依赖库
### 1. SQL 解析与执行
- **sqlparser**: v0.60
  - 用于解析 SQL 语句 (CREATE TABLE, INSERT, SELECT, CREATE INDEX 等)。
- **serde** & **serde_json**:
  - 用于元数据 (Schema) 和 JSON 数据类型的序列化与反序列化。

### 2. 存储引擎
- **sled**: v0.34
  - 高性能嵌入式 KV 数据库 (基于 Bw-Tree)。
  - 提供原子批处理 (Batch) 和前缀扫描 (Scan Prefix) 功能，支持事务构建。
  - **Storage Trait**: 抽象了底层存储，支持内存 (Memory) 和磁盘 (Sled) 两种模式。
  - **Indexing**: 基于 KV 的二级索引实现 (`index:{table}:{col}:{val}:{row_id}`).

### 3. 网络服务
- **axum**: v0.7
  - 现代、符合人体工程学的 Web 框架 (基于 Tokio)。
- **tokio**:
  - 异步运行时。

### 4. 工具与辅助
- **thiserror**: 错误处理。
- **async-trait**: 异步 Trait 支持。
- **uuid**: 生成唯一 Row ID。

## 架构设计
1.  **Protocol Layer**: HTTP (Axum), 接收 SQL 请求。
2.  **SQL Layer**: Parser -> AST -> Executor。
3.  **Transaction Layer**: 提供原子性 (Atomicity) 保证，管理 Write Buffer。
4.  **Storage Layer**: 抽象 KV 存储，处理数据持久化和索引维护。
