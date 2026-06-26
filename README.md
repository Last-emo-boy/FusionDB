# FusionDB

**FusionDB** is a high-performance, multimodal, ACID-compliant database written in Rust. It combines relational SQL, vector search, full-text search, and AI-native embedding into a single engine, with zero external dependencies at runtime.

![CI Status](https://github.com/last-emo-boy/FusionDB/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Tests](https://img.shields.io/badge/tests-176%20passing-brightgreen)
![Rust](https://img.shields.io/badge/rust-2021%20edition-orange)

---

## Table of Contents

- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Connecting](#connecting)
- [SQL Reference](#sql-reference)
- [HTTP API Reference](#http-api-reference)
- [AI-Native Functions](#ai-native-functions)
- [Storage Architecture](#storage-architecture)
- [Observability](#observability)
- [Benchmark](#benchmark)
- [Project Structure](#project-structure)
- [Current Limitations](#current-limitations)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

---

## Key Features

### Multimodal Data

| Capability | Description |
|---|---|
| **Relational SQL** | SELECT, INSERT, UPDATE, DELETE, JOIN (INNER/LEFT/RIGHT/CROSS), GROUP BY, ORDER BY (multi-column ASC/DESC), HAVING, DISTINCT, LIMIT/OFFSET, BETWEEN, IN, LIKE, ILIKE, IS NULL, CASE WHEN, UNION/INTERSECT/EXCEPT, subqueries, correlated EXISTS/NOT EXISTS, CTE (`WITH ... AS`) |
| **DDL** | CREATE/DROP TABLE (IF NOT EXISTS / IF EXISTS), CREATE/DROP VIEW, ALTER TABLE (ADD/DROP/RENAME COLUMN), CREATE/DROP INDEX (IF EXISTS), TRUNCATE, SHOW TABLES/VIEWS, SHOW CREATE TABLE, NOT NULL / DEFAULT / UNIQUE / CHECK constraints |
| **Functions** | UPPER, LOWER, LENGTH, CONCAT, SUBSTRING, REPLACE, TRIM, ABS, ROUND, CEIL, FLOOR, MOD, POWER, SQRT, COALESCE, NULLIF, CAST, NOW() |
| **Window Functions** | ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD with `OVER (PARTITION BY ... ORDER BY ...)` |
| **Advanced DML** | INSERT ... SELECT, INSERT with column list, INSERT ... ON CONFLICT (UPSERT), INSERT/UPDATE/DELETE ... RETURNING, COUNT(DISTINCT col), STRING_AGG/GROUP_CONCAT, bare aggregates without GROUP BY, `\|\|` string concat |
| **Vector Search** | VECTOR data type, HNSW index, `VECTOR_DISTANCE()` / `COSINE_SIMILARITY()` functions |
| **Full-Text Search** | BM25 inverted index, trigram fuzzy matching, `MATCH ... AGAINST` syntax |
| **Hybrid Search** | RRF (Reciprocal Rank Fusion) combining vector + text results |
| **AI Embedding** | Built-in `EMBEDDING()` SQL function, pluggable provider architecture |

### Storage Engine (FusionStorage)

| Component | Technology |
|---|---|
| **Write Path** | Lock-free SkipList MemTable → Segmented WAL → SSTable flush |
| **Read Path** | MVCC Snapshot Isolation, Row Cache (Moka), Bloom Filters |
| **Indexes** | BTree (secondary), FB+-Tree (MemTable), HNSW (vector), Inverted (FTS), Trigram |
| **Transactions** | OCC (Optimistic Concurrency Control), Snapshot Isolation, `BEGIN`/`COMMIT`/`ROLLBACK` |
| **Durability** | Segmented WAL (64MB rotation), CRC32 block checksums and LZ4 block compression on SSTables |
| **Compaction** | 4-way merge with MVCC key deduplication |
| **Columnar Analytics** | Arrow RecordBatch conversion, vectorized COUNT/SUM/AVG/MIN/MAX |
| **Performance** | Optimized merge iterator, streaming COUNT(*), pre-allocated scan buffers, hash join for equi-joins, ANALYZE statistics, cost-based comma/inner join reordering |

### Infrastructure

| Feature | Description |
|---|---|
| **Protocol** | PostgreSQL wire protocol (pgwire) + HTTP JSON API + optional Redis-compatible RESP endpoint |
| **Configuration** | TOML config file (`fusiondb.toml`) with all server/storage/auth settings |
| **Authentication** | Configurable password auth, SHA-256 password hashing, RBAC (CREATE USER, GRANT, REVOKE) |
| **Graceful Shutdown** | Ctrl+C → flush MemTable → save indexes → truncate WAL |
| **Observability** | Slow query log, Prometheus `/metrics/prometheus`, `/slow_queries` JSON |
| **Dashboard UI** | Supabase-style web dashboard — SQL Editor, Table Browser, Metrics Dashboard |
| **Distributed** | OpenRaft consensus framework (TypeConfig, Log/StateMachine, HTTP Network) |
| **Backend Plugin** | `BackendConfig` factory — swap between Fusion / Memory backends via config |

---

## Quick Start

### Build from Source

```bash
git clone https://github.com/last-emo-boy/FusionDB.git
cd FusionDB
cargo build --release
./target/release/fusiondb
```

Output:
```
No config file found at fusiondb.toml. Using defaults.
FusionDB v0.1.0 starting...
  HTTP:    127.0.0.1:8091
  PgWire:  127.0.0.1:8092
  Redis:   disabled
  Data:    data
Press Ctrl+C to shut down...
```

### Generate Default Config

```bash
./target/release/fusiondb --init
# Creates fusiondb.toml with all default settings
```

### Docker

```bash
docker build -t fusiondb .
docker run -d -p 8091:8091 -p 8092:8092 -v fusion_data:/data --name fusiondb fusiondb:latest
```

### Run Tests

```bash
cargo test                              # 176 tests (unit + SQL integration + pgwire)
```

### Benchmark

```bash
# Start the server first
cargo run

# In another terminal — run unified benchmark (8 parts)
python benchmark.py                     # Medium scale (default)
BENCH_SCALE=small python benchmark.py   # Quick smoke test
BENCH_SCALE=large python benchmark.py   # Full stress test
```

The benchmark covers **8 scenarios** in a single run:

| Part | Scenario | What it tests |
|---|---|---|
| 1 | **Base Benchmarks** | PK lookup, full/index scan, range, aggregation, sort, single-row writes, complex filters |
| 2 | **E-commerce Simulation** | Customer lookup, product browsing, order placement, status updates, inventory deduction |
| 3 | **Financial Ledger** | Balance queries, transfer history, account debits, audit aggregations |
| 4 | **Analytics / OLAP** | Revenue reports, top spenders, category rankings, time-series events, subqueries |
| 5 | **Concurrent Workload** | Multi-threaded mixed R/W at 80:20, 50:50, 20:80 ratios with throughput measurement |
| 6 | **Stress & Edge Cases** | Wide IN, 3-table JOIN, high-cardinality GROUP BY, bulk UPDATE, UNION, CROSS JOIN |
| 7 | **Inventory & Fulfillment** | Stock rollups, reorder candidates, shipment queues, reservation joins, restock writes |
| 8 | **Risk & Audit** | Large-transfer review, failed-transfer audits, account exposure, suspicious spend/activity patterns |

Results are printed to terminal and saved as `benchmark_report_<scale>.json`, including latency percentiles, standard deviation, coefficient of variation, success/error counts, row throughput, and concurrent workload throughput.

### Admin CLI

```bash
# Health and metadata
cargo run --bin fusiondb-cli -- health
cargo run --bin fusiondb-cli -- capabilities
cargo run --bin fusiondb-cli -- tables

# SQL and operations
cargo run --bin fusiondb-cli -- query "SELECT * FROM users LIMIT 5"
cargo run --bin fusiondb-cli -- checkpoint
cargo run --bin fusiondb-cli -- vacuum
cargo run --bin fusiondb-cli -- cdc --since 0 --limit 100

# Custom endpoint/user
cargo run --bin fusiondb-cli -- --url http://127.0.0.1:8091 --user admin metrics
```

### Dashboard UI (FusionDB Studio)

```bash
cd dashboard
npm install
npm run dev             # Starts at http://localhost:5173
```

The dashboard auto-proxies API requests to FusionDB's HTTP server (default `127.0.0.1:3000`). Make sure FusionDB is running first.

**Pages:**
- **Dashboard** — Real-time metrics, table list, slow query log, checkpoint trigger
- **Table Editor** — Browse tables, view schema, filter/insert/delete rows inline
- **SQL Editor** — CodeMirror editor with SQL syntax highlighting, Ctrl+Enter to execute, CSV export
- **Settings** — Connection info, database capabilities overview

---

## Connecting

### Method 1: PostgreSQL Client (Recommended)

FusionDB speaks the PostgreSQL wire protocol. Use any Postgres client:

```bash
# psql
psql -h 127.0.0.1 -p 8092 -U admin -d fusiondb
# Password: fusiondb

# Or programmatically (Python)
pip install psycopg2-binary
```

```python
import psycopg2
conn = psycopg2.connect(host="127.0.0.1", port=8092, user="admin", password="fusiondb", dbname="fusiondb")
cur = conn.cursor()
cur.execute("SELECT * FROM my_table LIMIT 10")
rows = cur.fetchall()
```

```rust
// Rust (tokio-postgres)
let (client, connection) = tokio_postgres::connect(
    "host=127.0.0.1 port=8092 user=admin password=fusiondb", tokio_postgres::NoTls
).await?;
let rows = client.query("SELECT * FROM my_table", &[]).await?;
```

```javascript
// Node.js (pg)
const { Client } = require('pg');
const client = new Client({ host: '127.0.0.1', port: 8092, user: 'admin', password: 'fusiondb' });
await client.connect();
const res = await client.query('SELECT * FROM my_table');
```

### Method 2: HTTP JSON API

```bash
# Execute SQL
curl -X POST http://127.0.0.1:8091/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM my_table LIMIT 5"}'

# Health check
curl http://127.0.0.1:8091/health

# View metrics
curl http://127.0.0.1:8091/metrics

# List tables
curl http://127.0.0.1:8091/tables
```

### Method 3: Prepared Statements (HTTP)

```bash
# 1. Prepare
curl -X POST http://127.0.0.1:8091/prepare \
  -d '{"sql": "SELECT * FROM users WHERE id = $1"}'
# Returns: {"statement_id": "uuid-xxx", "error": null}

# 2. Execute with parameters
curl -X POST http://127.0.0.1:8091/execute \
  -d '{"statement_id": "uuid-xxx", "params": [42]}'
```

---

## SQL Reference

### Data Types

| Type | Description | Example |
|---|---|---|
| `INTEGER` / `INT` | 64-bit signed integer | `42` |
| `FLOAT` / `DOUBLE` | 64-bit floating point | `3.14` |
| `TEXT` / `VARCHAR` | UTF-8 string | `'hello'` |
| `BOOLEAN` / `BOOL` | Boolean | `TRUE` / `FALSE` |
| `BLOB` | Binary data | — |
| `VECTOR(N)` | N-dimensional float32 vector | `[0.1, 0.2, 0.3]` |

### DDL (Data Definition)

```sql
-- Create table
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT,
    score FLOAT,
    embedding VECTOR(128)
);

-- Create table (skip if exists)
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);

-- Create table with DEFAULT values
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    status TEXT DEFAULT 'pending',
    quantity INTEGER DEFAULT 1
);

-- Create BTree index (speeds up WHERE / JOIN)
CREATE INDEX idx_name ON users (name);

-- Create unique index
CREATE UNIQUE INDEX idx_email ON users (email);

-- Create HNSW vector index
CREATE INDEX idx_vec ON users (embedding) USING HNSW;

-- Create Full-Text Search index
CREATE INDEX idx_fts ON users (name) USING FTS;

-- Alter table
ALTER TABLE users ADD COLUMN age INTEGER;
ALTER TABLE users DROP COLUMN age;
ALTER TABLE users RENAME COLUMN email TO mail;

-- Truncate (delete all rows, keep schema)
TRUNCATE TABLE users;

-- Manual compaction / space reclamation trigger
VACUUM;
VACUUM FULL;

-- Drop table
DROP TABLE users;
DROP TABLE IF EXISTS users;

-- Show all tables
SHOW TABLES;

-- Show create statement
SHOW CREATE TABLE users;

-- Describe table structure
DESCRIBE users;
-- or: EXPLAIN users;

-- Create a view
CREATE VIEW active_users AS SELECT * FROM users WHERE score > 80;
CREATE OR REPLACE VIEW active_users AS SELECT * FROM users WHERE score > 90;

-- Drop a view
DROP VIEW active_users;
DROP VIEW IF EXISTS active_users;

-- User management (RBAC)
CREATE USER alice WITH PASSWORD 'secret123';
CREATE USER admin WITH PASSWORD 'admin' SUPERUSER;
DROP USER alice;
DROP USER IF EXISTS alice;
SHOW USERS;

-- Grant / Revoke permissions
GRANT SELECT, INSERT ON users TO alice;
GRANT ALL ON * TO admin;
REVOKE INSERT ON users FROM alice;
```

### DML (Data Manipulation)

```sql
-- Insert single row
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 95.5, [0.1, 0.2]);

-- Insert with column list (missing columns get NULL/DEFAULT)
INSERT INTO users (id, name) VALUES (4, 'Dave');

-- Insert with RETURNING (get inserted data back)
INSERT INTO users VALUES (5, 'Eve', 'eve@example.com', 88.0, [0.7, 0.8]) RETURNING *;
INSERT INTO users (id, name) VALUES (6, 'Frank') RETURNING id, name;

-- UPSERT (INSERT ... ON CONFLICT)
INSERT INTO users VALUES (1, 'Alice Updated', 'alice@new.com', 99.0, [0.1, 0.2])
    ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, score = EXCLUDED.score;
INSERT INTO users VALUES (1, 'Ignored', NULL, 0, NULL)
    ON CONFLICT (id) DO NOTHING;

-- Insert multiple rows (batch)
INSERT INTO users VALUES 
    (2, 'Bob', 'bob@example.com', 87.0, [0.3, 0.4]),
    (3, 'Carol', 'carol@example.com', 92.1, [0.5, 0.6]);

-- Update
UPDATE users SET score = 100.0 WHERE id = 1;
UPDATE users SET name = 'Alice Z', score = score + 5 WHERE name = 'Alice';

-- Delete
DELETE FROM users WHERE id = 3;
DELETE FROM users WHERE score < 60;

-- Insert from query (INSERT ... SELECT)
INSERT INTO archive SELECT * FROM users WHERE score < 60;
```

### Queries (SELECT)

```sql
-- Basic
SELECT * FROM users;
SELECT name, score FROM users WHERE score > 90;

-- Comparison operators: =, !=, <>, <, >, <=, >=
SELECT * FROM users WHERE score >= 90 AND score <= 100;

-- BETWEEN
SELECT * FROM users WHERE score BETWEEN 80 AND 95;

-- IN
SELECT * FROM users WHERE id IN (1, 2, 3);

-- LIKE (prefix matching)
SELECT * FROM users WHERE name LIKE 'Ali%';

-- ILIKE (case-insensitive LIKE)
SELECT * FROM users WHERE name ILIKE 'alice%';

-- IS NULL / IS NOT NULL
SELECT * FROM users WHERE email IS NOT NULL;

-- AND / OR with nesting
SELECT * FROM users WHERE (score > 90 OR name = 'Bob') AND email IS NOT NULL;

-- DISTINCT
SELECT DISTINCT name FROM users;

-- ORDER BY
SELECT * FROM users ORDER BY score DESC;
SELECT * FROM users ORDER BY name ASC, score DESC;

-- LIMIT
SELECT * FROM users ORDER BY score DESC LIMIT 10;

-- Aggregations
SELECT COUNT(*) FROM users;
SELECT AVG(score), MIN(score), MAX(score), SUM(score) FROM users;

-- GROUP BY
SELECT name, COUNT(*) as cnt FROM orders GROUP BY name;

-- HAVING
SELECT name, SUM(amount) as total 
FROM orders 
GROUP BY name 
HAVING SUM(amount) > 1000;

-- JOIN (INNER, LEFT, RIGHT, CROSS)
SELECT u.name, o.total 
FROM users u 
INNER JOIN orders o ON u.id = o.user_id;

SELECT u.name, o.total 
FROM users u 
LEFT JOIN orders o ON u.id = o.user_id;

-- Aliases
SELECT u.name AS user_name, o.total AS order_total
FROM users u JOIN orders o ON u.id = o.user_id;

-- Subqueries
SELECT name FROM users WHERE id IN (SELECT user_id FROM orders);
SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM banned);
SELECT * FROM users WHERE score > (SELECT AVG(score) FROM users);

-- EXISTS / NOT EXISTS
SELECT name FROM users u
  WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);
SELECT name FROM users u
  WHERE NOT EXISTS (SELECT 1 FROM banned b WHERE b.user_id = u.id);

-- Common Table Expressions (CTE)
WITH high_scorers AS (
  SELECT * FROM users WHERE score > 90
)
SELECT name FROM high_scorers ORDER BY score DESC;

-- UNION / INTERSECT / EXCEPT
SELECT name FROM staff UNION ALL SELECT name FROM contractors;
SELECT name FROM staff UNION SELECT name FROM contractors;  -- dedup
SELECT id FROM a INTERSECT SELECT id FROM b;
SELECT id FROM a EXCEPT SELECT id FROM b;

-- CASE WHEN
SELECT name, 
  CASE WHEN score >= 90 THEN 'A' 
       WHEN score >= 70 THEN 'B' 
       ELSE 'F' END AS grade
FROM students;

-- CAST (type conversion)
SELECT CAST('42' AS INTEGER);
SELECT CAST(score AS TEXT) FROM users;
SELECT CAST(1 AS BOOLEAN);

-- String concatenation operator
SELECT first_name || ' ' || last_name AS full_name FROM users;

-- Built-in functions
SELECT UPPER(name), LOWER(email) FROM users;
SELECT LENGTH(name), CONCAT(first, ' ', last) FROM users;
SELECT SUBSTRING(name, 1, 3) FROM users;
SELECT REPLACE(name, 'old', 'new') FROM users;
SELECT TRIM(name) FROM users;
SELECT ABS(balance), ROUND(score, 2) FROM accounts;
SELECT COALESCE(nickname, name, 'anonymous') FROM users;
SELECT NULLIF(score, 0) FROM users;

-- COUNT(DISTINCT) and bare aggregates (no GROUP BY needed)
SELECT COUNT(DISTINCT category) FROM products;
SELECT SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders;

-- Window functions
SELECT name, score,
  ROW_NUMBER() OVER (ORDER BY score DESC) AS row_num,
  RANK() OVER (ORDER BY score DESC) AS rnk,
  DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rnk
FROM users;

-- Window functions with PARTITION BY
SELECT dept, name, salary,
  ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rank
FROM employees;

-- LAG / LEAD
SELECT date, revenue,
  LAG(revenue) OVER (ORDER BY date) AS prev_revenue,
  LEAD(revenue) OVER (ORDER BY date) AS next_revenue
FROM daily_sales;

-- SELECT without FROM
SELECT 1 + 2;
SELECT UPPER('hello');

-- EXPLAIN (query plan)
EXPLAIN SELECT * FROM users WHERE id = 1;

-- Statistics and cost-based join planning
ANALYZE TABLE users COMPUTE STATISTICS;
EXPLAIN SELECT *
FROM users
INNER JOIN orders ON users.id = orders.user_id
INNER JOIN order_items ON orders.id = order_items.order_id;
```

### Transactions

```sql
-- Explicit transactions (via PostgreSQL protocol)
BEGIN;
INSERT INTO accounts VALUES (1, 1000);
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
COMMIT;

-- Rollback on error
BEGIN;
DELETE FROM users WHERE id = 999;
ROLLBACK;
```

> Note: Every HTTP API call auto-wraps in a transaction. For multi-statement transactions, use the PostgreSQL protocol.

### Vector Operations

```sql
-- Insert vectors
INSERT INTO items VALUES (1, [0.1, 0.2, 0.3]);

-- Euclidean distance between two vectors
SELECT VECTOR_DISTANCE(embedding, [0.1, 0.2, 0.3]) AS dist FROM items;

-- Cosine similarity
SELECT COSINE_SIMILARITY(embedding, [0.1, 0.2, 0.3]) AS sim FROM items;

-- AI embedding from text (built-in bag-of-words provider)
SELECT EMBEDDING('hello world machine learning') AS vec;
```

---

## HTTP API Reference

All endpoints are served from `http://127.0.0.1:8091`.

### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check, returns `"OK"` |
| `POST` | `/query` | Execute SQL, returns JSON result |
| `POST` | `/prepare` | Register a prepared statement |
| `POST` | `/execute` | Execute prepared statement with params |
| `GET` | `/tables` | List all tables with schema |
| `GET` | `/metrics` | Internal performance counters (JSON) |
| `GET` | `/metrics/prometheus` | Prometheus-compatible metrics (OpenMetrics text) |
| `GET` | `/slow_queries` | Recent slow queries (JSON array) |
| `POST` | `/checkpoint` | Force SSTable flush / snapshot |
| `POST` | `/compact` | Run manual FusionStorage compaction (`fusiondb-cli vacuum`) |
| `GET` | `/cdc/events?since=N&limit=M` | Read committed CDC events for FusionStorage |
| `GET` | `/capabilities` | Show backend and feature capabilities |
| `POST` | `/vector_search` | Direct vector search (bypass SQL) |
| `POST` | `/hybrid_search` | Combined text + vector search |
| `GET` | `/raft/shards` | Show configured shard map when distributed sharding is enabled |
| `POST` | `/raft/shards/route` | Route a `{table,key}` pair to its shard owner |

### Request / Response Formats

**POST /query**
```json
// Request
{"sql": "SELECT name, score FROM users WHERE score > 90"}

// Response (SELECT)
{
  "result": [{
    "columns": ["name", "score"],
    "rows": [["Alice", 95.5], ["Carol", 92.1]]
  }],
  "error": null
}

// Response (INSERT/UPDATE/DELETE)
{
  "result": [{"message": "Inserted 3 rows"}],
  "error": null
}
```

**POST /vector_search**
```json
// Request
{"query": [0.1, 0.2, 0.3], "limit": 5}

// Response
{"results": [{"id": "item_1", "distance": 0.05}, ...]}
```

**POST /hybrid_search**
```json
// Request
{"text_query": "machine learning", "vector_query": [0.1, 0.2, 0.3], "limit": 5}

// Response
{"results": [{"id": "doc_42", "distance": 0.87}, ...]}
```

**GET /metrics**
```json
{
  "sql_parse_count": 1024,
  "sql_plan_count": 512,
  "row_read_count": 50000,
  "row_cache_hit_count": 35000,
  "row_write_count": 10000,
  "fts_search_count": 100,
  "fts_doc_hits": 5000,
  "wal_write_count": 10000,
  "wal_write_bytes": 2048000,
  "query_count": 5000,
  "slow_query_count": 12,
  "query_total_us": 320000000,
  "pg_active_connection_count": 24,
  "pg_connection_rejected_count": 3,
  "pg_connection_limit": 100
}
```

**GET /slow_queries**
```json
[
  {"sql": "SELECT * FROM big_table WHERE ...", "duration_ms": 245.3, "timestamp": "1740300000.123"},
  {"sql": "SELECT COUNT(*) FROM ...", "duration_ms": 102.1, "timestamp": "1740300001.456"}
]
```

**GET /metrics/prometheus**
```
# HELP fusiondb_query_count Total queries executed
# TYPE fusiondb_query_count counter
fusiondb_query_count 5000
# HELP fusiondb_slow_query_count Queries exceeding slow threshold
# TYPE fusiondb_slow_query_count counter
fusiondb_slow_query_count 12
...

```

**GET /cdc/events?since=0&limit=100**
```json
{
  "status": "ok",
  "data": {
    "events": [
      {
        "sequence": 1048576,
        "commit_ts": 1,
        "operation": "put",
        "key": { "encoding": "utf8", "data": "data:orders:0001" },
        "value": { "encoding": "utf8", "data": "..." }
      }
    ],
    "next_since": 1048576,
    "latest_sequence": 1048576
  },
  "error": null
}
```

CDC is available on `FusionStorage` and records committed storage writes with a monotonic `sequence` for resumable polling. Registered users must be superusers to read this feed; anonymous and `postgres` remain legacy superusers for local compatibility.

---

## AI-Native Functions

FusionDB has a built-in embedding provider architecture (`EmbeddingRegistry`).

### Built-in Provider: `builtin-bow`

Default 128-dimension bag-of-words embedding. Tokenizes text, hashes tokens to dimensions, L2-normalizes.

```sql
-- Generate embedding from text
SELECT EMBEDDING('rust programming language') AS vec;
-- Returns: VECTOR(128) — a 128-dimensional float32 vector

-- Semantic similarity between two texts
SELECT COSINE_SIMILARITY(
    EMBEDDING('database performance'),
    EMBEDDING('query optimization speed')
) AS similarity;

-- Store auto-generated embeddings
INSERT INTO docs (id, content, vec) 
VALUES (1, 'hello world', EMBEDDING('hello world'));
```

### Custom Provider (Rust API)

```rust
use fusiondb::ai::embedding::{EmbeddingProvider, EmbeddingRegistry};

struct OnnxProvider { /* ... */ }
impl EmbeddingProvider for OnnxProvider {
    fn embed(&self, text: &str) -> Vec<f32> { /* ONNX inference */ }
    fn dimension(&self) -> usize { 384 }
    fn name(&self) -> &str { "onnx-minilm" }
}

// Register
let registry = EmbeddingRegistry::new();
registry.register(Arc::new(OnnxProvider::new("model.onnx")));
registry.set_default("onnx-minilm");
```

---

## Storage Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Protocol Layer                             │
│   pgwire (PostgreSQL wire protocol)  |  HTTP/JSON (Axum)     │
│   Port 8092                          |  Port 8091            │
├──────────────────────────────────────────────────────────────┤
│                    Execution Layer                            │
│   SQL Parser (sqlparser) → Query Optimizer → Executor        │
│   ┌─────────┬──────────┬───────┬──────────┬──────────┐      │
│   │  DDL    │   DML    │ Query │  Expr    │  Agg     │      │
│   │ CREATE  │ INSERT   │ JOIN  │ EMBED()  │ GROUP BY │      │
│   │ DROP    │ UPDATE   │ SCAN  │ COSINE() │ HAVING   │      │
│   │ INDEX   │ DELETE   │ SORT  │ DIST()   │ COUNT..  │      │
│   └─────────┴──────────┴───────┴──────────┴──────────┘      │
├──────────────────────────────────────────────────────────────┤
│                    Storage Layer (FusionStorage)              │
│                                                              │
│  Write Path:                                                 │
│    Client → WAL (append-only) → MemTable (lock-free SkipMap) │
│    MemTable full (32MB) → Flush to SSTable → WAL truncate    │
│                                                              │
│  Read Path (MVCC):                                           │
│    Row Cache (Moka, 10K entries)                             │
│      → MemTable (current + immutable)                        │
│        → SSTables (Bloom filter → binary search)             │
│                                                              │
│  Indexes:                BTree         (secondary columns)   │
│                          FB+-Tree      (in-MemTable range)   │
│                          HNSW          (vector ANN search)   │
│                          Inverted+BM25 (full-text search)    │
│                          Trigram       (fuzzy text matching)  │
│                          Bloom Filter  (SSTable membership)  │
│                                                              │
│  Transaction:  OCC + Snapshot Isolation + Write Conflict Det │
├──────────────────────────────────────────────────────────────┤
│               Distributed Layer (OpenRaft)                    │
│   Leader Election | Log Replication | Membership Change      │
│   HTTP inter-node RPC (append/vote/snapshot)                 │
├──────────────────────────────────────────────────────────────┤
│               AI Layer                                        │
│   EmbeddingRegistry → BuiltinBOW (128-dim)                  │
│   Pluggable: ONNX Runtime, OpenAI API, etc.                 │
├──────────────────────────────────────────────────────────────┤
│               Analytics Layer                                 │
│   Arrow RecordBatch | Vectorized Aggregation (SIMD)          │
│   Columnar Vector Store (brute-force + HNSW)                 │
└──────────────────────────────────────────────────────────────┘
```

---

## Configuration

FusionDB uses a TOML configuration file (`fusiondb.toml`). Generate a default one with:

```bash
./target/release/fusiondb --init
```

### fusiondb.toml

```toml
[server]
http_port = 8091          # HTTP JSON API port
pg_port = 8092            # PostgreSQL wire protocol port
redis_enabled = false     # Optional Redis-compatible RESP endpoint for native memtier probes
redis_port = 6379         # Redis-compatible RESP endpoint port
bind = "127.0.0.1"        # Bind address (use "0.0.0.0" for all interfaces)
max_connections = 100     # Max concurrent PostgreSQL wire protocol connections

[storage]
data_dir = "data"          # Base data directory for all persistent files
wal_file = "fusion.wal"    # WAL file name (relative to data_dir)
sstable_dir = "sstables"   # SSTable directory (relative to data_dir)
memtable_flush_mb = 32     # MemTable size before flush to SSTable (MB)
row_cache_capacity = 10000 # Row cache entries (LRU)
statement_cache_capacity = 1000  # Prepared statement cache size
block_cache_capacity = 25000     # SSTable block cache (4KB blocks)
slow_query_threshold_ms = 100    # Slow query log threshold (ms)

[auth]
password = "fusiondb"      # Password for PostgreSQL cleartext auth

[distributed]
enabled = false            # Enable OpenRaft-backed distributed mode
node_id = 1                # Local Raft node id
advertise_addr = ""        # Peer-facing address; empty uses server bind/http_port
bootstrap = true           # Initialize configured members on startup
cluster_name = "fusiondb"  # OpenRaft cluster name
initial_members = []       # Optional [{ node_id = 1, addr = "127.0.0.1:8091" }]

[distributed.sharding]
enabled = false            # Enable shard map, routing metadata, and local shard key layout
strategy = "hash"          # "hash" or "range"
shard_count = 16           # Hash shard count; range uses boundaries + 1
range_boundaries = []      # Optional lexicographic range upper bounds
```

### Ports Summary

| Service | Default Port | Protocol | Description |
|---|---|---|---|
| HTTP API | `8091` | HTTP/JSON | REST API, metrics, health check |
| PostgreSQL | `8092` | pgwire | Standard PostgreSQL wire protocol |
| Redis-compatible | `6379` | RESP | Optional endpoint for `memtier_benchmark --protocol=redis` (`PING`, `ECHO`, `SELECT 0`, `INFO`, `SET`, `SETEX`, `GET`, `MGET`, `MSET`, `EXISTS`, `DEL`, `INCR`, `QUIT`) |

### Authentication

Default credentials for PostgreSQL protocol:
- **User**: any (not enforced)
- **Password**: `fusiondb` (configurable in `fusiondb.toml`)

### Data Directory Layout

```
data/
├── fusion.wal            # Active WAL segment
├── fusion.wal.seg.1      # Rotated WAL segments (64MB each)
├── fusion.wal.seg.2
└── sstables/
    ├── 1.sst             # SSTable files (with CRC32 checksums)
    ├── 2.sst
    └── ...
```

---

## Benchmark

Run the built-in benchmark suite:

```bash
cargo run --release --bin fusiondb &   # Start server
python benchmark.py                     # Run benchmark
```

Environment variables:
```bash
FUSIONDB_URL=http://127.0.0.1:8091/query  # Server endpoint
BENCH_ROWS=10000                            # Data size
BENCH_ITERS=10                              # Iterations per query
```

### Sample Results (10K rows, debug build, Windows)

| Category | Query | Avg Latency | Ops/sec |
|---|---|---|---|
| **Point Query** | PK Lookup | 6.6 ms | 152 |
| **Point Query** | Index Scan (val=X) | 10.6 ms | 95 |
| **Point Query** | Full Table Scan (val=X) | 38.7 ms | 26 |
| **Range** | BETWEEN | 38.1 ms | 26 |
| **Aggregation** | COUNT(*) | 24.8 ms | 40 |
| **Aggregation** | GROUP BY | 38.7 ms | 26 |
| **JOIN** | INNER JOIN (200×2000) | 23.2 ms | 43 |
| **Write** | Single INSERT | 12.1 ms | 82 |
| **Write** | Single UPDATE | 8.0 ms | 125 |
| **Write** | Bulk INSERT | — | 9,176 rows/sec |
| **Index Speedup** | scan vs index | 3.7x | — |

> Release build (`--release`) typically improves performance 3–10x.

---

## Project Structure

```
FusionDB/
├── src/
│   ├── main.rs                     # Entry point (config loading, graceful shutdown)
│   ├── lib.rs                      # Module declarations
│   ├── bin/
│   │   ├── fusiondb-cli.rs         # Admin CLI for HTTP health/query/ops endpoints
│   │   └── benchmark.rs            # TCP benchmark harness
│   ├── config.rs                   # TOML config file parsing (fusiondb.toml)
│   ├── monitor.rs                  # Metrics, slow query log, Prometheus export
│   ├── ai/
│   │   ├── mod.rs                  # AI module
│   │   └── embedding.rs            # EmbeddingProvider trait, BuiltinBOW, Registry
│   ├── catalog/
│   │   └── mod.rs                  # TableSchema, Column, IndexType definitions
│   ├── common/
│   │   ├── encoding.rs             # Key encoding utilities
│   │   ├── error.rs                # FusionError enum
│   │   └── value.rs                # Value enum (Integer, Float, String, Vector, ...)
│   ├── distributed/
│   │   ├── mod.rs                  # Raft node factory
│   │   ├── typ.rs                  # OpenRaft TypeConfig
│   │   ├── store.rs                # FusionRaftStore (RaftStorage impl)
│   │   ├── network.rs              # HTTP-based RaftNetwork
│   │   └── api.rs                  # Raft HTTP API routes (append/vote/snapshot/write)
│   ├── execution/
│   │   ├── mod.rs                  # Executor: prepare, execute, transaction management
│   │   ├── ddl.rs                  # CREATE/ALTER/DROP TABLE, TRUNCATE, SHOW CREATE TABLE
│   │   ├── dml.rs                  # INSERT, UPDATE, DELETE
│   │   ├── query.rs                # SELECT, JOIN, GROUP BY, UNION, subqueries, CTE, window fns
│   │   ├── expr.rs                 # Expression evaluator (WHERE, functions, operators)
│   │   ├── scan.rs                 # Table scan strategies (full, index, PK)
│   │   └── aggregation.rs          # Aggregate accumulators (COUNT, SUM, AVG, MIN, MAX, COUNT DISTINCT)
│   ├── parser/
│   │   └── mod.rs                  # SQL parser wrapper (sqlparser crate)
│   ├── server/
│   │   ├── mod.rs                  # Server startup orchestration
│   │   ├── http_server.rs          # Axum HTTP API (query, prepare, execute, vector_search)
│   │   ├── pg_server.rs            # pgwire PostgreSQL protocol handler
│   │   └── tcp_server.rs           # Raw TCP server (legacy)
│   └── storage/
│       ├── mod.rs                  # Storage + Transaction traits
│       ├── fusion.rs               # FusionStorage: MVCC + LSM + SkipMap + SSTable
│       ├── memory.rs               # In-memory storage (for testing)
│       ├── backend.rs              # Pluggable backend factory (BackendConfig)
│       ├── wal.rs                  # Segmented WAL (64MB rotation, multi-segment replay)
│       ├── sstable.rs              # SSTable with Bloom filters + CRC32 checksums + LZ4 block compression
│       ├── fbtree.rs               # FB+-Tree (fractal B-tree variant)
│       ├── vector_index.rs         # HNSW vector index
│       ├── inverted_index.rs       # BM25 inverted index for FTS
│       ├── trigram.rs              # Trigram index for fuzzy matching
│       ├── columnar.rs             # Columnar vector store (SIMD-friendly)
│       └── columnar_analytics.rs   # Arrow-based vectorized aggregation
├── tests/
│   ├── sql_integration.rs          # 98 SQL integration tests
│   └── pg_integration.rs           # 4 pgwire protocol tests
├── dashboard/                      # FusionDB Studio (Supabase-style web UI)
│   ├── src/
│   │   ├── components/Layout.tsx   # Sidebar + main layout
│   │   ├── pages/DashboardPage.tsx # Metrics dashboard
│   │   ├── pages/SqlEditorPage.tsx # SQL editor (CodeMirror)
│   │   ├── pages/TableEditorPage.tsx # Table browser + inline editor
│   │   ├── pages/SettingsPage.tsx  # Connection & capabilities
│   │   └── lib/api.ts             # HTTP API client
│   ├── package.json
│   └── vite.config.ts             # Vite + TailwindCSS + API proxy
├── benchmark.py                    # Comprehensive performance benchmark
├── Cargo.toml                      # Dependencies
├── Dockerfile                      # Container build
└── LICENSE                         # MIT
```

---

## Current Limitations

These are known gaps that should be addressed before production use:

### SQL Completeness
- No stored procedures / user-defined functions / triggers

### Storage & Reliability
- No online backup / point-in-time recovery
- No disk space reclamation after DELETE (tombstones persist until compaction)
- No configurable compression algorithm/tuning yet; SSTable blocks use LZ4 when the encoded block is smaller

### Transactions
- OCC may have high abort rates under write-heavy contention
- No `SERIALIZABLE` isolation (only Snapshot Isolation)
- No savepoints (`SAVEPOINT` / `RELEASE`)

### Distributed
- OpenRaft can be enabled via `[distributed]`, with `/raft/*` HTTP RPCs, leader-forwarded writes, and local follower reads
- Raft log/state metadata is currently in-memory and intended as the control-plane wiring foundation
- Snapshot transfer serializes visible key-value state for new node bootstrap
- Sharding has a configurable hash/range control plane, route API, local row-data shard key layout (`shard:{id}:data:{table}:{row_id}`), and local secondary-index KV shard layouts (`shard:{id}:index:*`, `shard:{id}:fts:*`)
- HTTP and pgwire SQL execution now reject deterministic non-local shard-owner point writes (`INSERT ... VALUES` with an explicit primary key, pgwire `COPY FROM STDIN` rows with an explicit primary key, plus `UPDATE`/`DELETE` by primary-key equality) with a route hint instead of silently executing them on the wrong node, including pgwire writes against schemas created earlier in the same session transaction
- HTTP `/query`, HTTP prepared `/execute`, and pgwire simple/extended query plus `COPY FROM STDIN` can forward deterministic point writes whose routed rows all target one non-local shard owner to that owner's HTTP endpoint
- HTTP `/query`, HTTP prepared `/execute`, and pgwire simple/extended query can forward deterministic primary-key point reads to a non-local shard owner
- HTTP `/query`, HTTP prepared `/execute`, and pgwire simple/extended query can fan out simple single-table SELECT scans across shard owners and merge row results, including distributed `COUNT(*)` summation; other aggregates, DISTINCT, ORDER BY/LIMIT, joins, subqueries, and broader distributed planning are still conservative
- Mixed local/remote writes, multi-owner writes, distributed index ownership/maintenance, and broader cross-node query planning remain in progress
- No dedicated read-replica topology management
- No distributed transactions (2PC)

### Security
- Password auth over pgwire is cleartext (SCRAM-SHA-256 blocked by pgwire 0.37)
- TLS infrastructure ready but pgwire 0.37 requires external TLS proxy
- No row-level security

### Operations
- No client-side connection pooling library; pgwire has configurable server-side connection slots and backpressure
- No automatic compaction tuning / maintenance scheduler
- CDC is currently a resumable event feed; distributed streaming replication remains future work

---

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the detailed checklist. Summary:

| Phase | Status | Highlights |
|---|---|---|
| **1. Data Integrity** | ✅ Done | Graceful shutdown, TOML config, segmented WAL, SSTable CRC32, compaction dedup |
| **2. SQL Completeness** | ✅ Done | ALTER TABLE, UNION/INTERSECT/EXCEPT, subqueries, CASE WHEN, TRUNCATE, functions |
| **3. Security** | 🔲 Next | TLS/SSL, SCRAM-SHA-256, RBAC |
| **4. Performance** | ✅ Done | Connection slots, parallel scan, LZ4 SSTable compression, cost-based optimizer |
| **5. Distributed** | 🔲 In progress | OpenRaft main-loop wiring and snapshot transfer; sharding control plane, local row/index shard layouts, and point-write owner guard |
| **6. Operations** | ✅ Done | Slow query log, Prometheus metrics, config file, admin CLI, CDC feed |

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch: `git checkout -b feature/amazing-feature`
3. Run tests: `cargo test`
4. Commit your changes: `git commit -m 'Add amazing feature'`
5. Push and open a Pull Request

### Development Tips

```bash
# Run with logging
RUST_LOG=debug cargo run --bin fusiondb

# Run specific test
cargo test test_insert_and_select -- --nocapture

# Run benchmark with custom size
BENCH_ROWS=50000 python benchmark.py

# Check code without building
cargo check

# Format code
cargo fmt
```

---

## License

Distributed under the MIT License. See `LICENSE` for more information.
