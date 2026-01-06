# FusionDB 🚀

**FusionDB** is a high-performance, multimodal, ACID-compliant database written in Rust. It is designed to be "out-of-the-box" ready (like Redis) while offering powerful SQL capabilities, vector search, and full-text search in a single engine.

![CI Status](https://github.com/cheny/w33dDB/actions/workflows/ci.yml/badge.svg)
![Release](https://img.shields.io/github/v/release/cheny/w33dDB)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

## 🌟 Key Features

*   **Multimodal Storage**:
    *   **SQL**: Full support for standard SQL queries (SELECT, INSERT, UPDATE, DELETE, JOIN, GROUP BY).
    *   **Vector Search**: Built-in HNSW index for high-speed approximate nearest neighbor search (ANN).
    *   **Full-Text Search**: BM25 inverted index for powerful text search capabilities.
    *   **Hybrid Search**: Combine Vector and Text search using Reciprocal Rank Fusion (RRF).
*   **High Performance**:
    *   **FusionStorage Engine**: A custom storage engine combining MVCC (Multi-Version Concurrency Control) and LSM-Tree (Log-Structured Merge Tree).
    *   **Lock-Free**: Uses lock-free SkipLists for high concurrency.
    *   **SIMD Accelerated**: Vector operations are optimized using SIMD instructions.
*   **Reliability**:
    *   **ACID Compliant**: Full transaction support with Snapshot Isolation.
    *   **Durability**: WAL (Write-Ahead Logging) and automatic crash recovery.
*   **Easy to Use**:
    *   **PostgreSQL Protocol**: Compatible with existing Postgres clients and drivers.
    *   **Zero Configuration**: Starts up with sensible defaults.

## 🚀 Quick Start

### Option 1: Docker (Recommended)

The easiest way to run FusionDB is using Docker:

```bash
docker run -d -p 5432:5432 -v fusion_data:/data --name fusiondb fusiondb:latest
```

### Option 2: Pre-built Binaries

Download the latest release for your platform from the [Releases Page](https://github.com/cheny/w33dDB/releases).

**Linux/macOS:**
```bash
./fusiondb
```

**Windows:**
```powershell
.\fusiondb.exe
```

The server will start listening on port `5432` (Postgres protocol) and `8080` (HTTP API).

### Option 3: Build from Source

Requirements: Rust (latest stable).

```bash
git clone https://github.com/cheny/w33dDB.git
cd w33dDB
cargo build --release
./target/release/fusiondb
```

## 🔌 Connecting

You can connect using any PostgreSQL client (psql, DBeaver, etc.):

```bash
psql -h localhost -p 5432 -U admin -d default
```

## 📖 Usage Examples

### 1. Vector Search (AI/ML)

FusionDB treats vectors as first-class citizens.

```sql
-- Create a table with a vector column (3 dimensions)
CREATE TABLE items (
    id INT, 
    embedding VECTOR(3)
);

-- Insert data
INSERT INTO items VALUES (1, [0.1, 0.2, 0.3]);
INSERT INTO items VALUES (2, [0.4, 0.5, 0.6]);

-- Create HNSW Index for fast search
CREATE INDEX idx_vec ON items (embedding) USING HNSW;

-- Search for nearest neighbors
SELECT id, embedding 
FROM items 
ORDER BY embedding <-> [0.1, 0.2, 0.3] 
LIMIT 5;
```

### 2. Full-Text Search

```sql
-- Create table
CREATE TABLE docs (
    id INT, 
    content TEXT
);

-- Insert documents
INSERT INTO docs VALUES (1, 'FusionDB is fast');
INSERT INTO docs VALUES (2, 'Rust is safe');

-- Create Inverted Index
CREATE INDEX idx_text ON docs (content) USING FTS;

-- Search using MATCH ... AGAINST
SELECT * FROM docs WHERE MATCH(content) AGAINST ('FusionDB');
```

### 3. Hybrid Search (SQL + Vector)

Combine standard filtering with vector similarity:

```sql
SELECT * FROM products 
WHERE category = 'electronics' 
ORDER BY description_embedding <-> [0.1, ...] 
LIMIT 10;
```

## 🏗 Architecture

FusionDB uses a layered architecture:

1.  **Protocol Layer**: Handles PostgreSQL wire protocol (pgwire).
2.  **Execution Layer**: Query planning, optimization, and execution (Volcano model).
3.  **Storage Layer (FusionStorage)**:
    *   **MemTable**: Lock-free SkipList for fast writes.
    *   **WAL**: Write-Ahead Log for durability.
    *   **SSTable**: Persistent on-disk storage (LSM-Tree).
    *   **VectorIndex**: HNSW graph for vectors.
    *   **InvertedIndex**: BM25 index for text.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1.  Fork the repository.
2.  Create your feature branch (`git checkout -b feature/AmazingFeature`).
3.  Commit your changes (`git commit -m 'Add some AmazingFeature'`).
4.  Push to the branch (`git push origin feature/AmazingFeature`).
5.  Open a Pull Request.

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.
