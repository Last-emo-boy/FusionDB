# BENCHPROD-001 Composite Index Execution

Date: 2026-05-26
Goal: Move FusionDB toward production benchmark readiness for TPC-C, TSBS, LDBC, and CH-benCHmark.

## Completed

- Added composite secondary BTree index support for `CREATE INDEX idx ON table (a, b, ...)`.
- Kept persisted table schema bincode stable by storing composite index definitions in `index_meta:` as `v2:<table>:<col1>,<col2>`.
- Added composite index backfill, equality lookup planning, `EXPLAIN`, `SHOW INDEXES`, `DROP INDEX`, and DML maintenance.
- Made `INSERT`, `INSERT ... SELECT`, `UPDATE`, `DELETE`, and `UPSERT DO UPDATE` maintain composite index entries.
- Fixed existing single-column integer BTree backfill to use the same comparable key encoding as incremental maintenance and lookup.

## Benchmark Relevance

- TPC-C and CH-benCHmark need fast equality access on compound business keys such as warehouse/district/customer/order/item dimensions.
- LDBC-style workloads depend heavily on selective multi-column lookup before deeper joins.
- TSBS can use this foundation for tag/value composite predicates before dedicated time-series layout exists.
- memtier remains a protocol-level gap, not a core SQL/storage task.

## Verified

- `cargo check --lib`
- `cargo test --test sql_index_cache`
- `cargo test --test sql_dml`
- `cargo test --test sql_returning_upsert_vector_rbac`
- `cargo test --test sql_ddl`

## Next TASK Queue

- `BENCHPROD-002`: COPY/import compatibility for pgbench/BenchBase/TPC-C loading.
- `BENCHPROD-003`: Foreign key metadata and immediate validation subset.
- `BENCHPROD-004`: `ANALYZE` statistics skeleton for optimizer and join planning.
- `BENCHPROD-005`: Real benchmark harness scaffold with capability report and skip reasons.
