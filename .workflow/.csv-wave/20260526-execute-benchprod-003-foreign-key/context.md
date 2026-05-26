# BENCHPROD-003 Foreign Key Execution

Date: 2026-05-26
Goal: Improve production benchmark schema compatibility for TPC-C and CH-benCHmark.

## Completed

- Added single-column `FOREIGN KEY` metadata stored outside `TableSchema` in `fk_meta:` records.
- Supports column-level `REFERENCES parent(col)` and table-level `CONSTRAINT name FOREIGN KEY (col) REFERENCES parent(col)`.
- Validates child `INSERT`, `INSERT ... SELECT`, `UPDATE`, and `UPSERT DO UPDATE` against parent rows.
- Blocks parent row `DELETE` and key-changing `UPDATE` while children reference the old value.
- Blocks dropping/renaming FK columns and dropping parent tables while referenced.

## Deferred

- Composite foreign keys.
- `ON DELETE` / `ON UPDATE` cascade, set null, set default.
- Deferrable constraints and transaction-end validation.
- Indexed child reference probes for high-cardinality parent delete/update checks.

## Verified

- `cargo fmt --check`
- `cargo check --lib`
- `cargo test --test sql_view_show_constraints`
- `cargo test --test sql_dml`
- `cargo test --test sql_ddl`
- `cargo test --test sql_index_cache`
- `cargo test --test sql_returning_upsert_vector_rbac`

## Next TASK Queue

- `BENCHPROD-004`: `ANALYZE` statistics skeleton for optimizer and join planning.
- `BENCHPROD-002`: COPY/import compatibility for pgbench/BenchBase/TPC-C loading.
- `BENCHPROD-005`: Real benchmark harness scaffold with capability report and skip reasons.
