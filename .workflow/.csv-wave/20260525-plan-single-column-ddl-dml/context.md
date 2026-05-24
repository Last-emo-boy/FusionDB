# Single-column DDL/DML decode plan

Goal: continue database-core performance iteration by reusing single-column row decoding where DDL/DML logic only needs one column value.

Scope:
- Include: `src/execution/dml.rs`, `src/execution/ddl.rs`.
- Exclude: `dashboard/`.

Findings:
- `INSERT` UNIQUE constraint checks scan existing rows but only need the candidate unique column value.
- `CREATE INDEX` scans table rows but only needs the indexed column value.
- Both paths decoded full rows, allocating values for columns that were not read.

Plan:
- TASK-017: switch non-primary UNIQUE constraint duplicate checks to `RowDecoder::decode_column`.
- TASK-018: switch `CREATE INDEX` backfill to `RowDecoder::decode_column`.
