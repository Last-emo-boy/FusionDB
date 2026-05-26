# TASK-132 Execution Context

Implemented a conservative fast path for simple bare `SUM(column)` and `AVG(column)` projections.

Scope:
- `src/execution/query.rs`
- `tests/sql_integration.rs`

Behavior:
- Applies only to single-table aggregate queries without `WHERE`, `JOIN`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, or aggregate `DISTINCT`.
- Decodes only aggregate argument columns with `RowDecoder::decode_column`.
- Preserves alias output names.
- Leaves expression aggregates such as `SUM(quantity * unit_price)` on the generic path.
