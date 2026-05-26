# TASK-133 Execution Context

Implemented conservative direct scan for simple `GROUP BY column` aggregate queries.

Scope:
- `src/execution/query.rs`
- `tests/sql_integration.rs`

Supported shape:
- `SELECT group_col, COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col) FROM table GROUP BY group_col`
- Alias on group column and aggregate projections is preserved.

Explicitly not optimized:
- `WHERE`, `JOIN`, `HAVING`, `ORDER BY`, `LIMIT`
- aggregate `DISTINCT`
- aggregate expression arguments like `SUM(quantity * unit_price)`
