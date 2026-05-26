# TASK-134 Execution Context

Implemented conservative filtered aggregate direct scan for simple `WHERE` predicates.

Supported shape:
- `SELECT SUM(col), AVG(col) FROM table WHERE predicate_col <op> constant`
- Commuted constant-vs-column comparisons are normalized.
- Supported operators: `=`, `!=`, `>`, `<`, `>=`, `<=`.

Explicitly not optimized:
- `AND`, `OR`, `IN`, `BETWEEN`, `LIKE`, subqueries, column-vs-column predicates
- `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, joins
- aggregate expression arguments
