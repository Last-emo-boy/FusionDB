# TASK-135 Execution Context

Extended simple `GROUP BY column` aggregate direct scan to handle grouped-result `ORDER BY` and `LIMIT`.

Supported ORDER BY forms:
- output alias, e.g. `ORDER BY total DESC`
- aggregate expression text, e.g. `ORDER BY COUNT(*) DESC`
- ordinal, e.g. `ORDER BY 2 DESC`

Scope remains restricted to single-table, no `WHERE`, no `JOIN`, no `HAVING`, and simple column aggregate arguments.
