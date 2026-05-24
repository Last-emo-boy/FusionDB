# TASK-049 Execution

Added internal sort-key source structures for ordinary SELECT `ORDER BY` so comparator calls no longer re-scan projection lists for ordinal, alias, and projection expression resolution.

Files changed: `src/execution/query.rs`, `tests/sql_integration.rs`.
