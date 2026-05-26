# TASK-130 Execution Context

## Scope

- `src/execution/query.rs`
- `tests/sql_integration.rs`

## Implementation

Added a direct scan fast path for simple `SELECT DISTINCT column FROM table` queries.

The fast path applies only when the query has:

- one base table;
- one projected identifier or compound identifier, optionally aliased;
- no `WHERE`, `JOIN`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, or `OFFSET`.

It decodes only the target column with `RowDecoder::decode_column`, deduplicates directly with `HashSet<Value>`, and returns one-column result rows.

## Test Coverage

Added `test_select_distinct_fast_path_preserves_null_and_alias` to verify alias output and `NULL` preservation as a distinct value.
