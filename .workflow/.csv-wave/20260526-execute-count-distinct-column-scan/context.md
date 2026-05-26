# TASK-129 Execution Context

## Scope

- `src/execution/query.rs`
- `tests/sql_integration.rs`

## Implementation

Added a bare aggregate fast path for `SELECT COUNT(DISTINCT column) FROM table` when there is no `JOIN`, `WHERE`, or `GROUP BY`.

The executor now:

- resolves the single distinct column against the table schema once;
- scans table rows and decodes only that column with `RowDecoder::decode_column`;
- inserts non-`NULL` values into a `HashSet<Value>`;
- returns the count directly without full-row materialization or generic aggregate evaluation.

## Test Coverage

Added `test_count_distinct_fast_path_ignores_null_with_alias` to verify:

- duplicate values count once;
- `NULL` values do not contribute to `COUNT(DISTINCT ...)`;
- projection alias is preserved.
