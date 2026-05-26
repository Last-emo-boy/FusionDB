# TASK-131 Execution Context

## Scope

- `src/execution/query.rs`
- `tests/sql_integration.rs`

## Implementation

Added a strict fast path for `SELECT column, COUNT(*) FROM table GROUP BY column`.

The fast path:

- requires a single base table and no `WHERE`, `JOIN`, `HAVING`, `ORDER BY`, `LIMIT`, or `OFFSET`;
- accepts a grouped identifier or alias plus `COUNT(*)`;
- decodes only the grouping column with `RowDecoder::decode_column`;
- counts groups directly in `HashMap<Value, i64>`.

Generic `GROUP BY` remains responsible for grouped `SUM`, `AVG`, `HAVING`, sorted output, and joined inputs.

## Test Coverage

Added `test_group_by_count_fast_path_preserves_null_and_alias` to verify alias output and `NULL` as its own group.
