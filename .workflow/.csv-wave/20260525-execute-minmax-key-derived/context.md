# MIN/MAX primary-key key-derived execution

Executed TASK-013 and TASK-014.

Changes:
- `src/execution/query.rs`: added `primary_key_value_from_data_key`.
- `src/execution/query.rs`: changed no-filter `MIN/MAX(pk)` to derive the returned primary-key value from the boundary data key instead of decoding the row value.
- `tests/sql_integration.rs`: extended `test_select_min_max_primary_key` with a negative integer primary key.

Dashboard files were not modified.
