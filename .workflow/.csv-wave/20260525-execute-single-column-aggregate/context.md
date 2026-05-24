# Single-column aggregate decoding execution

Executed TASK-005 and TASK-006.

Changes:
- `src/common/encoding.rs`: added `RowDecoder::column_bounds` and `RowDecoder::decode_column`.
- `src/common/encoding.rs`: added `test_row_encoding_single_column`.
- `src/execution/query.rs`: changed the primary-key `MIN/MAX` fast path to decode only the requested column.
- `tests/sql_integration.rs`: added `test_select_min_max_primary_key`.

Dashboard files were not modified.
