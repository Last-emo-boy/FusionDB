# Execute Context

Implemented TASK-025.

Changes:
- `src/execution/scan.rs`: primary-key equality lookup now calls `decode_row_for_projection` instead of always decoding the full row.
- `tests/sql_integration.rs`: added regression coverage with a corrupt unused payload column, proving projected lookups skip unused column decoding.

Constraint honored: no `dashboard/` changes.
