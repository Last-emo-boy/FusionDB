# DELETE primary-key point lookup execution

Executed TASK-015 and TASK-016.

Changes:
- `src/execution/dml.rs`: added `primary_key_row_id_from_eq_selection`.
- `src/execution/dml.rs`: refactored UPDATE to use the shared helper.
- `src/execution/dml.rs`: changed DELETE to use point lookup for first-column primary-key equality predicates.
- `tests/sql_integration.rs`: added a secondary-index cleanup regression for primary-key DELETE.

Dashboard files were not modified.
