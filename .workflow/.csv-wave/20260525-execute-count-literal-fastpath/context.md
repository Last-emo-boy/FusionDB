# COUNT literal fast path execution

Executed TASK-011 and TASK-012.

Changes:
- `src/execution/query.rs`: imported `DuplicateTreatment`.
- `src/execution/query.rs`: added `count_prefix_eligible_arg`.
- `src/execution/query.rs`: extended the no-filter aggregate fast path to use `count_prefix` for wildcard and non-NULL literal `COUNT` arguments while excluding `DISTINCT`.
- `tests/sql_integration.rs`: added `test_select_count_null_literal` to lock `COUNT(NULL)` semantics.

Dashboard files were not modified.
