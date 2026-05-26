# TASK-124 execution

- Target: `src/execution/query.rs`
- Change: single-table, no-WHERE, no-GROUP-BY, no-HAVING, no-DISTINCT `ORDER BY <primary-key> ASC LIMIT/OFFSET` queries now pass a bounded window to scan.
- Safety: query planning confirms the order key is the table primary key before pushdown; `scan_single_table` keeps its existing primary-key ASC guard.
- Test: added `test_select_order_by_primary_key_limit_offset`.
- Constraint: database core only; no `dashboard/` changes.
