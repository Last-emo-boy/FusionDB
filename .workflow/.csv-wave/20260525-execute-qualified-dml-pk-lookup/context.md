# Execute Context

Execution status: completed.

Changes:
- `primary_key_row_id_from_eq_selection` now recognizes `table.id` and alias-qualified primary-key references.
- DML passes the target table name and alias as allowed qualifiers before using the point lookup fast path.
- Regression tests cover qualified `DELETE` without row decoding and qualified `UPDATE` avoiding full-scan decode of non-target rows.
