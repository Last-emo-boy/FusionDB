# Execute Context

Execution status: completed.

Changes:
- Normalized `literal < pk`, `literal <= pk`, `literal > pk`, and `literal >= pk` into primary-key range scan operators.
- Rejected range fast paths when the non-key side references another column.
- Extended `EXPLAIN` to report commuted primary-key range scans.
