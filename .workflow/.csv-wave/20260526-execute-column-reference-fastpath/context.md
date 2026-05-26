# TASK-121 Execution

Target: `src/execution/scan.rs`

Change:
- Replaced the temporary `HashSet<String>` in `expr_has_column_reference`.
- Added direct recursive boolean traversal over supported expression shapes.
- Short-circuits as soon as an identifier or compound identifier is found.

Behavior:
- Predicate eligibility for point lookups, range lookups, and commuted predicates is unchanged.
- The optimization only removes allocation when the caller needs an existence check.
