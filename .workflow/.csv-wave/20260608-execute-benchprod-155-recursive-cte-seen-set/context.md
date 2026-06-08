# BENCHPROD-155 Recursive CTE Seen Set

## Goal

Avoid repeated linear scans while recursive `UNION` checks whether a candidate row has already been emitted.

## Implementation

- `src/execution/query/mod.rs`
  - Builds a `HashSet<Vec<Value>>` from the deduplicated anchor rows for non-`UNION ALL` recursive CTEs.
  - Uses HashSet insertion to decide whether each recursive candidate row is new.
  - Leaves `UNION ALL` behavior unchanged.
- `tests/sql_set_subquery.rs`
  - Added coverage where the recursive term deliberately emits an already-seen row at the fixpoint.

## Verification

- `cargo test --test sql_set_subquery test_with_recursive_union_skips_seen_recursive_rows -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 40/40.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-155` is complete. Recursive `UNION` duplicate detection now uses a hash lookup path instead of scanning the growing accumulated result set for every candidate row.
