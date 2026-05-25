# TASK-120 Execution

Target: `src/execution/scan.rs`

Change:
- Added `conjunctive_predicate_count` to count `AND` leaf predicates.
- Added `collect_conjunctive_predicates` to allocate the predicate vector with that count.
- Reused the helper in JOIN key extraction, JOIN ON splitting, and selection predicate splitting.

Behavior:
- Predicate split order remains left-to-right.
- Predicate classification, filtering, and JOIN planning behavior are unchanged.
- The optimization only avoids repeated Vec growth for multi-predicate expressions.
