# TASK-063 Set Operation Row Keys

Scope: `src/execution/query.rs`

Implemented:
- Added `deduplicate_rows` using `HashSet<Vec<Value>>`.
- Replaced `format!("{:?}", row)` row keys in `SELECT DISTINCT` and set operation deduplication.
- Replaced `INTERSECT` and `EXCEPT` membership sets with direct `Vec<Value>` keys.

Validation is recorded in `.workflow/.csv-wave/20260525-verify-set-operation-row-keys/verification.json`.
