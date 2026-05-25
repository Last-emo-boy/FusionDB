# TASK-087 Execution Context

## Scope

- `src/execution/scan.rs`
- Database core only; `dashboard/` untouched.

## Change

- BTree equality index row id collection uses `HashSet::with_capacity(index_entries.len())`.
- FTS token row id collection uses `HashSet::with_capacity(index_entries.len())`.
- LIKE prefix row id collection reserves scanned candidate capacity before insertion.
- Trigram row key conversion uses `HashSet::with_capacity(row_keys.len())`.

## Expected Impact

- Lower rehash and allocation churn on index-backed row id collection paths.
- Query behavior remains unchanged because only container capacities changed.
