# TASK-085 Execution Context

## Scope

- `src/execution/scan.rs`
- Database core only; `dashboard/` untouched.

## Change

- `fetch_rows_by_join_key` now preallocates `seen_row_ids` and `rows` with `index_entries.len()`.

## Expected Impact

- Lower allocation and rehash cost in indexed join probe fetches.
- Query behavior remains unchanged because only container capacities changed.
