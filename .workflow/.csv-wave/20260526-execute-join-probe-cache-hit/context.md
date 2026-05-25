# TASK-082 Execution Context

## Scope

- `src/execution/scan.rs`
- Database core only; `dashboard/` untouched.

## Change

- Added `append_join_probe_matches` to centralize indexed join probe row materialization.
- Changed the indexed join probe cache loop to handle cache hits directly from `HashMap::get`.
- Kept async `fetch_rows_by_join_key` only in the miss path, then inserted fetched candidates into the probe cache.

## Expected Impact

- Repeated left-side probe keys avoid the prior contains/get double lookup pattern.
- Shared materialization reduces duplicated probe logic while preserving residual predicate, LIMIT, and left outer join handling.
