# TASK-090 Execution Context

## Scope

- `src/execution/scan.rs`
- Database core only; `dashboard/` untouched.

## Change

- `distinct_probe_keys` now uses `HashSet::with_capacity(left_rows.len())`.
- `probed_rows` now uses `Vec::with_capacity(min(left_rows.len(), limit))`.
- `probe_cache` now uses `HashMap::with_capacity(distinct_probe_keys.len())`.

## Expected Impact

- Lower allocation churn in indexed join probe execution.
- Join behavior remains unchanged because only container capacities changed.
