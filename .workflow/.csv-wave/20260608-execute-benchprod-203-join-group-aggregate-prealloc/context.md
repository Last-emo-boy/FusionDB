# BENCHPROD-203 Join GROUP BY Aggregate Preallocation

## Goal

Avoid implicit vector growth while the join GROUP BY aggregate fast path initializes per-group accumulators and materializes result rows.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced join aggregate accumulator `collect()` with a vector preallocated from `aggregate_plans.len()`.
  - Replaced final join group result row `collect()` with a vector preallocated from `groups.len()`.
  - Preserved join key matching, accumulator update order, aggregate finalization, and output row shape.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_join_group_by_count_sum_fast_shape -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_join_group_by_aggregate_fast_path_order_limit_offset -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_join_group_by_aggregate_fast_path_matches_chbench_shape -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-203` is complete. Join GROUP BY aggregate execution now preallocates accumulator and result row vectors where required capacities are known.
