# BENCHPROD-241 Bare Aggregate Fallback Preallocation

## Goal

Avoid implicit container growth while evaluating bare aggregate fallback projections.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced bare aggregate accumulator `collect()` with `Vec::with_capacity(bare_aggs.len())` and explicit pushes.
  - Replaced aggregate final-value `HashMap` `collect()` with `HashMap::with_capacity(aggregate_plans.len())` and explicit inserts.
  - Replaced final projection result-row `collect::<Result<Vec<_>>>()` with `Vec::with_capacity(select.projection.len())` and explicit pushes.
  - Preserved accumulator initialization order, aggregate finalization, expression mapping, projection order, and error propagation.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_bare_aggregate_sum_multiply_expr -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_bare_aggregate_sum_avg -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-241` is complete. Bare aggregate fallback now preallocates accumulator, aggregate map, and result row buffers from known aggregate and projection counts.
