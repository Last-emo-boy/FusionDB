# BENCHPROD-218 Group Aggregate Accumulator Preallocation

## Goal

Avoid implicit vector growth while the generic group aggregate fallback initializes per-group accumulator vectors from known aggregate plan counts.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced per-group aggregate accumulator `collect()` with `Vec::with_capacity(aggregate_plans.len())`.
  - Preserved aggregate plan order and accumulator type selection.
  - Preserved aggregate argument evaluation and accumulator update behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_sum -- --nocapture`
  - Passed: 3 matching tests.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_aggregates_with_multi_predicate_partial_decode -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_aggregate_order_by_limit_offset_topn_window -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-218` is complete. Generic group aggregate fallback now preallocates per-group accumulator vectors from known aggregate plan counts.
