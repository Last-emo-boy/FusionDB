# BENCHPROD-274 Bare Aggregate Collector Preallocation

## Goal

Avoid repeated growth of collecting accumulator internals for bare aggregate queries with a known input row count.

## Implementation

- `src/execution/aggregation.rs`
  - Added `AGGREGATE_PREALLOC_LIMIT`.
  - Added `AggregateAccumulator::input_capacity_hint`.
  - Added `AggregateAccumulator::with_input_capacity`.
  - `COUNT_DISTINCT`, `ARRAY_AGG`, `STRING_AGG`, and `GROUP_CONCAT` now use bounded collection preallocation when the caller supplies an input row count.
  - Added unit coverage for bounded capacity and collector preallocation.
- `src/execution/query/mod.rs`
  - Bare aggregate execution now initializes accumulators with `AggregateAccumulator::with_input_capacity(name, rows.len())`.
  - Group-by accumulator initialization remains unchanged to avoid overallocating once per group.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test accumulator -- --nocapture`
  - Passed: 10/10.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate count_distinct -- --nocapture`
  - Passed: 5/5.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate bare -- --nocapture`
  - Passed: 10/10.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate -- --nocapture`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-274` is complete. Bare aggregate collectors now preallocate from a bounded input-row capacity hint while preserving existing aggregate semantics.
