# BENCHPROD-215 TSBS Lastpoint Output Preallocation

## Goal

Avoid implicit vector growth in the TSBS `DISTINCT ON` lateral lastpoint fast path while building output column names and ORDER BY sort keys from known counts.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced output schema column-name `collect()` calls with `Vec::with_capacity(output_schema.columns.len())`.
  - Replaced ORDER BY sort key `collect()` with `Vec::with_capacity(order_exprs.len())`.
  - Preserved DISTINCT ON row selection, ORDER BY source resolution, sort direction, trimming, and final column order.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_tsbs_lastpoint_distinct_on_lateral_join -- --nocapture`
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

`BENCHPROD-215` is complete. TSBS lastpoint output metadata and sort key construction now preallocates vectors from known schema and ORDER BY lengths.
