# BENCHPROD-229 ANALYZE Statistics Preallocation

## Goal

Avoid implicit vector growth while collecting and finalizing `ANALYZE TABLE` column statistics.

## Implementation

- `src/execution/analyze.rs`
  - Replaced collector `iter().map().collect()` with `Vec::with_capacity(schema.columns.len())`.
  - Replaced finalized stats `into_iter().map().collect()` with `Vec::with_capacity(collectors.len())`.
  - Preserved row scanning, row-cache reuse, per-column observation, finalized stats content, and EXPLAIN stats consumption.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl test_analyze_table_collects_statistics -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl test_explain_includes_analyze_statistics -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl test_explain_join_order_includes_analyze_estimates -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl`
  - Passed: 28/28.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-229` is complete. `ANALYZE TABLE` statistics collection now preallocates collector and finalized stats vectors from known column counts.
