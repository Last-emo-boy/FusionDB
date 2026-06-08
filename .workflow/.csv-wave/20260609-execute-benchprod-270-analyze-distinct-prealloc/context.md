# BENCHPROD-270 ANALYZE Distinct Collector Preallocation

## Goal

Avoid repeated `HashSet` growth while `ANALYZE` collects per-column distinct statistics.

## Implementation

- `src/execution/analyze.rs`
  - Added `ANALYZE_DISTINCT_PREALLOC_LIMIT`.
  - Added `analyze_distinct_capacity`.
  - `collect_table_stats` derives a bounded capacity hint from `kv_pairs.len()`.
  - `ColumnStatsCollector` now initializes `distinct` with `HashSet::with_capacity`.
  - Added unit coverage for the bounded capacity helper.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test analyze_distinct_capacity_is_bounded -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl test_analyze_table_collects_statistics -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl test_explain_includes_analyze_statistics -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl -- --nocapture`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-270` is complete. ANALYZE distinct collectors now preallocate bounded per-column sets from the scanned row count.
