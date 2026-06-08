# BENCHPROD-221 IN-Subquery List Preallocation

## Goal

Avoid implicit vector growth while materializing `IN (subquery)` results into SQL expression lists.

## Implementation

- `src/execution/expr/subquery.rs`
  - Replaced `filter_map().map().collect()` with `Vec::with_capacity(rows.len())`.
  - Preserved skipping empty result rows.
  - Preserved conversion of only each result row's first value into a SQL expression.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_subquery_in -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_subquery_not_in -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery`
  - Passed: 48/48.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-221` is complete. IN-subquery materialization now preallocates expression lists from known result row counts.
