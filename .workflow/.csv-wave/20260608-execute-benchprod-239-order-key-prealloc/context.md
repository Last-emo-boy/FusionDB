# BENCHPROD-239 ORDER BY Sort-Key Preallocation

## Goal

Avoid implicit vector growth while constructing `ORDER BY` sort keys from parsed order expressions.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced `ORDER BY` sort-key iterator `collect()` with `Vec::with_capacity(exprs.len())` and explicit pushes.
  - Preserved sort-key order, source resolution, ascending/descending flags, projected/full-schema row handling, limit-window handling, and row sorting semantics.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_aggregate_order_by_limit_offset_topn_window -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_order_by_prefers_projected_column_over_ambiguous_join_input -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_select_order_by_primary_key_limit_offset -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_union_all_order_by_limit_offset -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery`
  - Passed: 48/48.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-239` is complete. `ORDER BY` sort-key construction now preallocates from the known number of parsed order expressions.
