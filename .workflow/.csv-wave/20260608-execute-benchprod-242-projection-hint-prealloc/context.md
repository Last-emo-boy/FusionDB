# BENCHPROD-242 Projection Hint Preallocation

## Goal

Avoid implicit vector growth while converting extracted projection hint column sets into scan hint vectors.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced the initial projection hint `HashSet` `collect()` with `Vec::with_capacity(cols.len())` and explicit pushes.
  - Replaced the materialized-subquery projection hint `HashSet` `collect()` with the same preallocated conversion.
  - Preserved extracted hint contents, `HashSet` consumption order, deferred subquery handling, filter column inclusion, and scan projection behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_tpcc_order_status_uses_filter_columns_outside_projection -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_join_left_filter_projection_skips_unused_left_column_decode -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_comma_join_reorder_preserves_ldbc_q4_shape_with_deferred_exists -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_count_with_simple_where_streams_only_needed_columns -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-242` is complete. Projection hint vectors now preallocate from the known extracted column count before consuming the hint set.
