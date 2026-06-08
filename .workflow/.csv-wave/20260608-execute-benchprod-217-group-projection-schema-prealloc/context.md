# BENCHPROD-217 Group Projection Schema Preallocation

## Goal

Avoid implicit vector growth while the simple projected group aggregate fast path builds a temporary result schema from known projection output columns.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced temporary schema column `collect()` with `Vec::with_capacity(simple_columns.len())`.
  - Preserved projected group aggregate rows, aliases, output column order, and temporary schema metadata.
  - Kept the later `columns = simple_columns` handoff unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_group_by_projection_can_coalesce_aggregate -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_group_by_projection_can_materialize_correlated_scalar_array_subquery -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_projection_alias_and_ordinal_expressions -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_projection_scalar_function_from_group_columns -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery`
  - Passed: 48/48.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-217` is complete. Simple projected group aggregate temporary schema construction now preallocates schema column vectors from known projection output counts.
