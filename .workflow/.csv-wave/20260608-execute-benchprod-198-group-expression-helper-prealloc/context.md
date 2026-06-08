# BENCHPROD-198 GROUP BY Helper Preallocation

## Goal

Avoid implicit vector growth while GROUP BY helper code builds row value sources, group output scope columns, and resolved projection aliases.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced `collect()` in `compile_group_key_sources` with a vector preallocated from `group_exprs.len()`.
  - Replaced `group_output_scope` column `collect()` with explicit preallocation from `group_exprs.len()`.
  - Replaced `resolve_group_by_projection_aliases` `collect()` with explicit preallocation from `group_exprs.len()`.
  - Preserved alias, ordinal, scalar function, and group expression fallback behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_projection_alias_and_ordinal_expressions -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate test_group_by_projection_scalar_function_from_group_columns -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_join_projection_pushdown_with_group_by -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_group_aggregate`
  - Passed: 50/50.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-198` is complete. GROUP BY helper vectors now use explicit capacities where the group expression count is already known.
