# BENCHPROD-222 Array Expression Element Preallocation

## Goal

Avoid implicit vector growth while converting Fusion array values into SQL array expression elements.

## Implementation

- `src/execution/expr/subquery.rs`
  - Replaced `iter().map().collect()` for `Value::Array` conversion with `Vec::with_capacity(values.len())`.
  - Preserved recursive `fusion_value_to_sql_expr` conversion for each element.
  - Preserved SQL array element order and `named: true` construction.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_array_agg_over_array_expression_preserves_nested_values -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_array_agg_over_generated_subscripts -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_group_by_projection_can_materialize_correlated_scalar_array_subquery -- --nocapture`
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

`BENCHPROD-222` is complete. Array value conversion now preallocates SQL array expression elements from known array lengths while preserving the existing conversion behavior.
