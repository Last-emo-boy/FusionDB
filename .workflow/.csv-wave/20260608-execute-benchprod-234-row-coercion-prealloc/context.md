# BENCHPROD-234 Row Coercion Output Preallocation

## Goal

Avoid implicit vector growth while coercing an INSERT row to its table schema.

## Implementation

- `src/execution/types.rs`
  - Replaced `coerce_row_to_schema` iterator `collect()` with `Vec::with_capacity(row.len())` and explicit pushes.
  - Kept the column count mismatch check before allocation and coercion.
  - Preserved column order, null pass-through, per-column type coercion, and error propagation from `coerce_value_to_column_type`.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_insert_select -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_expr_functions test_cast_expressions -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml`
  - Passed: 43/43.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_expr_functions`
  - Passed: 22/22.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-234` is complete. Row schema coercion now preallocates its output vector from the validated input row width.
