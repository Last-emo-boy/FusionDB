# BENCHPROD-201 generate_subscripts Preallocation

## Goal

Avoid implicit vector growth while `generate_subscripts` table-function helpers build output rows and argument expression lists.

## Implementation

- `src/execution/scan/mod.rs`
  - Replaced dimension-1 output row `collect()` with a vector preallocated from the array length.
  - Replaced nested-dimension output row `collect()` with a vector preallocated from the maximum nested array length.
  - Replaced table-function argument expression `collect()` with a vector preallocated from `args.args.len()`.
  - Preserved dimension handling, invalid-argument behavior, and row values.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_generate_subscripts_from_array_literal -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_generate_subscripts_depends_on_left_row_array -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_array_agg_over_generated_subscripts -- --nocapture`
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

`BENCHPROD-201` is complete. `generate_subscripts` helper vectors now use explicit capacities where row and argument counts are known.
