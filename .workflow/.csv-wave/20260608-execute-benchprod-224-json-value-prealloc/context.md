# BENCHPROD-224 JSON Value Conversion Preallocation

## Goal

Avoid implicit container growth while converting parsed JSON literals into Fusion `Value` trees.

## Implementation

- `src/execution/expr/value.rs`
  - Replaced JSON array `iter().map().collect()` with `Vec::with_capacity(arr.len())` and explicit recursive pushes.
  - Replaced JSON object `HashMap::new()` with `HashMap::with_capacity(obj.len())`.
  - Preserved recursive conversion, array ordering, and object key/value mapping.
- `tests/sql_expr_functions.rs`
  - Added `test_json_literal_preserves_nested_arrays_and_objects` to cover nested JSON object and array literal conversion.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_expr_functions test_json_literal_preserves_nested_arrays_and_objects -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_expr_functions`
  - Passed: 22/22.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-224` is complete. JSON literal conversion now preallocates array and object containers from known parsed JSON sizes.
