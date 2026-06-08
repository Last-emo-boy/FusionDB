# BENCHPROD-223 Vector Extraction Buffer Preallocation

## Goal

Avoid implicit vector growth while extracting numeric vector function arguments into `Vec<f64>`.

## Implementation

- `src/execution/expr/function.rs`
  - Replaced `Value::Vector` iterator collection with `Vec::with_capacity(vec.len())` and explicit f32-to-f64 conversion.
  - Replaced `Value::Array` `Vec::new()` with `Vec::with_capacity(arr.len())`.
  - Preserved numeric conversion and non-numeric array error behavior.
- `tests/sql_returning_upsert_vector_rbac.rs`
  - Added `test_vector_distance_accepts_numeric_array_literals` to cover the numeric array conversion path.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac test_vector_distance_accepts_numeric_array_literals -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac test_hnsw_order_by_projection -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac`
  - Passed: 14/14.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-223` is complete. Vector function execution now preallocates extraction buffers from known input lengths for both stored vectors and numeric array literals.
