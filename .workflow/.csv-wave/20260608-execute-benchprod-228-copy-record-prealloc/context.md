# BENCHPROD-228 COPY Record Row Preallocation

## Goal

Avoid implicit vector growth while converting each parsed COPY CSV record into a `Vec<Value>`.

## Implementation

- `src/execution/copy.rs`
  - Replaced CSV record `iter().map().collect()` with `Vec::with_capacity(record.len())` and explicit field conversion.
  - Preserved CSV record order, null-marker handling, `copy_field_to_value` conversion, and outer row accumulation behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_copy_from_csv_with_header_and_index_lookup -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_copy_from_csv_with_column_list_and_defaults -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_copy_from_csv_accepts_quoted_table_and_columns -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml`
  - Passed: 43/43.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-228` is complete. COPY CSV record conversion now preallocates row value vectors from known record field counts.
