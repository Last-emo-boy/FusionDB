# BENCHPROD-208 Insert Default Row Preallocation

## Goal

Avoid implicit vector growth while column-list INSERT paths construct full rows from schema defaults before overlaying provided column values.

## Implementation

- `src/execution/dml/insert.rs`
  - Replaced default full-row `collect()` allocation in prepared raw VALUES row mapping with `Vec::with_capacity(context.schema.columns.len())`.
  - Applied the same preallocation to the inline VALUES insert path using `schema.columns.len()`.
  - Preserved default parsing, type coercion, serial default candidate filtering, mapped value overlay, and validation behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_insert_with_column_list -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_insert_omitted_serial_primary_key_generates_ids -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_copy_from_csv_with_column_list_and_defaults -- --nocapture`
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

`BENCHPROD-208` is complete. Column-list INSERT default row construction now preallocates full-row vectors from schema column counts.
