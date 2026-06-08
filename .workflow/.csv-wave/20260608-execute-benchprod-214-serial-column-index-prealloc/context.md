# BENCHPROD-214 SERIAL Column Index Preallocation

## Goal

Avoid implicit vector growth while INSERT paths discover SERIAL-family columns that need default value generation.

## Implementation

- `src/execution/dml/insert.rs`
  - Replaced `serial_default_candidate_column_indexes` `filter_map().collect()` with `Vec::with_capacity(schema.columns.len())`.
  - Preserved SERIAL type matching across `SERIAL`, `SERIAL2`, `SERIAL4`, `SERIAL8`, `SMALLSERIAL`, and `BIGSERIAL`.
  - Preserved schema column order in the returned candidate index list.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_insert_omitted_serial_primary_key_generates_ids -- --nocapture`
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

`BENCHPROD-214` is complete. INSERT SERIAL default column discovery now preallocates candidate index vectors from schema column counts.
