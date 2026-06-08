# BENCHPROD-236 INSERT RETURNING Row Preallocation

## Goal

Avoid delayed vector reservation while collecting rows for `INSERT ... VALUES ... RETURNING`.

## Implementation

- `src/execution/dml/insert.rs`
  - Replaced `Vec::new()` plus `reserve(raw_rows.len())` in `insert_raw_values_rows` with conditional `Vec::with_capacity(raw_rows.len())`.
  - Moved the main VALUES-path returned-row buffer initialization to the point where `values.rows.len()` is available and used it for direct capacity.
  - Preserved non-RETURNING behavior, RETURNING row order and contents, UPSERT conflict handling, serial defaults, and insert count reporting.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac test_insert_returning -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_insert_omitted_serial_primary_key_generates_ids -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac`
  - Passed: 14/14.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml`
  - Passed: 43/43.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-236` is complete. `INSERT ... VALUES ... RETURNING` row collection now preallocates returned-row storage from known input row counts.
