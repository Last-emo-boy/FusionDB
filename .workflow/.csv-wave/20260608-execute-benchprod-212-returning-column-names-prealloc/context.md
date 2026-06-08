# BENCHPROD-212 RETURNING Column Name Preallocation

## Goal

Avoid implicit vector growth while DML `RETURNING *` builds result column names from schema columns.

## Implementation

- `src/execution/dml/returning.rs`
  - Replaced wildcard RETURNING column-name `collect()` with `Vec::with_capacity(schema.columns.len())`.
  - Preserved result row cloning for `RETURNING *`.
  - Preserved expression-specific RETURNING column labels and evaluation behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac test_insert_returning -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac test_update_returning -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac test_delete_returning -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac`
  - Passed: 13/13.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_insert_omitted_serial_primary_key_generates_ids -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-212` is complete. DML `RETURNING *` column-name construction now preallocates vectors from schema column counts.
