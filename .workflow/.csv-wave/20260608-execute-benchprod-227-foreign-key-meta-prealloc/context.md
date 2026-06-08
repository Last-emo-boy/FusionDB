# BENCHPROD-227 Foreign-Key Metadata Preallocation

## Goal

Avoid implicit vector growth while collecting foreign-key metadata during `CREATE TABLE` constraint parsing.

## Implementation

- `src/execution/foreign_key.rs`
  - Replaced `collect_foreign_keys` metadata `Vec::new()` with `Vec::with_capacity(columns.len().saturating_add(constraints.len()))`.
  - Replaced table-level child column collection with `Vec::with_capacity(fk.columns.len())`.
  - Replaced table-level parent column collection with `Vec::with_capacity(fk.referred_columns.len())`.
  - Preserved column-level and table-level FK parsing, default referenced `id` behavior, column-count mismatch errors, and enforcement semantics.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_view_show_constraints test_foreign_key_insert_update_and_parent_delete_checks -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_view_show_constraints test_table_level_foreign_key_constraint -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_view_show_constraints test_composite_foreign_key_insert_update_and_parent_checks -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_view_show_constraints`
  - Passed: 16/16.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_copy_from_csv_enforces_constraints_on_direct_path -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-227` is complete. Foreign-key metadata collection now preallocates metadata and table-level column vectors from known parser counts.
