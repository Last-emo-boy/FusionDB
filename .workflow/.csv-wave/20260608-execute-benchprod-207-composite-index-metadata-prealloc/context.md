# BENCHPROD-207 Composite Index Metadata Preallocation

## Goal

Avoid implicit vector growth while loading composite index metadata from table-scoped metadata directories and filtering already-loaded metadata down to unique composite indexes.

## Implementation

- `src/execution/composite_index.rs`
  - Preallocated table-directory composite index metadata result vectors from `entries.len()` after `scan_prefix`.
  - Replaced unique composite index `filter().collect()` with an explicitly preallocated vector sized from the loaded index count.
  - Left legacy global metadata scanning unchanged because that path scans cross-table metadata and preallocating from all entries could overallocate.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_create_composite_btree_index_and_lookup -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_composite_index_prefix_scan_skips_nonmatching_row_decode -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_composite_index_range_order_limit_skips_outside_range_decode -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_dml_maintains_composite_index_entries -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_composite_index_dml_uses_table_metadata_directory -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_view_show_constraints test_composite_foreign_key_insert_update_and_parent_checks -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache`
  - Passed: 37/37.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml`
  - Passed: 43/43.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-207` is complete. Composite index metadata loading and unique-index filtering now preallocate result vectors from known source lengths where the capacity bound is local to the table or loaded index set.
