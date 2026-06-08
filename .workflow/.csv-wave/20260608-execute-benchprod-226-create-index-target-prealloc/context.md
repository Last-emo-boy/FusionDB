# BENCHPROD-226 CREATE INDEX Target Column Preallocation

## Goal

Avoid implicit vector growth while resolving parsed target columns during `CREATE INDEX`.

## Implementation

- `src/execution/ddl/index.rs`
  - Replaced `target_col_indices` `Vec::new()` with `Vec::with_capacity(columns.len())`.
  - Replaced `target_col_names` `Vec::new()` with `Vec::with_capacity(columns.len())`.
  - Preserved simple-column validation, missing-column errors, composite BTree validation, and single-column index type handling.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_show_indexes_reports_composite_columns -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_composite_index_dml_uses_table_metadata_directory -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac test_hnsw_order_by_projection -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache`
  - Passed: 37/37.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-226` is complete. `CREATE INDEX` target-column resolution now preallocates its index and name buffers from the known parsed column count.
