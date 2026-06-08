# BENCHPROD-231 Composite Metadata Column Preallocation

## Goal

Avoid implicit vector growth while parsing encoded composite-index metadata columns.

## Implementation

- `src/execution/composite_index.rs`
  - Replaced current v3/u3 metadata column `collect()` with a preallocated parsing loop sized from `columns.matches(',').count() + 1`.
  - Replaced v2 metadata column `collect()` with the same preallocated parsing loop.
  - Preserved trimming, empty-column filtering, empty metadata rejection, and `ordered_encoding` differences between metadata versions.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_create_composite_btree_index_and_lookup -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_show_indexes_reports_composite_columns -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_composite_index_dml_uses_table_metadata_directory -- --nocapture`
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

`BENCHPROD-231` is complete. Composite-index metadata parsing now preallocates parsed column vectors from known encoded comma counts.
