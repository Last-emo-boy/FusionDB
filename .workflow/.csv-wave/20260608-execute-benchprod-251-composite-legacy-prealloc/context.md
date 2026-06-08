# BENCHPROD-251 Composite Legacy Metadata Preallocation

## Goal

Avoid implicit vector growth while loading composite-index metadata through the legacy `index_meta:` scan fallback.

## Implementation

- `src/execution/composite_index.rs`
  - Replaced `Vec::new()` with `Vec::with_capacity(entries.len())` in `load_composite_indexes_for_table_legacy_scan`.
  - Preserved UTF-8 checks, `index_meta:` prefix stripping, metadata parsing, table filtering, and composite-column filtering.
- `tests/sql_dml.rs`
  - Added `test_composite_index_dml_falls_back_to_legacy_metadata_scan`.
  - The test removes the table metadata directory marker and directory entry while retaining legacy `index_meta:` metadata, then verifies DML maintenance and query lookup still work.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_composite_index_dml_falls_back_to_legacy_metadata_scan -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_composite_index_dml_uses_table_metadata_directory -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml -- --nocapture`
  - Passed: 44/44.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

The full `sql_dml` run printed transient SSTable open retry warnings before passing. The final test result was green.

## Result

`BENCHPROD-251` is complete. Composite index legacy metadata loading now preallocates from the known scan entry count, with direct fallback coverage.
