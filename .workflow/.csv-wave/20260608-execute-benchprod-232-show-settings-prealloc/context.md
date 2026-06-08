# BENCHPROD-232 SHOW ALL Settings Preallocation

## Goal

Avoid implicit vector growth while constructing fixed `SHOW ALL` settings rows.

## Implementation

- `src/execution/ddl/show.rs`
  - Replaced fixed settings `into_iter().map().collect()` with `Vec::with_capacity(settings.len())` and explicit row pushes.
  - Preserved `SHOW ALL` column names, setting row order, and setting values.
- `tests/sql_ddl.rs`
  - Added `test_show_all_returns_settings_rows` to cover executor-level `SHOW ALL` output.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl test_show_all_returns_settings_rows -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl`
  - Passed: 29/29.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-232` is complete. `SHOW ALL` settings result construction now preallocates row storage from the fixed settings count.
