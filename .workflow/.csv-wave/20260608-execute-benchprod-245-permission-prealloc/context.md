# BENCHPROD-245 Permission Vector Preallocation

## Goal

Avoid implicit vector growth while constructing authorization permission entries for statements whose permission output length is already known.

## Implementation

- `src/execution/mod.rs`
  - Replaced table-to-permission `collect()` paths with `Vec::with_capacity(...)` and explicit pushes for SELECT and COPY query branches.
  - Preallocated TRUNCATE and DROP permission vectors from their statement name counts.
  - Preallocated CREATE VIEW permissions from the source table count plus the view entry.
  - Added `statement_permissions_preserve_preallocated_entries` to verify output order and operation strings for SELECT JOIN, TRUNCATE, CREATE VIEW, COPY query, and DROP.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test execution::tests::statement_permissions_preserve_preallocated_entries -- --nocapture`
  - Passed: 1/1 target unit test; remaining binaries had 0 matching tests.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac -- --nocapture`
  - Passed: 14/14.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl -- --nocapture`
  - Passed: 29/29.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_view_show_constraints -- --nocapture`
  - Passed: 16/16.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-245` is complete. Authorization permission vectors now preallocate from known statement table counts while preserving existing permission order and operation semantics.
