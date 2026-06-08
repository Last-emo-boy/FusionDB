# BENCHPROD-250 EXPLAIN Qualified Name Preallocation

## Goal

Avoid the temporary `Vec<String>` allocation while resolving EXPLAIN compound identifiers such as `table.column`.

## Implementation

- `src/execution/ddl/explain.rs`
  - Replaced identifier segment collection plus `join(".")` with a `String::with_capacity(...)`.
  - Precomputed capacity from identifier segment lengths plus dot separators.
  - Preserved segment order, dot separators, and the existing `resolve_column_index` call.
- `tests/sql_ddl.rs`
  - Added `test_explain_qualified_primary_key_lookup` to verify `table.id = 1` still plans as `Primary Key Lookup`.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl test_explain_qualified_primary_key_lookup -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl -- --nocapture`
  - Passed: 30/30.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-250` is complete. EXPLAIN qualified column names now build directly into a preallocated string, and qualified primary-key EXPLAIN behavior has direct test coverage.
