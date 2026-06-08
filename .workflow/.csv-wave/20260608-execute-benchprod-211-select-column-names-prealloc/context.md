# BENCHPROD-211 SELECT Column Name Preallocation

## Goal

Avoid implicit vector growth while SELECT result metadata builds wildcard column-name lists from schema columns.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced plain wildcard column-name `collect()` with `Vec::with_capacity(schema.columns.len())`.
  - Applied the same preallocation to qualified wildcard projection handling.
  - Preserved result column order, `COUNT(*)` detection, and projection label behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_select test_select_all -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_select test_select_projection -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_select`
  - Passed: 27/27.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-211` is complete. SELECT wildcard result column-name construction now preallocates vectors from schema column counts.
