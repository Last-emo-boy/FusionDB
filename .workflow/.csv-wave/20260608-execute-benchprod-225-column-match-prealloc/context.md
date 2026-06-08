# BENCHPROD-225 Column Match Preallocation

## Goal

Avoid implicit vector growth while resolving fallback column-name matches during expression evaluation.

## Implementation

- `src/execution/expr/value.rs`
  - Replaced fallback match-index `collect()` with `Vec::with_capacity(schema.columns.len())`.
  - Preserved exact-name lookup, case-insensitive lookup, suffix fallback matching, and ambiguous-column behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_three_table_join_with_alias_projection -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_set_subquery test_correlated_not_exists_membership_filter_with_alias -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_select`
  - Passed: 27/27.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-225` is complete. Fallback column-name resolution now preallocates its match buffer from the known schema width.
