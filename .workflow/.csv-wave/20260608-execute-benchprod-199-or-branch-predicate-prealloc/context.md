# BENCHPROD-199 OR Branch Predicate Preallocation

## Goal

Avoid implicit vector growth while predicate planning converts split OR branches into per-branch conjunctive predicate lists.

## Implementation

- `src/execution/scan/predicate.rs`
  - Replaced `collect::<Vec<_>>()` in `extract_common_or_conjunctive_predicates` with an explicitly preallocated vector sized from `branches.len()`.
  - Preserved per-branch conjunctive predicate vector preallocation from `conjunctive_predicate_count`.
  - Preserved common predicate lifting and residual OR predicate reconstruction behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test collect_conjunctive_predicates_lifts_common_or_join_key -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_select test_where_or -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_or_branch_common_join_key_matches_chbenchmark_q19_shape -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_implicit_join_common_or_equi_predicate_matches_chbenchmark_q19_shape -- --nocapture`
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

`BENCHPROD-199` is complete. OR branch predicate planning now uses explicit capacity for the branch predicate vector where the branch count is known.
