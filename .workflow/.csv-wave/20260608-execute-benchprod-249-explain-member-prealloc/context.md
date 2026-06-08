# BENCHPROD-249 EXPLAIN Predicate Member Preallocation

## Goal

Avoid implicit vector growth while converting EXPLAIN join-order predicate schema member sets into sorted member lists.

## Implementation

- `src/execution/ddl/explain.rs`
  - Replaced `members.into_iter().collect::<Vec<_>>()` with `Vec::with_capacity(members.len())`.
  - Pushed member indices into the preallocated vector before `sort_unstable()`.
  - Preserved predicate column extraction, schema matching, unknown-column fallback, sorted member output, and join-order selection behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl test_explain_join_order_includes_analyze_estimates -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl -- --nocapture`
  - Passed: 29/29.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-249` is complete. EXPLAIN join-order predicate member lists now preallocate from the known set size before sorting.
