# BENCHPROD-210 Join Reorder Vector Preallocation

## Goal

Avoid implicit vector growth while comma-join reorder planning builds schema snapshots and the reordered relation list from known relation counts.

## Implementation

- `src/execution/scan/join.rs`
  - Replaced schema clone `collect()` with `Vec::with_capacity(relation_count)`.
  - Replaced reordered relation `collect()` with `Vec::with_capacity(order.len() + passthrough.len())`.
  - Preserved join reorder scoring, passthrough sorting, final extension, and changed-order detection.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_comma_join_reorder_preserves_ldbc_q4_shape_with_deferred_exists -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl test_explain_join_order_includes_analyze_estimates -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_ddl`
  - Passed: 28/28.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-210` is complete. Comma-join reorder planning now preallocates schema and reordered relation vectors from known counts.
