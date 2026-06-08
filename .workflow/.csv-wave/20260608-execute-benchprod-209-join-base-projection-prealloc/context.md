# BENCHPROD-209 Join Base Projection Preallocation

## Goal

Avoid implicit vector growth while join base scans derive base-table projection columns from an already-built stage projection.

## Implementation

- `src/execution/scan/join.rs`
  - Replaced join base projection `filter_map().collect()` with `Vec::with_capacity(stage_projection.len())` plus explicit pushes.
  - Preserved base column resolution behavior.
  - Preserved fallback to full table scan when the derived projection is empty or covers the full schema.
  - Preserved `scan_join_base` error propagation for relation prefixing.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_join_left_filter_projection_skips_unused_left_column_decode -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_join_base_scan_reuses_row_cache -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join test_join_projection_pushdown_with_group_by -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-209` is complete. Join base scan projection derivation now preallocates base projection vectors from known stage projection lengths.
