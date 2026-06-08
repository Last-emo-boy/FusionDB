# BENCHPROD-268 EXPLAIN Schema Membership Preallocation

## Goal

Avoid initial `HashSet` growth while EXPLAIN analyzes predicate schema membership for join-order planning.

## Implementation

- `src/execution/ddl/explain.rs`
  - `predicate_schema_members_for_explain` now preallocates extracted columns from the saturating total schema width.
  - The schema member set now preallocates from `schemas.len()`.
  - Added unit coverage for a local predicate and a join predicate.
  - EXPLAIN plan semantics are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test predicate_schema_members_for_explain_tracks_local_and_join_predicates -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache test_create_composite_btree_index_and_lookup -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_join -- --nocapture`
  - Passed: 31/31.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-268` is complete. EXPLAIN predicate schema-membership analysis now preallocates temporary sets from known schema bounds.
