# BENCHPROD-290 Trigram-Index Map Preallocation

## Goal

Avoid the first growth step for a newly constructed trigram index when the first indexed table is inserted.

## Implementation

- `src/storage/trigram.rs`
  - Added `postings_map()` with capacity for the first table entry.
  - Added `id_map()` with capacity for the first table row-id map entry.
  - Changed `TrigramIndex::new()` to initialize both top-level maps through those constructors.
  - Changed `Default` to call `TrigramIndex::new()` so both construction paths share the preallocation behavior.
  - Added unit coverage for first-table map preallocation.
  - Trigram extraction, posting updates, persistence, LIKE lookup, and row-key mapping behavior are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test storage::trigram::tests -- --nocapture`
  - Passed: 3/3.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

## Result

`BENCHPROD-290` is complete. New trigram indexes now reserve top-level map capacity for the first indexed table.
