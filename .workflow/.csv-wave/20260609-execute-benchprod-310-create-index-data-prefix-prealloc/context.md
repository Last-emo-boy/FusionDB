# BENCHPROD-310 CREATE INDEX Data Prefix Preallocation

## Goal

Avoid generic `format!` allocation work when `CREATE INDEX` backfill scans build `data:<table>:` prefixes from known table-name length.

## Implementation

- `src/execution/ddl/index.rs`
  - Added `create_index_data_prefix_for_table()` with explicit capacity for `data:<table>:`.
  - Replaced `format!` prefix construction in CREATE INDEX existing-row backfill scans.
  - Added unit coverage for helper output and capacity.
  - Generated prefix bytes, transaction scan prefixes, BTree index entries, composite index entries, FTS/trigram backfill, HNSW backfill, row-cache reuse, and SQL results are unchanged.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test create_index_data_prefix -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo test --test sql_returning_upsert_vector_rbac test_hnsw_order_by_projection -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; $env:CARGO_PROFILE_TEST_DEBUG='0'; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo verification used an E: workspace temp directory and `CARGO_PROFILE_TEST_DEBUG=0` to avoid the recurring MSVC PDB pressure seen in this workspace.

Focused index tests were launched in parallel and printed Cargo file-lock wait messages before passing.

## Result

`BENCHPROD-310` is complete. CREATE INDEX data-prefix construction now reserves capacity from known table-name length.
