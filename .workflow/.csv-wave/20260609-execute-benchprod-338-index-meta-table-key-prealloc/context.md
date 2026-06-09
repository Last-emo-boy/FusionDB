# BENCHPROD-338: index_meta_table key preallocation

## Purpose

Continue the database-core performance pass by removing avoidable `format!` allocation work from composite-index table metadata directory keys.

## Scope

- `src/execution/composite_index.rs`
  - Replaced `composite_index_table_marker_key()` with explicit-capacity construction.
  - Replaced `composite_index_table_prefix()` with explicit-capacity construction.
  - Replaced `composite_index_table_meta_key()` with direct explicit-capacity construction instead of formatting the prefix plus index name.
  - Added marker, prefix, and per-index key helper unit tests.

## Verification

- `cargo test composite_index_table -- --nocapture`
  - Passed: 3/3.
- `cargo test --test sql_dml test_composite_index_dml_uses_table_metadata_directory -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_dml test_composite_index_dml_falls_back_to_legacy_metadata_scan -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_index_cache test_create_composite_btree_index_and_lookup -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_index_cache test_composite_index_prefix_scan_skips_nonmatching_row_decode -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_dml -- --nocapture`
  - Passed: 44/44.
- `cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `cargo fmt --check`
  - Passed after running `cargo fmt`.
- `git diff --check`
  - Passed; Git printed a CRLF normalization warning for the edited Rust file.

## Notes

- This is a behavior-equivalent change: generated key bytes remain `index_meta_table:<table>:__marker`, `index_meta_table:<table>:`, and `index_meta_table:<table>:<index>`.
- `rg 'format!\("index_meta_table:' src/execution src/storage -n` returns no matches after the change.
- The bench repository was checked before the task and remained clean.
