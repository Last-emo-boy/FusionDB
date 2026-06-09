# BENCHPROD-340: FTS key preallocation

## Purpose

Continue the database-core performance pass by removing avoidable `format!` allocation work from full-text-search index keys and scan prefixes.

## Scope

- `src/execution/dml/mod.rs`
  - Added `fts_index_key_for_row()`.
  - Added `fts_column_prefix_for_column()`.
  - Added `fts_token_prefix_for_token()`.
  - Added three helper unit tests.
- `src/execution/ddl/index.rs`
  - Replaced FTS backfill key construction in `CREATE INDEX`.
- `src/execution/ddl/table.rs`
  - Replaced FTS column prefix and rewritten primary-key FTS key construction.
- `src/execution/dml/insert.rs`
  - Replaced FTS insert key construction in both insert paths.
- `src/execution/dml/update.rs`
  - Replaced old/new FTS key construction for updates.
- `src/execution/dml/delete.rs`
  - Replaced FTS key construction for deletes.
- `src/execution/scan/index_plan.rs`
  - Replaced FTS token scan prefix construction for `MATCH ... AGAINST`.

## Verification

- `cargo test fts_index_key -- --nocapture`
  - Passed: 1/1.
- `cargo test fts_column_prefix -- --nocapture`
  - Passed: 1/1.
- `cargo test fts_token_prefix -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_index_cache test_fts_match_against_multi_token_intersects_index_hits -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_index_cache test_create_index_backfills_trigram_index_on_fusion_storage -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_index_cache test_update_refreshes_trigram_index_on_fusion_storage -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_index_cache test_delete_removes_trigram_index_on_fusion_storage -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_expr_functions test_parameter_placeholder_match_against -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `cargo test --test sql_dml -- --nocapture`
  - Passed: 44/44.
- `cargo fmt --check`
  - Passed after running `cargo fmt`.
- `git diff --check`
  - Passed; Git printed CRLF normalization warnings for edited Rust files.

## Notes

- This is a behavior-equivalent change: generated key bytes remain `fts:<table>:<column>:<token>:<row>`, `fts:<table>:<column>:`, and `fts:<table>:<column>:<token>:`.
- `rg 'format!\([^\n]*"fts:|"fts:\{}' src/execution src/storage -n` returns no matches after the change.
- The bench repository was checked before the task and remained clean.
