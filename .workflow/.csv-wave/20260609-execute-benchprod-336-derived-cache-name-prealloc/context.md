# BENCHPROD-336: derived cache-name preallocation

## Purpose

Continue the database-core performance pass by removing avoidable `format!` allocation work from fixed-prefix derived table cache-name construction.

## Scope

- `src/execution/expr/subquery.rs`
  - Added `subquery_derived_cache_name_for_alias()`.
  - Replaced `format!("derived:{}", alias_name)` in `table_factor_cache_name()`.
  - Added a helper unit test.

## Verification

- `cargo test subquery_derived_cache -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_set_subquery test_correlated_exists_two_table_membership_matches_ldbc_q6_shape -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_set_subquery test_correlated_not_exists_membership_filter_with_alias -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_join test_comma_join_reorder_preserves_ldbc_q4_shape_with_deferred_exists -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_join test_derived_table_join_matches_chbenchmark_q17_shape -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_set_subquery -- --nocapture`
  - Passed: 48/48.
- `cargo test --test sql_join -- --nocapture`
  - Passed: 31/31.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed; Git printed a CRLF normalization warning for the edited Rust file.

## Notes

- This is a behavior-equivalent change: generated cache-name bytes remain `derived:<alias>`.
- The value is used for EXISTS join membership cache keys, not persisted storage metadata.
- `rg 'format!\("derived:\{}"' src/execution src/storage -n` returns no matches after the change.
- The bench repository was checked before the task and remained clean.
