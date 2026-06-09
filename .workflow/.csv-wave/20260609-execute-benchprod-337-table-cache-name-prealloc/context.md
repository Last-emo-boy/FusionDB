# BENCHPROD-337: table cache-name preallocation

## Purpose

Continue the database-core performance pass by removing avoidable `format!` allocation work from fixed-prefix table factor cache-name construction.

## Scope

- `src/execution/expr/subquery.rs`
  - Added `subquery_object_name_display_capacity()`.
  - Added `subquery_table_cache_name_for_factor()`.
  - Replaced `format!("table:{}:{}", name, alias_name)` in `table_factor_cache_name()`.
  - Added a helper unit test.

## Verification

- `cargo test subquery_table_cache -- --nocapture`
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

- This is a behavior-equivalent change: generated cache-name bytes remain `table:<object_name>:<alias>`.
- `ObjectName` output still uses sqlparser `Display` through `std::fmt::Write`, so quoted and multipart names keep their existing rendering.
- The value is used for EXISTS join membership cache keys, not persisted storage metadata.
- `rg 'format!\("table:\{}:\{}"' src/execution src/storage -n` returns no matches after the change.
- The bench repository was checked before the task and remained clean.
