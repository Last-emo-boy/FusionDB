# BENCHPROD-335: view key preallocation

## Purpose

Continue the database-core performance pass by removing avoidable `format!` allocation work from fixed-prefix view definition key construction.

## Scope

- `src/execution/ddl/view.rs`
  - Added `view_key_for_view()`.
  - Replaced `format!("view:{}", view_name)` in `handle_create_view()`.
  - Replaced `format!("view:{}", view_name)` in `handle_drop_view()`.
  - Added a helper unit test.
- `src/execution/scan/mod.rs`
  - Added `Executor::scan_view_key_for_table()`.
  - Replaced view lookup key construction in `scan_table_base()`.
  - Replaced view lookup key construction in `scan_single_table()`.
  - Added a helper unit test.

## Verification

- `cargo test view_key -- --nocapture`
  - Passed: 2/2.
- `cargo test --test sql_view_show_constraints test_create_view_basic -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_view_show_constraints test_create_or_replace_view -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_view_show_constraints test_drop_view -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_view_show_constraints test_show_views -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_view_show_constraints -- --nocapture`
  - Passed: 16/16.
- `cargo test --test sql_join test_view_timestamp_predicate_matches_chbenchmark_q15_shape -- --nocapture`
  - Passed: 1/1.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed; Git printed CRLF normalization warnings for edited Rust files.

## Notes

- This is a behavior-equivalent change: generated key bytes remain `view:<view_name>`.
- `rg 'format!\("view:\{}"' src/execution src/storage -n` returns no matches after the change.
- The bench repository was checked before the task and remained clean.
