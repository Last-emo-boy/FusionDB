# BENCHPROD-339: fk_meta key preallocation

## Purpose

Continue the database-core performance pass by removing avoidable `format!` allocation work from foreign-key metadata keys and prefixes.

## Scope

- `src/execution/foreign_key.rs`
  - Added `foreign_key_child_key_for_name()`.
  - Added `foreign_key_parent_key_for_name()`.
  - Added `foreign_key_child_prefix_for_table()`.
  - Added `foreign_key_parent_prefix_for_table()`.
  - Replaced child/parent metadata key construction in `store_foreign_keys()`.
  - Replaced child/parent metadata prefix construction in load and delete paths.
  - Added four helper unit tests.
- `src/execution/dml/update.rs`
  - Replaced `fk_meta` prefix formatting in simple primary-key update fast-path metadata probes.

## Verification

- `cargo test foreign_key_child_key -- --nocapture`
  - Passed: 1/1.
- `cargo test foreign_key_parent_key -- --nocapture`
  - Passed: 1/1.
- `cargo test foreign_key_child_prefix -- --nocapture`
  - Passed: 1/1.
- `cargo test foreign_key_parent_prefix -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_view_show_constraints test_foreign_key_insert_update_and_parent_delete_checks -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_view_show_constraints test_composite_foreign_key_insert_update_and_parent_checks -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_view_show_constraints test_foreign_key_blocks_dependent_alter_and_drop_table -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_dml test_copy_from_csv_enforces_constraints_on_direct_path -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_view_show_constraints -- --nocapture`
  - Passed: 16/16.
- `cargo test --test sql_dml -- --nocapture`
  - Passed: 44/44.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed; Git printed CRLF normalization warnings for the edited Rust files.

## Notes

- This is a behavior-equivalent change: generated key bytes remain `fk_meta:child:<table>:<fk>`, `fk_meta:parent:<table>:<fk>`, `fk_meta:child:<table>:`, and `fk_meta:parent:<table>:`.
- `rg 'format!\("fk_meta:' src/execution src/storage -n` returns no matches after the change.
- The bench repository was checked before the task and remained clean.
