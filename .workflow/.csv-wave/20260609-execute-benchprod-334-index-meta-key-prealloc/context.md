# BENCHPROD-334: index_meta key preallocation

## Purpose

Continue the database-core performance pass by removing avoidable `format!` allocation work from fixed-prefix index metadata key construction.

## Scope

- `src/execution/ddl/index.rs`
  - Added `index_meta_key_for_index()`.
  - Replaced `format!("index_meta:{}", index_name_str)` in `handle_create_index()`.
  - Replaced `format!("index_meta:{}", index_name)` in `handle_drop_index()`.
  - Added a helper unit test.
- `src/execution/ddl/table.rs`
  - Added `table_index_meta_key_for_index()`.
  - Replaced `format!("index_meta:{}", index_name)` in composite primary key metadata storage.
  - Replaced `format!("index_meta:{}", index_name)` in `ALTER TABLE ADD PRIMARY KEY`.
  - Added a helper unit test.

## Verification

- `cargo test index_meta_key -- --nocapture`
  - Passed: 2/2.
- `cargo test --test sql_index_cache test_create_btree_index -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_index_cache test_drop_index -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_index_cache test_create_composite_btree_index_and_lookup -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_ddl test_create_table_table_level_composite_primary_key -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_ddl test_alter_table_only_add_primary_key_pgbench_shape -- --nocapture`
  - Passed: 1/1.
- `cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `cargo test --test sql_ddl -- --nocapture`
  - Passed: 33/33.
- `cargo fmt --check`
  - Passed after running `cargo fmt`.
- `git diff --check`
  - Passed; Git printed CRLF normalization warnings for edited Rust files.

## Notes

- This is a behavior-equivalent change: generated key bytes remain `index_meta:<index_name>`.
- `rg 'format!\("index_meta:\{}"' src/execution src/storage -n` returns no matches after the change.
- The bench repository was checked before the task and remained clean.
