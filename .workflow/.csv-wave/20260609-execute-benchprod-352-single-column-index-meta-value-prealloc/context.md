# BENCHPROD-352 Single-Column Index Metadata Value Preallocation

## Objective

Preallocate legacy single-column index metadata values without changing their serialized bytes.

## Scope

- `src/execution/composite_index.rs`
- `src/execution/ddl/index.rs`
- `src/execution/ddl/table.rs`

## Change

- Added `Executor::single_column_index_meta_value(table, column)`.
- Replaced `format!("{}:{}", table, column)` in DDL `CREATE INDEX` metadata writes.
- Replaced `format!("{}:{}", table, column)` in `ALTER TABLE ADD PRIMARY KEY` metadata writes.
- Added a focused helper test.

Generated metadata remains `table:column`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test single_column_index_meta_value -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_create_btree_index -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_show_indexes_reports_composite_columns -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_ddl test_alter_table_add_primary_key_rewrites_secondary_btree_index_row_ids -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache -- --nocapture` | passed: 38/38 |
| `cargo test --test sql_ddl -- --nocapture` | passed: 33/33 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!("\{}:\{}"' src/execution src/storage -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed existing CRLF normalization warnings for the edited Rust files while exiting successfully.
