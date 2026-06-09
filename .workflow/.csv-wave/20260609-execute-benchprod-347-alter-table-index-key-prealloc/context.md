# BENCHPROD-347 ALTER TABLE Index Key Preallocation

## Objective

Preallocate ordinary index keys used by ALTER TABLE ADD PRIMARY KEY and secondary BTree row-id rewrite.

## Scope

- `src/execution/ddl/table.rs`

## Change

- Added `table_index_key_for_value(table_name, column_name, value, row_id)`.
- Added `table_index_key_for_prefix_value_row(prefix, value, row_id)`.
- Replaced ALTER TABLE ADD PRIMARY KEY index creation and secondary BTree row-id rewrite `format!` calls.
- Added focused helper tests.

The generated key remains `index:<table>:<column>:<value>:<row_id>`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test table_index_key -- --nocapture` | passed: 2/2 |
| `cargo test --test sql_ddl test_alter_table_only_add_primary_key_pgbench_shape -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_ddl test_alter_table_add_primary_key_rewrites_secondary_btree_index_row_ids -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_ddl test_alter_table_add_primary_key_rejects_existing_primary_key -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_ddl test_alter_table_add_primary_key_requires_first_column -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_ddl test_alter_table_add_primary_key_rejects_existing_nulls -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_ddl -- --nocapture` | passed: 33/33 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg '"index:\{}:\{}:\{}:\{}"|index_key = format!\(|new_index_key = format!\(' src/execution/ddl/table.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully. The full `sql_ddl` suite printed slow-query logs while passing 33/33 tests.
