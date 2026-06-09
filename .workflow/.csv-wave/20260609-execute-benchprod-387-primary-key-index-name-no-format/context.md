# BENCHPROD-387 Primary Key Index Names Without Format Allocation

## Objective

Avoid `format!` allocation while generating default primary-key index names.

## Scope

- `src/execution/ddl/table.rs`
- `tests/sql_ddl.rs`

## Change

- Added `table_primary_key_index_name` for `table_pkey`.
- Added `table_column_primary_key_index_name` for `table_column_pkey`.
- Replaced two default-name `format!` calls in composite primary-key storage and `ALTER TABLE ADD PRIMARY KEY`.
- Added a SQL regression for `ALTER TABLE ... ADD PRIMARY KEY (id)` without an explicit constraint name.

Generated name bytes are unchanged. Explicit constraint/index names still bypass the helpers and remain cloned from SQL.

## Verification

| Command | Result |
| --- | --- |
| `cargo test table_primary_key_index_name_preallocates_exact_name -- --nocapture` | passed: 1/1 |
| `cargo test table_column_primary_key_index_name_preallocates_exact_name -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_ddl test_alter_table_add_primary_key_uses_default_index_name -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_ddl primary_key -- --nocapture` | passed: 12/12 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'table_primary_key_index_name\|table_column_primary_key_index_name\|format!\("\{\}_pkey"\|format!\("\{\}_\{\}_pkey"' src/execution/ddl/table.rs -n` | default primary-key index name paths use helpers; old format patterns are absent |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed existing CRLF normalization warnings for edited files while exiting successfully.
