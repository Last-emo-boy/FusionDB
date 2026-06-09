# BENCHPROD-346 CREATE INDEX Key Preallocation

## Objective

Preallocate ordinary secondary index keys written during CREATE INDEX backfill.

## Scope

- `src/execution/ddl/index.rs`

## Change

- Added `create_index_key_for_value(table_name, column_name, value, row_id)`.
- Replaced the ordinary BTree CREATE INDEX backfill `format!` call.
- Added a focused helper test.

The generated key remains `index:<table>:<column>:<value>:<row_id>`.

## Verification

| Command | Result |
| --- | --- |
| `cargo fmt --check` | passed |
| `cargo test create_index_key_for_value -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_create_btree_index -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_create_integer_btree_index_after_load_uses_comparable_keys -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_create_index_reuses_row_cache_for_backfill -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_empty_secondary_index_lookup_skips_full_table_scan -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache -- --nocapture` | passed: 38/38 |
| `git diff --check` | passed |
| `rg 'index_key = format!\(|"index:\{}:\{}:\{}:\{}"' src/execution/ddl/index.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
