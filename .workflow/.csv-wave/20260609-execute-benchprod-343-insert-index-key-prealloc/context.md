# BENCHPROD-343 INSERT Index Key Preallocation

## Objective

Preallocate ordinary secondary index keys written by INSERT paths.

## Scope

- `src/execution/dml/insert.rs`

## Change

- Added `insert_index_key_for_value(table_name, column_name, value, row_id)`.
- Replaced two INSERT path `format!` calls that generated `index:<table>:<column>:<value>:<row_id>`.
- Added a focused helper test.

The generated key remains `index:<table>:<column>:<value>:<row_id>`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test insert_index_key_for_value -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_indexed_text_insert_updates_trigram_index_on_fusion_storage -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_create_btree_index -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_delete_primary_key_updates_secondary_index -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_duplicate_primary_key_insert_is_rejected_and_upsert_updates_cache -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_copy_from_csv_with_header_and_index_lookup -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_insert_select -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml -- --nocapture` | passed: 44/44 |
| `cargo test --test sql_index_cache -- --nocapture` | passed: 38/38 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'index_key = format!\(|"index:\{}:\{}:\{}:\{}"' src/execution/dml/insert.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully. The full `sql_dml` suite printed existing SSTable retry warnings while passing 44/44 tests.
