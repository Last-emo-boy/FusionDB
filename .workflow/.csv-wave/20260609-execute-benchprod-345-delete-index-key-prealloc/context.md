# BENCHPROD-345 DELETE Index Key Preallocation

## Objective

Preallocate ordinary secondary index keys used by DELETE when removing indexed rows.

## Scope

- `src/execution/dml/delete.rs`

## Change

- Added `delete_index_key_for_value(table_name, column_name, value, row_id)`.
- Replaced the ordinary secondary index key `format!` call in DELETE.
- Added a focused helper test.

The generated key remains `index:<table>:<column>:<value>:<row_id>`.

## Verification

| Command | Result |
| --- | --- |
| `cargo fmt --check` | passed |
| `cargo test delete_index_key_for_value -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_delete_primary_key_updates_secondary_index -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_delete_primary_key_reuses_row_cache_for_secondary_index -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_delete_with_where -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml -- --nocapture` | passed: 44/44 |
| `git diff --check` | passed |
| `rg 'index_key = format!\(|"index:\{}:\{}:\{}:\{}"' src/execution/dml/delete.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully. The full `sql_dml` suite printed existing SSTable retry warnings while passing 44/44 tests.
