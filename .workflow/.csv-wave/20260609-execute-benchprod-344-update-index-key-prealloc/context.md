# BENCHPROD-344 UPDATE Index Key Preallocation

## Objective

Preallocate ordinary secondary index keys used by UPDATE when indexed values change.

## Scope

- `src/execution/dml/update.rs`

## Change

- Added `update_index_key_for_value(table_name, column_name, value, row_id)`.
- Replaced old/new ordinary secondary index key `format!` calls in UPDATE.
- Added a focused helper test.

The generated key remains `index:<table>:<column>:<value>:<row_id>`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test update_index_key_for_value -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_update_invalidates_row_cache_for_index_lookup -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_update_primary_key_fast_path_preserves_untouched_secondary_index -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_update_primary_key_reuses_row_cache_for_secondary_index -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_update_single_row -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml -- --nocapture` | passed: 44/44 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'old_index_key = format!\(|new_index_key = format!\(|"index:\{}:\{}:\{}:\{}"' src/execution/dml/update.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully. The full `sql_dml` suite printed existing SSTable retry warnings and slow-query logs while passing 44/44 tests.
