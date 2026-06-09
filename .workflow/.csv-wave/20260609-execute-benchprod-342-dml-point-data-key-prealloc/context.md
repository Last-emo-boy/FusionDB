# BENCHPROD-342 DML Point Data Key Preallocation

## Objective

Preallocate data keys in UPDATE and DELETE primary-key point lookup paths.

## Scope

- `src/execution/dml/delete.rs`
- `src/execution/dml/update.rs`

## Change

- Added `delete_data_key_for_prefix_row(prefix, row_id)`.
- Added `update_data_key_for_prefix_row(prefix, row_id)`.
- Replaced four `format!("{}{}", prefix, row_id)` calls in UPDATE and DELETE point lookup paths.
- Added focused helper tests in both files.

The generated key remains `data:<table>:<row_id>`. These paths reuse an already-built `data:<table>:` prefix, so the helper reserves `prefix.len() + row_id.len()` exactly before appending the row id.

## Verification

| Command | Result |
| --- | --- |
| `cargo test data_key_for_prefix_row -- --nocapture` | passed: 2/2 |
| `cargo test --test sql_dml test_delete_primary_key_without_secondary_index_skips_row_decode -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_delete_qualified_primary_key_without_secondary_index_skips_row_decode -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_delete_commuted_primary_key_without_secondary_index_skips_row_decode -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_update_primary_key_simple_table_fast_path -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_update_qualified_primary_key_uses_point_lookup -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_update_commuted_primary_key_uses_point_lookup -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_delete_primary_key_reuses_row_cache_for_secondary_index -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_update_primary_key_reuses_row_cache_for_secondary_index -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml -- --nocapture` | passed: 44/44 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\("\{}\{}", prefix, row_id\)' src/execution/dml/delete.rs src/execution/dml/update.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed existing CRLF normalization warnings for the edited Rust files while exiting successfully. The full `sql_dml` suite printed existing SSTable retry warnings and one slow-query log while passing 44/44 tests.
