# BENCHPROD-348 Composite Metadata Value Preallocation

## Objective

Preallocate composite index metadata values without changing their serialized bytes.

## Scope

- `src/execution/composite_index.rs`

## Change

- Added `composite_index_meta_value_for_prefix(prefix, table, columns)`.
- Replaced `format!("v3:{}:{}", table, columns.join(","))`.
- Replaced `format!("u3:{}:{}", table, columns.join(","))`.
- Added focused helper tests for v3 and u3 metadata values.

Generated metadata remains `v3:<table>:<columns>` and `u3:<table>:<columns>`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test composite_index_meta_value -- --nocapture` | passed: 1/1 |
| `cargo test composite_unique_meta_value -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_create_composite_btree_index_and_lookup -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_show_indexes_reports_composite_columns -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_dml_maintains_composite_index_entries -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_composite_index_dml_uses_table_metadata_directory -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_composite_index_dml_falls_back_to_legacy_metadata_scan -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_ddl test_create_table_table_level_composite_primary_key -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache -- --nocapture` | passed: 38/38 |
| `cargo test --test sql_dml -- --nocapture` | passed: 44/44 |
| `cargo test --test sql_ddl -- --nocapture` | passed: 33/33 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\("v3:\{}:\{}"|format!\("u3:\{}:\{}"' src/execution/composite_index.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully. The full `sql_dml` suite printed existing SSTable retry warnings and slow-query logs while passing 44/44 tests.
