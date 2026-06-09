# BENCHPROD-356 Secondary Index Scan Prefix Preallocation

## Objective

Preallocate secondary index scan prefixes without changing scan boundaries.

## Scope

- `src/execution/scan/index_plan.rs`

## Change

- Added `Executor::secondary_index_prefix_for_value`.
- Added `Executor::secondary_index_prefix_for_value_start`.
- Replaced equality, `IN`-list, and LIKE fixed-prefix secondary index prefix `format!` calls.
- Added focused helper tests.

Equality and `IN` prefixes remain `index:<table>:<column>:<value>:`. LIKE fixed-prefix scans still use `index:<table>:<column>:<prefix>` without the trailing colon.

## Verification

| Command | Result |
| --- | --- |
| `cargo test secondary_index_prefix_for_value -- --nocapture` | passed: 2/2 |
| `cargo test --test sql_index_cache test_create_btree_index -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_indexed_text_insert_updates_trigram_index_on_fusion_storage -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_select test_select_in_list -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache -- --nocapture` | passed: 38/38 |
| `cargo test --test sql_select -- --nocapture` | passed: 27/27 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\(\s*"index:\{}:\{}:\{}:?' src/execution/scan/index_plan.rs -n -U` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
