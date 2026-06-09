# BENCHPROD-362 Column Fallback Suffix Preallocation

## Objective

Preallocate fallback column suffixes without changing qualified-name resolution.

## Scope

- `src/execution/expr/value.rs`

## Change

- Added `column_suffix_for_fallback`.
- Replaced `format!(".{}", fallback_name)` in `Executor::resolve_column_index`.
- Added a focused helper test for exact output bytes and capacity.

Fallback suffixes remain `.<fallback_name>`. Column matching, ambiguity detection, and case-insensitive fallback behavior are otherwise unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test column_suffix_for_fallback -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_ddl test_explain_qualified_primary_key_lookup -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_select_qualified_min_max_primary_key_uses_key_bounds -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml qualified_primary_key -- --nocapture` | passed: 2/2 |
| `cargo test --test sql_join ambiguous_join_input -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions -- --nocapture` | passed: 22/22 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\("\.\{}", fallback_name\)' src/execution/expr/value.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
