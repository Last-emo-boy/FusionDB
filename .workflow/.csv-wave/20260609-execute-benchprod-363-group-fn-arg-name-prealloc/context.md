# BENCHPROD-363 Group Function Arg Name Preallocation

## Objective

Preallocate temporary group-function argument names without changing generated column names.

## Scope

- `src/execution/expr/mod.rs`

## Change

- Added `group_function_arg_name`.
- Added `usize_decimal_len`.
- Replaced both `format!("__group_fn_arg_{}", index)` call sites in group-function evaluation.
- Added focused helper tests for exact output bytes and digit width.

Generated names remain `__group_fn_arg_<index>`. The evaluated function arguments and temporary schema columns now share the same helper.

## Verification

| Command | Result |
| --- | --- |
| `cargo test group_function_arg_name -- --nocapture` | passed: 1/1 |
| `cargo test expr_usize_decimal_len -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate test_group_by_projection_scalar_function_from_group_columns -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate -- --nocapture` | passed: 50/50 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\("__group_fn_arg_\{}", index\)' src/execution/expr/mod.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
