# BENCHPROD-371 Coalesce Name Check Without Display Allocation

## Objective

Avoid allocating an `ObjectName` display string when detecting `COALESCE` during final group expression evaluation.

## Scope

- `src/execution/expr/mod.rs`

## Change

- Added `function_name_eq_ascii`.
- Replaced `func.name.to_string().eq_ignore_ascii_case("COALESCE")` with direct `ObjectNamePart` matching.
- Added a focused helper test for case-insensitive single-part function names.

The `COALESCE` group expression path still evaluates arguments in order and returns the first non-NULL value.

## Verification

| Command | Result |
| --- | --- |
| `cargo test function_name_eq_ascii -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_set_subquery test_group_by_projection_can_coalesce_aggregate -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate test_group_by_projection_scalar_function_from_group_columns -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_set_subquery -- --nocapture` | passed: 48/48 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'func\.name\.to_string\(\)\.eq_ignore_ascii_case\("COALESCE"\)' src/execution/expr/mod.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
