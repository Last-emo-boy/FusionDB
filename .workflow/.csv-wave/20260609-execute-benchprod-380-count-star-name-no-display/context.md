# BENCHPROD-380 Count Star Detection Without Display Allocation

## Objective

Avoid allocating an `ObjectName` display string while detecting simple `COUNT(*)` group-by projections.

## Scope

- `src/execution/query/column_scan.rs`

## Change

- Replaced `is_simple_count_star` function-name detection through `func.name.to_string().eq_ignore_ascii_case("COUNT")`.
- Reused `column_scan_function_name_eq_ascii` for direct `ObjectNamePart` matching.
- Added a focused unit test that parses `Count(*)` and exercises `is_simple_count_star`.

The duplicate-treatment, single-argument, and wildcard checks are unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test is_simple_count_star -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate group_by_count -- --nocapture` | passed: 7/7 |
| `cargo test --test sql_group_aggregate select_count -- --nocapture` | passed: 7/7 |
| `cargo test --test sql_group_aggregate -- --nocapture` | passed: 50/50 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'func\.name\.to_string\(\)\.eq_ignore_ascii_case\("COUNT"\)' src/execution/query/column_scan.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
