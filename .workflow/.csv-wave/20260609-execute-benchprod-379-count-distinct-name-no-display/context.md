# BENCHPROD-379 Count Distinct Detection Without Display Allocation

## Objective

Avoid allocating an `ObjectName` display string while detecting `COUNT(DISTINCT ...)` column-scan projections.

## Scope

- `src/execution/query/column_scan.rs`

## Change

- Replaced `count_distinct_projection` function-name detection through `func.name.to_string().eq_ignore_ascii_case("COUNT")`.
- Reused `column_scan_function_name_eq_ascii` for direct `ObjectNamePart` matching.
- Extended the focused helper test to cover `COUNT`.

The `COUNT(DISTINCT ...)` duplicate-treatment and single-argument checks are unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test column_scan_function_name_eq_ascii -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate count_distinct -- --nocapture` | passed: 5/5 |
| `cargo test --test sql_group_aggregate -- --nocapture` | passed: 50/50 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'func\.name\.to_string\(\)\.eq_ignore_ascii_case\("COUNT"\)' src/execution/query/column_scan.rs -n` | remaining match is `is_simple_count_star`, not `count_distinct_projection` |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
