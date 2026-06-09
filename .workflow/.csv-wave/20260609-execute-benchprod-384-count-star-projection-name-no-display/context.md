# BENCHPROD-384 Count Star Projection Detection Without Display Allocation

## Objective

Avoid allocating an `ObjectName` display string and uppercase `String` while recognizing projected `COUNT(*)` column handling.

## Scope

- `src/execution/query/mod.rs`

## Change

- Replaced the projected `COUNT(*)` name check through `func.name.to_string().to_uppercase()`.
- Reused `query_function_name_eq_ascii` for direct `ObjectNamePart` matching.
- Added a focused helper test for case-insensitive `COUNT`, non-`COUNT` rejection, and qualified-name rejection.

The wildcard-argument check is unchanged. `COUNT(*)` still reports the literal `COUNT(*)` column name, and non-wildcard `COUNT` expressions still use existing expression display formatting.

## Verification

| Command | Result |
| --- | --- |
| `cargo test query_function_name_eq_ascii_matches_without_display_string -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate test_select_count_star -- --nocapture` | passed: 2/2 |
| `cargo test --test sql_group_aggregate select_count -- --nocapture` | passed: 7/7 |
| `cargo test --test sql_group_aggregate -- --nocapture` | passed: 50/50 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'func\.name\.to_string\(\)\.to_uppercase\(\) == "COUNT"\|query_function_name_eq_ascii\(&func\.name, "COUNT"\)\|let fname = func\.name\.to_string\(\)\.to_uppercase\(\)' src/execution/query/mod.rs -n` | `COUNT(*)` projection path uses `query_function_name_eq_ascii`; window function uppercase allocation remains as next candidate |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
