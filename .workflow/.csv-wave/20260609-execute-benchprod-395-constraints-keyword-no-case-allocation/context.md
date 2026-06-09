# BENCHPROD-395 Constraint Keyword Matching Without Case-Conversion Allocation

## Objective

Avoid allocating case-converted strings while matching `CHECK` prefixes and keyword default values.

## Scope

- `src/execution/dml/constraints.rs`

## Change

- Added `starts_with_check_keyword_ascii`.
- Added `parse_keyword_default_value`.
- Replaced `expr_str.to_uppercase().starts_with("CHECK")`.
- Replaced `def_str.to_lowercase().as_str()` matching for `true`, `false`, and `null`.
- Added focused helper tests for ASCII case-insensitive behavior.

Parsing, error handling, and fallback string behavior remain unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test check_keyword_prefix_matching_is_ascii_case_insensitive -- --nocapture` | passed: 1/1 |
| `cargo test keyword_default_value_matching_is_ascii_case_insensitive -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_view_show_constraints test_default_column_values -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_view_show_constraints test_check_constraint -- --nocapture` | passed: 1/1 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'to_uppercase\(\)\|to_lowercase\(\)\|starts_with_check_keyword_ascii\|parse_keyword_default_value' src/execution/dml/constraints.rs -n` | constraint keyword matching uses helpers; old case-conversion allocation paths are absent |

Cargo verification uses `.tmp` under the workspace for `TEMP`/`TMP` and sets `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
