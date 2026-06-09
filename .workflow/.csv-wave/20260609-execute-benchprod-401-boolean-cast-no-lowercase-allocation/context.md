# BENCHPROD-401 Boolean Cast Matching Without Lowercase Allocation

## Objective

Avoid allocating lowercase strings while coercing string values to `BOOLEAN`.

## Scope

- `src/execution/types.rs`

## Change

- Added `is_true_boolean_literal`.
- Added `is_false_boolean_literal`.
- Replaced `s.trim().to_ascii_lowercase().as_str()` in `coerce_to_boolean` with direct ASCII case-insensitive literal checks.
- Added focused tests for mixed-case accepted literals and invalid-literal error text.

Accepted true literals remain `true`, `t`, `1`, `yes`, and `on`. Accepted false literals remain `false`, `f`, `0`, `no`, and `off`. Matching still trims surrounding whitespace. Invalid errors still include the original untrimmed string.

## Verification

| Command | Result |
| --- | --- |
| `cargo test coerce_to_boolean_matches_ascii_case_without_lowercase_allocation -- --nocapture` | passed: 1/1 |
| `cargo test coerce_to_boolean_preserves_invalid_error_text -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions test_cast_expressions -- --nocapture` | passed: 1/1 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg -n 'coerce_to_boolean\|to_ascii_lowercase\(\)\|is_true_boolean_literal\|is_false_boolean_literal' src/execution/types.rs` | `coerce_to_boolean` uses ASCII case-insensitive helpers; old `to_ascii_lowercase` path is absent from `types.rs` |

Cargo verification uses `.tmp` under the workspace for `TEMP`/`TMP` and sets `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
