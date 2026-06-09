# BENCHPROD-367 Concat Value Direct Append

## Objective

Avoid temporary scalar and debug fallback strings in the `CONCAT` function while preserving exact output text.

## Scope

- `src/execution/expr/function.rs`

## Change

- Added `append_concat_value`.
- Replaced `CONCAT` branches that used `to_string()` or `format!("{:?}")` before `push_str`.
- Added a focused helper test covering exact text for string, integer, float, boolean, NULL, and debug fallback values.

`CONCAT` still appends strings directly, skips NULL, and uses the same display/debug text for all other values.

## Verification

| Command | Result |
| --- | --- |
| `cargo test append_concat_value -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions -- --nocapture` | passed: 22/22 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'result\.push_str\(&n\.to_string\(\)\)|result\.push_str\(&f\.to_string\(\)\)|result\.push_str\(&b\.to_string\(\)\)|result\.push_str\(&format!\("\{:\?\}", val\)\)' src/execution/expr/function.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
