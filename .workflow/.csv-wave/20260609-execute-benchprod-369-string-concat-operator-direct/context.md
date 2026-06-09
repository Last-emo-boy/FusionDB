# BENCHPROD-369 String Concat Operator Direct Path

## Objective

Avoid operand clones and temporary scalar strings in the SQL `||` operator while preserving exact results.

## Scope

- `src/execution/expr/value.rs`

## Change

- Added `concat_operator_values`.
- Added `append_string_concat_value`.
- Replaced `StringConcat` logic that cloned operands and built `l` / `r` temporary strings.
- Kept string-string concatenation on the existing exact-capacity `concat_string_values` helper.
- Added a focused helper test for scalar, NULL, fallback debug, and array behavior.

Array concat behavior remains `array || array`, `array || value`, and `value || array`. Scalar concat still returns NULL if either side is NULL.

## Verification

| Command | Result |
| --- | --- |
| `cargo test concat_operator_values -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions test_string_concat_operator -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions -- --nocapture` | passed: 22/22 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'let l = match left_val|let r = match right_val|concat_string_values\(&l, &r\)|left_val\.clone\(\)|right_val\.clone\(\)' src/execution/expr/value.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
