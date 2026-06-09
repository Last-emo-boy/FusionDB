# BENCHPROD-361 String Concat Preallocation

## Objective

Preallocate SQL string-concat results without changing expression values.

## Scope

- `src/execution/expr/value.rs`

## Change

- Added `concat_string_values`.
- Replaced `format!("{}{}", l, r)` in `BinaryOperator::StringConcat`.
- Added a focused helper test for exact output bytes and capacity.

String concat still returns the left value followed by the right value. Array concat and null handling remain unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test concat_string_values -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions string_concat -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions -- --nocapture` | passed: 22/22 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\("\{\}\{\}", l, r\)' src/execution/expr/value.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
