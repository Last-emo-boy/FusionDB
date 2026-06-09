# BENCHPROD-374 Evaluate Function Names Without Success-Path Uppercase Allocation

## Objective

Avoid allocating an `ObjectName` display string and uppercase `String` when dispatching supported scalar functions in `evaluate_function`.

## Scope

- `src/execution/expr/function.rs`

## Change

- Added `EvaluatedFunction` and `evaluated_function_kind`.
- Replaced the success-path `func.name.to_string().to_uppercase()` dispatch with direct `ObjectNamePart` matching through `function_name_eq_ascii`.
- Added a focused classifier test for aliases, case-insensitive names, and qualified-name rejection.

The unsupported-function error branch still formats the uppercase function name so existing error text remains unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test evaluated_function_kind -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions -- --nocapture` | passed: 22/22 |
| `cargo test --test sql_returning_upsert_vector_rbac test_vector_distance_accepts_numeric_array_literals -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_returning_upsert_vector_rbac test_hnsw_order_by_projection -- --nocapture` | passed: 1/1 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'let name = func\.name\.to_string\(\)\.to_uppercase\(\)' src/execution/expr/function.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
