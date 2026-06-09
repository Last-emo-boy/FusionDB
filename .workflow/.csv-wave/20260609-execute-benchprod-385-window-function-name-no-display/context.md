# BENCHPROD-385 Window Function Detection Without Display Allocation

## Objective

Avoid allocating an `ObjectName` display string and uppercase `String` while recognizing supported window functions.

## Scope

- `src/execution/query/mod.rs`

## Change

- Added `query_window_function_name`.
- Replaced the window precompute dispatch through `func.name.to_string().to_uppercase()`.
- Added a focused helper test for case-insensitive `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, non-window rejection, and qualified-name rejection.

`compute_window_function` still receives the same canonical function names. The `OVER` clause check and final projection behavior are unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test query_window_function_name_matches_without_display_string -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_window -- --nocapture` | passed: 4/4 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'query_window_function_name\|let fname = func\.name\.to_string\(\)\.to_uppercase\(\)\|func\.name\.to_string\(\)\|to_string\(\)\.to_uppercase\(\)' src/execution/query/mod.rs -n` | window path uses `query_window_function_name`; old window uppercase binding is absent from `query/mod.rs` |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
