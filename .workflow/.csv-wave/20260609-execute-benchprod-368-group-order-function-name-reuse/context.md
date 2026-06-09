# BENCHPROD-368 Group Order Function Name Reuse

## Objective

Avoid repeated `ORDER BY` function display allocation while resolving group aggregate fast-path order keys.

## Scope

- `src/execution/query/column_scan.rs`

## Change

- Added `simple_group_by_function_order_index`.
- Changed the `Expr::Function` branch in `simple_group_by_order_keys` to build `func.to_string()` once per order expression.
- Kept the same case-insensitive comparison against output column names.
- Added a focused helper test for preformatted function names.

The matched function text remains the same display output previously produced by `format!("{}", func)`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test simple_group_by_function_order_index -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate test_group_by_column_aggregates_fast_path_order_by_limit -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate -- --nocapture` | passed: 50/50 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'eq_ignore_ascii_case\(&format!\("\{\}", func\)\)|position\(\|column\| column\.eq_ignore_ascii_case\(&format!\("\{\}", func\)\)\)' src/execution/query/column_scan.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
