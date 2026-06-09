# BENCHPROD-377 Simple Column Aggregate Detection Without Display Allocation

## Objective

Avoid allocating an `ObjectName` display string and uppercase `String` while building simple column aggregate scan plans.

## Scope

- `src/execution/query/column_scan.rs`

## Change

- Added `column_scan_function_name_eq_ascii`.
- Replaced the `simple_column_aggregate_projection` function-name dispatch through `func.name.to_string().to_uppercase()`.
- Preserved the same aggregate plan selection for `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STRING_AGG`, and `GROUP_CONCAT`.
- Added a focused helper test for case-insensitive simple names and qualified-name rejection.

The later group-by column aggregate planner still has its own `func.name.to_string().to_uppercase()` and remains a separate next-task candidate.

## Verification

| Command | Result |
| --- | --- |
| `cargo test column_scan_function_name_eq_ascii -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate bare_ -- --nocapture` | passed: 10/10 |
| `cargo test --test sql_group_aggregate -- --nocapture` | passed: 50/50 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'func_name\.as_str\(\)' src/execution/query/column_scan.rs -n` | remaining match is the later group-by aggregate path, not `simple_column_aggregate_projection` |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`cargo fmt` applied the project-standard branch formatting before the final `cargo fmt --check`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
