# BENCHPROD-378 Group Column Aggregate Detection Without Display Allocation

## Objective

Avoid allocating an `ObjectName` display string and uppercase `String` while building group-by column aggregate scan plans.

## Scope

- `src/execution/query/column_scan.rs`

## Change

- Added `GroupColumnAggregateFunction`.
- Added `group_column_aggregate_function_kind`.
- Replaced the `simple_group_by_column_aggregate_projection` function-name dispatch through `func.name.to_string().to_uppercase()`.
- Preserved the same planning rules for `COUNT`, `COUNT DISTINCT`, `SUM`, `AVG`, `MIN`, `MAX`, `STRING_AGG`, and `GROUP_CONCAT`.
- Added a focused helper test for case-insensitive simple names and qualified-name rejection.

The function name classification now happens before the existing DISTINCT, wildcard, and column-argument checks.

## Verification

| Command | Result |
| --- | --- |
| `cargo test group_column_aggregate_function_kind -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate group_by_column_aggregates -- --nocapture` | passed: 3/3 |
| `cargo test --test sql_group_aggregate group_by_count_distinct -- --nocapture` | passed: 2/2 |
| `cargo test --test sql_group_aggregate group_concat_group_by -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate string_agg_group_by -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate -- --nocapture` | passed: 50/50 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'let func_name = func\.name\.to_string\(\)\.to_uppercase\(\)' src/execution/query/column_scan.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`cargo fmt` applied project-standard line wrapping before the final `cargo fmt --check`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
