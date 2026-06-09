# BENCHPROD-381 Simple Projected Group Aggregate Detection Without Display Allocation

## Objective

Avoid allocating an `ObjectName` display string and uppercase `String` while building simple projected group aggregate plans.

## Scope

- `src/execution/query/mod.rs`

## Change

- Added `query_function_name_eq_ascii`.
- Added `simple_projected_group_aggregate_name`.
- Replaced `simple_projected_group_aggregate_projection` dispatch through `func.name.to_string().to_uppercase()`.
- Added a focused helper test for case-insensitive supported names and qualified-name rejection.

The planner still supports the same functions in this path: `COUNT(*)`, `SUM(column)`, and `ARRAY_AGG(column)`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test simple_projected_group_aggregate_name -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate group_by_sum -- --nocapture` | passed: 3/3 |
| `cargo test --test sql_group_aggregate group_by_projection -- --nocapture` | passed: 2/2 |
| `cargo test --test sql_set_subquery array_agg -- --nocapture` | passed: 2/2 |
| `cargo test --test sql_group_aggregate -- --nocapture` | passed: 50/50 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'simple_projected_group_aggregate_name|let func_name = func\.name\.to_string\(\)\.to_uppercase\(\)' src/execution/query/mod.rs -n` | simple projected path uses helper; remaining uppercase matches are later planner paths |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`cargo fmt` applied project-standard test formatting before the final `cargo fmt --check`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
