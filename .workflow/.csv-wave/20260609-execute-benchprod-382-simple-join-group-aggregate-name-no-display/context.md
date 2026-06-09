# BENCHPROD-382 Simple Join Group Aggregate Detection Without Display Allocation

## Objective

Avoid allocating an `ObjectName` display string and uppercase `String` while building simple join group aggregate plans.

## Scope

- `src/execution/query/mod.rs`

## Change

- Added `simple_join_group_aggregate_name`.
- Replaced `simple_join_group_aggregate_projection` dispatch through `func.name.to_string().to_uppercase()`.
- Added a focused helper test for case-insensitive `COUNT`/`SUM` and qualified-name rejection.

The planner still supports the same functions in this path: `COUNT(*)`, `COUNT(column)`, and `SUM(column)`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test simple_join_group_aggregate_name -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_join join_group_by_aggregate -- --nocapture` | passed: 3/3 |
| `cargo test --test sql_join join_projection_pushdown_with_group_by -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_join -- --nocapture` | passed: 31/31 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'simple_join_group_aggregate_name|let func_name = func\.name\.to_string\(\)\.to_uppercase\(\)' src/execution/query/mod.rs -n` | simple join path uses helper; remaining uppercase match is a later planner path |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
