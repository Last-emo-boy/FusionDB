# BENCHPROD-375 Aggregate Function Detection Without Display Allocation

## Objective

Avoid allocating an `ObjectName` display string and uppercase `String` while detecting aggregate functions in expression extraction.

## Scope

- `src/execution/expr/mod.rs`

## Change

- Added `aggregate_function_name`.
- Replaced aggregate extraction's `func.name.to_string().to_uppercase()` with direct `ObjectNamePart` matching.
- Kept recorded accumulator names unchanged: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `ARRAY_AGG`, `STRING_AGG`, `GROUP_CONCAT`, and `COUNT_DISTINCT`.
- Added a focused helper test for case-insensitive aggregate names and qualified-name rejection.

Non-aggregate function expressions now recurse into their arguments without first allocating an uppercase display name.

## Verification

| Command | Result |
| --- | --- |
| `cargo test aggregate_function_name -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate test_count_distinct -- --nocapture` | passed: 3/3 |
| `cargo test --test sql_group_aggregate string_agg -- --nocapture` | passed: 3/3 |
| `cargo test --test sql_group_aggregate group_concat -- --nocapture` | passed: 2/2 |
| `cargo test --test sql_group_aggregate -- --nocapture` | passed: 50/50 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'let name = func\.name\.to_string\(\)\.to_uppercase\(\)' src/execution/expr/mod.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`cargo fmt` applied the project-standard line wrap before the final `cargo fmt --check`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
