# BENCHPROD-383 Single Table Prefix Aggregate Detection Without Display Allocation

## Objective

Avoid allocating an `ObjectName` display string and uppercase `String` while recognizing single-table `COUNT`/`MIN`/`MAX` prefix aggregate optimizations.

## Scope

- `src/execution/query/mod.rs`

## Change

- Added `single_table_prefix_aggregate_name`.
- Replaced the single-table aggregate optimization dispatch through `func.name.to_string().to_uppercase()`.
- Added a focused helper test for case-insensitive `COUNT`, `MIN`, `MAX`, and qualified-name rejection.

The prefix-count and primary-key `MIN`/`MAX` eligibility checks are unchanged, and output column naming still uses the existing function display formatting.

## Verification

| Command | Result |
| --- | --- |
| `cargo test single_table_prefix_aggregate_name -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate select_count -- --nocapture` | passed: 7/7 |
| `cargo test --test sql_group_aggregate bare_min_max -- --nocapture` | passed: 2/2 |
| `cargo test --test sql_group_aggregate -- --nocapture` | passed: 50/50 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'single_table_prefix_aggregate_name|let func_name = func\.name\.to_string\(\)\.to_uppercase\(\)' src/execution/query/mod.rs -n` | single-table prefix path uses helper; no `let func_name` uppercase matches remain |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
