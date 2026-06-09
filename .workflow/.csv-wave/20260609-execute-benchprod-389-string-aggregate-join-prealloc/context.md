# BENCHPROD-389 String Aggregate Joins With Preallocation

## Objective

Avoid generic `join` calls while finalizing `STRING_AGG` and `GROUP_CONCAT` values.

## Scope

- `src/execution/query/column_scan.rs`

## Change

- Added `join_string_aggregate_values`.
- Replaced both `self.strings.join(",")` finalization paths.
- Added a focused helper test for the exact comma-joined result.

Output bytes are unchanged. Empty aggregate states still return `NULL`, and non-empty string aggregate values still use comma separators.

## Verification

| Command | Result |
| --- | --- |
| `cargo test join_string_aggregate_values_preallocates_exact_value -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate string_agg -- --nocapture` | passed: 3/3 |
| `cargo test --test sql_group_aggregate group_concat -- --nocapture` | passed: 2/2 |
| `cargo test --test sql_group_aggregate -- --nocapture` | passed: 50/50 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'join_string_aggregate_values\|strings\.join\(","\)' src/execution/query/column_scan.rs -n` | string aggregate finalizers use `join_string_aggregate_values`; old `strings.join` path is absent |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
