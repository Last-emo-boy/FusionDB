# BENCHPROD-365 Query Qualified Column Preallocation

## Objective

Preallocate qualified column names in the TSBS lastpoint lateral path without changing aliases or output columns.

## Scope

- `src/execution/query/mod.rs`

## Change

- Added `query_qualified_column_name`.
- Replaced TSBS lastpoint/lateral `alias.column` `format!` calls.
- Preserved the existing short-circuit behavior for the inner `ORDER BY time` check.
- Added a focused helper test for exact output bytes and capacity.

Generated names remain `<alias>.<column>`, including `c.tags_id`, `t.id`, `t.hostname`, `c.time`, and output columns such as `b.tags_id`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test query_qualified_column_name -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_join test_tsbs_lastpoint_distinct_on_lateral_join -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_join -- --nocapture` | passed: 31/31 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\("\{\}\.(tags_id|id|time|hostname)"|format!\("\{\}\.\{}", derived_alias, column\.name\)' src/execution/query/mod.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
