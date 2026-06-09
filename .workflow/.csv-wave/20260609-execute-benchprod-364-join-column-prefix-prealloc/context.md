# BENCHPROD-364 JOIN Column Prefix Preallocation

## Objective

Preallocate JOIN schema column prefixes without changing output column names.

## Scope

- `src/execution/scan/join.rs`

## Change

- Added `join_prefixed_column_name`.
- Replaced `format!("{}.{}", prefix, col.name)` in `Executor::prefix_schema_columns`.
- Added a focused helper test for exact output bytes and capacity.

Generated JOIN column names remain `<prefix>.<column>`. Columns that already contain a dot are still left unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test join_prefixed_column_name -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_join test_join_base_scan_reuses_row_cache -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_join -- --nocapture` | passed: 31/31 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\("\{\}\.\{}", prefix, col\.name\)' src/execution/scan/join.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
