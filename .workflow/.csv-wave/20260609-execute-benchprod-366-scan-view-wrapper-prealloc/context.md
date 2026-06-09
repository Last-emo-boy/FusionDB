# BENCHPROD-366 Scan View Wrapper SQL Preallocation

## Objective

Preallocate the SQL wrapper used when scanning a stored view definition without changing the text parsed by the query engine.

## Scope

- `src/execution/scan/mod.rs`

## Change

- Added `scan_view_wrapped_query_sql`.
- Replaced `format!("SELECT * FROM ({}) AS _v", view_sql)` in base table view expansion.
- Added a focused helper test for exact output bytes and capacity.

Generated wrapper SQL remains `SELECT * FROM (<view_sql>) AS _v`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test scan_view_wrapped_query_sql -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_view_show_constraints -- --nocapture` | passed: 16/16 |
| `cargo test --test sql_join test_view_timestamp_predicate_matches_chbenchmark_q15_shape -- --nocapture` | passed: 1/1 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\\(\"SELECT \\* FROM \\(\\{\\}\\) AS _v\"|SELECT \\* FROM \\(\\{\\}\\) AS _v' src/execution/scan/mod.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
