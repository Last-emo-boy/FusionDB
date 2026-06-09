# BENCHPROD-399 Composite Orderable Type Matching Without Uppercase Allocation

## Objective

Avoid allocating uppercase data type strings while deciding whether composite index range columns are orderable.

## Scope

- `src/execution/composite_index.rs`

## Change

- Added `starts_with_ascii_case_insensitive`.
- Added `composite_column_type_matches_any`.
- Added `composite_column_type_is_integer`.
- Replaced `data_type.to_ascii_uppercase()` in `composite_column_type_is_orderable` with direct ASCII case-insensitive equality and prefix checks.
- Added focused tests for mixed-case orderable type names and near-miss non-orderable names.

Integer, boolean, date, timestamp, datetime, and interval recognition remains unchanged. `TIMESTAMP(...)`, `DATETIME(...)`, and `INTERVAL ...` prefix matching remains ASCII case-insensitive. Composite index key encoding and range scan construction are unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test composite_column_type_orderable_matches_case_without_uppercase_allocation -- --nocapture` | passed: 1/1 |
| `cargo test composite_column_type_orderable_rejects_non_orderable_names -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache composite_index -- --nocapture` | passed: 3/3 |
| `cargo test --test sql_dml composite_index -- --nocapture` | passed: 4/4 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg -n 'composite_column_type_is_orderable\|to_ascii_uppercase\(\)\|starts_with_ascii_case_insensitive' src/execution/composite_index.rs` | composite orderable type matching uses ASCII case-insensitive helpers; old `to_ascii_uppercase` path is absent |

Cargo verification uses `.tmp` under the workspace for `TEMP`/`TMP` and sets `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
