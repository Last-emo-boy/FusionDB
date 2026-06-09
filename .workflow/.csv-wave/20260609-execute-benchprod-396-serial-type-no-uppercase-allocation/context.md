# BENCHPROD-396 Serial Default Data Type Matching Without Uppercase Allocation

## Objective

Avoid allocating uppercase data type strings while finding `SERIAL` default candidate columns.

## Scope

- `src/execution/dml/insert.rs`

## Change

- Added `is_serial_default_data_type`.
- Replaced `column.data_type.trim().to_ascii_uppercase()` matching in `serial_default_candidate_column_indexes`.
- Added focused helper coverage for supported serial aliases, trimming, and a non-match.

`SERIAL`, `SERIAL2`, `SERIAL4`, `SERIAL8`, `SMALLSERIAL`, and `BIGSERIAL` recognition remains unchanged. Serial value generation and row scanning are unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test serial_default_data_type_matching_is_ascii_case_insensitive -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_insert_omitted_serial_primary_key_generates_ids -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml serial -- --nocapture` | passed: 1/1 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'is_serial_default_data_type\|to_ascii_uppercase\(\)' src/execution/dml/insert.rs -n` | serial type matching uses `is_serial_default_data_type`; old `to_ascii_uppercase` path is absent |

Cargo verification uses `.tmp` under the workspace for `TEMP`/`TMP` and sets `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
