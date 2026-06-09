# BENCHPROD-403 Primary Key Row Type Matching Without Uppercase Allocation

## Objective

Avoid allocating uppercase primary-key data type strings while reconstructing projected primary-key rows from row ids.

## Scope

- `src/execution/scan/index_plan.rs`

## Change

- Replaced `column.data_type.to_ascii_uppercase()` in `primary_key_row_from_id` with a borrowed `&str`.
- Added `primary_key_type_starts_with_ascii_case_insensitive`.
- Updated `primary_key_row_from_parts` to use `is_integer_type_name`, `eq_ignore_ascii_case`, and ASCII case-insensitive prefix matching.
- Added a focused test for mixed-case `INT`, `DATE`, and `TIMESTAMP(...)` primary-key type names.

Integer, `DATE`, `TIMESTAMP...`, and `DATETIME` row-id decoding keeps the same behavior. Unsupported type names and undecodable row ids still fall back to `Value::String(row_id.to_string())`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test primary_key_row_from_parts_matches_type_case_without_uppercase_allocation -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_primary_key_point_lookup_reuses_row_cache -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_primary_key_projection_reuses_full_row_cache -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_select_order_by_primary_key_limit_offset -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_primary_key_only_projection_with_pk_order -- --nocapture` | passed: 1/1 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg -n 'primary_key_row_from_id\|primary_key_row_from_parts\|to_ascii_uppercase\(\)\|primary_key_type_starts_with_ascii_case_insensitive' src/execution/scan/index_plan.rs` | primary-key row reconstruction uses borrowed type names and ASCII case-insensitive checks; old `to_ascii_uppercase` path is absent |

Cargo verification uses `.tmp` under the workspace for `TEMP`/`TMP` and sets `CARGO_PROFILE_TEST_DEBUG=0`.

During implementation, the first helper name collided with another `Executor` associated function from `composite_index.rs`; it was renamed to the primary-key-specific helper above. One exploratory test command, `cargo test --test sql_index_cache pk_lookup_cache -- --nocapture`, matched 0 tests, so the concrete test names listed above were run instead.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
