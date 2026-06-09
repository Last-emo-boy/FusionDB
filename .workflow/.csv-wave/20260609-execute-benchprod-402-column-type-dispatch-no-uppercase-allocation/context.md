# BENCHPROD-402 Column Type Dispatch Without Uppercase Allocation

## Objective

Avoid allocating uppercase data type strings while dispatching column-value coercion.

## Scope

- `src/execution/types.rs`

## Change

- Replaced `data_type.trim().to_ascii_uppercase()` in `coerce_value_to_column_type` with direct use of the trimmed type name.
- Added `type_name_matches_any`.
- Added `type_name_starts_with_ascii_case_insensitive`.
- Updated type-name helper predicates to perform ASCII case-insensitive equality and prefix checks directly.
- Added focused tests for mixed-case type dispatch and prefix-based type names.

Integer, float, decimal, boolean, date, timestamp, interval, and text dispatch remains case-insensitive. Parameterized forms such as `numeric(...)`, `float(...)`, `timestamp(...)`, `varchar(...)`, and `character...` remain recognized.

## Verification

| Command | Result |
| --- | --- |
| `cargo test coerce_value_to_column_type_matches_type_names_without_uppercase_allocation -- --nocapture` | passed: 1/1 |
| `cargo test type_name_helpers_match_prefixes_case_insensitively -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions test_cast_expressions -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions test_production_scalar_types_insert_compare_and_order -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_copy_from_csv_coerces_epoch_nanoseconds_to_timestamptz -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_copy_from_text_coerces_timezone_offset_to_timestamptz -- --nocapture` | passed: 1/1 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg -n 'coerce_value_to_column_type\|to_ascii_uppercase\(\)\|type_name_matches_any\|type_name_starts_with_ascii_case_insensitive' src/execution/types.rs` | `coerce_value_to_column_type` uses direct type-name helpers; old `to_ascii_uppercase` path is absent from `types.rs` |

Cargo verification uses `.tmp` under the workspace for `TEMP`/`TMP` and sets `CARGO_PROFILE_TEST_DEBUG=0`.

One attempted command, `cargo test --test sql_expr_functions test_cast_expressions test_scalar_type_literals_and_coercions -- --nocapture`, was rejected by Cargo because only one test filter is accepted. The intended regressions were rerun separately and passed.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
