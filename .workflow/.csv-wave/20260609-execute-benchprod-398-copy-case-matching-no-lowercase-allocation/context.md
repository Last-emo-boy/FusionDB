# BENCHPROD-398 COPY Case Matching Without Lowercase Allocation

## Objective

Avoid allocating lowercase strings while parsing COPY format names and COPY boolean fields.

## Scope

- `src/execution/copy.rs`

## Change

- Replaced `format.value.to_ascii_lowercase()` in `copy_from_options` with direct `eq_ignore_ascii_case` checks for `csv` and `text`.
- Replaced `trimmed.to_ascii_lowercase().as_str()` in `copy_field_to_value` with direct `eq_ignore_ascii_case` checks for `true`, `t`, `false`, and `f`.
- Added focused tests for mixed-case COPY format matching, unsupported format error text, and mixed-case boolean field parsing.

`COPY FORMAT CSV` still switches the default delimiter from tab to comma. `COPY FORMAT TEXT` keeps the default tab delimiter. Unsupported format errors still include the original format spelling. The fallback string branch still returns the original field string, including surrounding whitespace.

## Verification

| Command | Result |
| --- | --- |
| `cargo test copy_from_options_matches_format_case_without_lowercase_allocation -- --nocapture` | passed: 1/1 |
| `cargo test copy_field_to_value_matches_boolean_case_without_lowercase_allocation -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml copy -- --nocapture` | passed: 6/6 |
| `cargo test --test pg_integration copy_from_stdin -- --nocapture` | passed: 2/2 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg -n 'to_ascii_lowercase\(\)\|eq_ignore_ascii_case\(\"csv\"\)\|copy_field_to_value_matches_boolean' src/execution/copy.rs` | COPY format matching uses `eq_ignore_ascii_case`; old `to_ascii_lowercase` paths are absent from `copy.rs` |

Cargo verification uses `.tmp` under the workspace for `TEMP`/`TMP` and sets `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
