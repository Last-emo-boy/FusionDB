# BENCHPROD-372 Generate Subscripts Name Check Without Display Allocation

## Objective

Avoid allocating an `ObjectName` display string when detecting `generate_subscripts` during scan schema derivation.

## Scope

- `src/execution/scan/mod.rs`

## Change

- Added `scan_object_name_eq_ascii`.
- Replaced `name.to_string().eq_ignore_ascii_case("generate_subscripts")` with direct `ObjectNamePart` matching.
- Added a focused helper test for case-insensitive single-part names and qualified-name rejection.

The `generate_subscripts` table-function schema path still requires arguments and still only recognizes the simple built-in function name.

## Verification

| Command | Result |
| --- | --- |
| `cargo test scan_object_name_eq_ascii -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_set_subquery generate_subscripts -- --nocapture` | passed: 2/2 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'name\.to_string\(\)\.eq_ignore_ascii_case\("generate_subscripts"\)' src/execution/scan/mod.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
