# BENCHPROD-386 Foreign Key Default Name Without Join Format Allocation

## Objective

Avoid allocating an intermediate joined child-column string and a `format!` result while generating default foreign key names.

## Scope

- `src/execution/foreign_key.rs`

## Change

- Added `foreign_key_default_name`.
- Replaced `child_columns.join("_")` plus `format!("fk_{}_{}_{}")` with direct appends into one preallocated `String`.
- Added a focused default-name test for a multi-column child key.

The generated default name bytes remain `fk_{child_table}_{child_columns_joined_by_underscore}_{parent_table}`. Explicit foreign key names, metadata storage keys, and constraint validation logic are unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test foreign_key_default_name_preallocates_exact_name -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_view_show_constraints foreign_key -- --nocapture` | passed: 4/4 |
| `cargo test --test sql_dml fk -- --nocapture` | passed: 1/1 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'foreign_key_default_name\|child_columns\.join\("_"\)\|format!\("fk_\{\}_\{\}_\{\}"' src/execution/foreign_key.rs -n` | `foreign_key_default_name` is used; old join/format default-name path is absent |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
