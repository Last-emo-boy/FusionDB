# BENCHPROD-357 Foreign Key Composite Index Prefix Preallocation

## Objective

Preallocate the foreign-key composite-index lookup prefix without changing scan boundaries.

## Scope

- `src/execution/foreign_key.rs`

## Change

- Added `foreign_key_composite_index_value_prefix`.
- Replaced the composite unique-index parent lookup prefix `format!` in foreign-key validation.
- Added a focused helper test for exact output bytes and capacity.

Composite foreign-key parent lookup prefixes remain `<composite_index_prefix><value_key>:`. For example, `index:district:warehouse_id,district_id:` plus `i1|i2` still scans `index:district:warehouse_id,district_id:i1|i2:`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test foreign_key_composite_index_value_prefix -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_view_show_constraints foreign_key -- --nocapture` | passed: 4/4 |
| `cargo test --test sql_view_show_constraints -- --nocapture` | passed: 16/16 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'let prefix = format!\(\s*\"\{\}\{\}:\"' src/execution/foreign_key.rs -n -U` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
