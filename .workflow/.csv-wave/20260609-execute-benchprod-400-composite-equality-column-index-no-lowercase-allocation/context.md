# BENCHPROD-400 Composite Equality Matching By Column Index Without Lowercase Allocation

## Objective

Avoid allocating lowercase column-name keys while matching composite index equality predicates.

## Scope

- `src/execution/composite_index.rs`

## Change

- Renamed the equality collection helper to `composite_index_equality_values_by_column_index`.
- Changed its map from `HashMap<String, Value>` to `HashMap<usize, Value>`, keyed by schema column index.
- Replaced per-index-column `column.to_ascii_lowercase()` lookup with `schema.get_column_index(column)` followed by index-keyed lookup.
- Added a focused test that parses mixed-case predicates and verifies equality values are stored by schema index.

Case-insensitive column resolution still flows through existing schema resolution. Composite index key encoding, scan prefixes/ranges, and row-id ordering are unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test composite_index_equality_values_use_column_indices_without_lowercase_keys -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache composite_index -- --nocapture` | passed: 3/3 |
| `cargo test --test sql_dml composite_index -- --nocapture` | passed: 4/4 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg -n 'to_ascii_lowercase\(\)\|composite_index_equality_values_by_column_index\|HashMap<usize, Value>\|column.to_ascii_lowercase' src/execution/composite_index.rs` | composite equality matching uses `HashMap<usize, Value>`; old column-name lowercase paths are absent |

Cargo verification uses `.tmp` under the workspace for `TEMP`/`TMP` and sets `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
