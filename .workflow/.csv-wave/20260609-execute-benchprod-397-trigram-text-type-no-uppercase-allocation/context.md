# BENCHPROD-397 Trigram Text Data Type Matching Without Uppercase Allocation

## Objective

Avoid allocating uppercase data type strings while finding indexed text columns for trigram maintenance.

## Scope

- `src/execution/dml/mod.rs`

## Change

- Added `starts_with_ascii_case_insensitive`.
- Added `is_trigram_text_data_type`.
- Replaced `col.data_type.trim().to_ascii_uppercase()` matching in `indexed_trigram_text_columns`.
- Added focused tests for text-type matching and indexed trigram column filtering.

`TEXT`, `STRING`, `VARCHAR`, `CHAR`, `VARCHAR(...)`, `CHAR(...)`, and `CHARACTER...` recognition remains unchanged. `is_indexed` and `IndexType::BTree | IndexType::FTS` filtering remains unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test trigram_text_data_type_matching_is_ascii_case_insensitive -- --nocapture` | passed: 1/1 |
| `cargo test indexed_trigram_text_columns_filters_text_indexes_without_uppercase_allocation -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_indexed_text_insert_updates_trigram_index_on_fusion_storage -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache trigram -- --nocapture` | passed: 5/5 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'is_trigram_text_data_type\|to_ascii_uppercase\(\)' src/execution/dml/mod.rs -n` | trigram text data type matching uses `is_trigram_text_data_type`; old `to_ascii_uppercase` path is absent |

Cargo verification uses `.tmp` under the workspace for `TEMP`/`TMP` and sets `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
