# BENCHPROD-370 Embedding Text Borrow

## Objective

Avoid cloning string inputs for `EMBEDDING()` while preserving exact fallback text for non-string values.

## Scope

- `src/execution/expr/function.rs`

## Change

- Added `embedding_text_for_value`.
- Changed `EMBEDDING` to pass `Cow::as_ref()` into the embedding registry.
- String values now borrow existing bytes instead of cloning.
- Non-string values still allocate the same `Debug` fallback text as before.
- Added a focused helper test for borrowed string text and owned fallback text.

## Verification

| Command | Result |
| --- | --- |
| `cargo test embedding_text_for_value -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_returning_upsert_vector_rbac test_hnsw_order_by_projection -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_returning_upsert_vector_rbac -- --nocapture` | passed: 14/14 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'Value::String\\(s\\) => s\\.clone\\(\\)|embedding_registry\\.embed\\(&text\\)' src/execution/expr/function.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
