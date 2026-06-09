# BENCHPROD-351 HNSW Index Name Format Clear

## Objective

Preallocate the remaining HNSW index names and remove all `format!("hnsw_...")` construction under `src`.

## Scope

- `src/execution/ddl/table.rs`
- `src/execution/scan/mod.rs`
- `src/storage/fusion.rs`

## Change

- Reused `Executor::hnsw_index_name_for_column` in DDL table row-id rewrite and scan vector search paths.
- Added `vector_rebuild_hnsw_index_name_for_column(table_name, column_name)` for storage vector rebuild.
- Added a focused storage helper test.

Generated names remain `hnsw_<table>_<column>`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test hnsw_index_name_for_column -- --nocapture` | passed: 3/3 |
| `cargo test --test sql_returning_upsert_vector_rbac test_hnsw_order_by_projection -- --nocapture` | passed: 1/1 |
| `cargo test fusion_rebuild_vector_index_decodes_only_hnsw_columns -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_ddl -- --nocapture` | passed: 33/33 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\s*\(\s*"hnsw_' src -n -U` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed existing CRLF normalization warnings for the edited Rust files while exiting successfully.

## Remaining HNSW Name Format Calls

None found under `src` with multiline `rg 'format!\s*\(\s*"hnsw_' src -n -U`.
