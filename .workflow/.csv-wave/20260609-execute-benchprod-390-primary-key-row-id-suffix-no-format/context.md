# BENCHPROD-390 Primary Key Rewrite Row-Id Suffix Without Format Allocation

## Objective

Avoid `format!` allocation while matching old row-id suffixes during `ALTER TABLE ADD PRIMARY KEY` rewrites.

## Scope

- `src/execution/ddl/table.rs`

## Change

- Added `table_row_id_suffix`.
- Replaced both `format!(":{}", old_row_id)` calls in BTree and FTS rewrite branches.
- Added a focused helper test for exact suffix bytes.

Generated suffix bytes remain `:{row_id}`. Index scanning, suffix matching, and new index key construction are unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test table_row_id_suffix_preallocates_exact_suffix -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_ddl test_alter_table_add_primary_key_rewrites_secondary_btree_index_row_ids -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_ddl primary_key -- --nocapture` | passed: 12/12 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'table_row_id_suffix\|format!\(":\{\}"\|old_suffix' src/execution/ddl/table.rs -n` | primary-key rewrite suffix paths use `table_row_id_suffix`; old format pattern is absent |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
