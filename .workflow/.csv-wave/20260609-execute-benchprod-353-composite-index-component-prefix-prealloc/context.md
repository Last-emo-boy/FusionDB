# BENCHPROD-353 Composite Index Component Prefix Preallocation

## Objective

Preallocate ordered composite index component prefixes without changing encoded component bytes.

## Scope

- `src/execution/composite_index.rs`

## Change

- Added `Executor::prefixed_index_component(prefix, encoded)`.
- Replaced `format!("i{}", ...)`, `format!("d{}", ...)`, `format!("t{}", ...)`, `format!("v{}", ...)`, `format!("s{}", ...)`, and `format!("n{}", ...)`.
- Added a focused helper test.

Generated component bytes remain unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test prefixed_index_component -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_create_composite_btree_index_and_lookup -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_composite_index_range_order_limit_skips_outside_range_decode -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_dml_maintains_composite_index_entries -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache -- --nocapture` | passed: 38/38 |
| `cargo test --test sql_dml -- --nocapture` | passed: 44/44 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!("[idtvns]\{}"' src/execution/composite_index.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully. The full `sql_dml` suite printed existing SSTable retry warnings while passing 44/44 tests.
