# BENCHPROD-354 Composite Index Entry Key Preallocation

## Objective

Preallocate composite index entry keys and value prefixes without changing encoded key bytes.

## Scope

- `src/execution/composite_index.rs`

## Change

- Added `Executor::composite_index_entry_key(prefix, value_key, row_id)`.
- Added `Executor::composite_index_value_prefix(prefix, value_key)`.
- Replaced entry key construction in `composite_index_key` and `composite_index_key_for_meta`.
- Replaced composite unique scan prefix construction in `validate_composite_unique_constraints`.
- Added focused helper tests.

Generated keys remain unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test composite_index_entry_key -- --nocapture` | passed: 1/1 |
| `cargo test composite_index_value_prefix -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_create_composite_btree_index_and_lookup -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache test_composite_index_prefix_scan_skips_nonmatching_row_decode -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_dml test_dml_maintains_composite_index_entries -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_index_cache -- --nocapture` | passed: 38/38 |
| `cargo test --test sql_dml -- --nocapture` | passed: 44/44 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully. The full `sql_dml` suite printed existing SSTable retry warnings while passing 44/44 tests.

## Remaining Candidate

Composite range-scan boundary builders still use `format!` and remain queued for a later iteration.
