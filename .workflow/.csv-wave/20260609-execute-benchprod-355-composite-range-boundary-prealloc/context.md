# BENCHPROD-355 Composite Range Boundary Preallocation

## Objective

Preallocate composite index range-scan prefixes and bounds without changing encoded boundary bytes.

## Scope

- `src/execution/composite_index.rs`

## Change

- Added `Executor::composite_index_components_prefix`.
- Added `Executor::composite_index_range_prefix`.
- Added `Executor::composite_index_range_bound`.
- Replaced range-scan `index_prefix`, `range_prefix`, `start`, and `end` boundary `format!` calls.
- Added focused helper tests.

Generated prefixes and bounds remain unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test composite_index_components_prefix -- --nocapture` | passed: 1/1 |
| `cargo test composite_index_range -- --nocapture` | passed: 4/4 |
| `cargo test --test sql_index_cache -- --nocapture` | passed: 38/38 |
| `cargo test --test sql_dml test_tpcc_order_status_limit_finds_late_composite_index_match -- --nocapture` | passed: 1/1 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\(' src/execution/composite_index.rs -n` | only unique-constraint error message remains |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
