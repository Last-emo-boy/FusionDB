# BENCHPROD-393 Generic STRING_AGG Join Preallocation

## Objective

Avoid generic `STRING_AGG` finalize using slice `join` by preallocating the final result buffer.

## Scope

- `src/execution/aggregation.rs`

## Change

- Added `join_string_aggregate_values`.
- Replaced generic `AggregateAccumulator::StringAgg` finalize's `vals.join(sep)` with the helper.
- Added focused helper coverage for separator-aware output and capacity.

The helper sums value bytes and separator bytes before appending into the result. Empty `STRING_AGG` behavior remains unchanged because `finalize` still returns `Value::Null` before calling the helper.

## Verification

| Command | Result |
| --- | --- |
| `cargo test join_string_aggregate_values_preallocates_exact_join -- --nocapture` | passed: 1/1 |
| `cargo test test_collecting_accumulators_preallocate_first_input -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate test_string_agg -- --nocapture` | passed: 2/2 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'join_string_aggregate_values\|vals\.join\(sep\)' src/execution/aggregation.rs -n` | generic `STRING_AGG` finalize uses `join_string_aggregate_values`; old `vals.join(sep)` path is absent |

Cargo verification uses `.tmp` under the workspace for `TEMP`/`TMP` and sets `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
